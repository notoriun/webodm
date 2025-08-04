import os
import shutil
import tempfile
import traceback
import json
import socket
import time
import uuid

from threading import Event, Thread
from celery.utils.log import get_task_logger
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Count
from django.db.models import Q
from app.models import Profile

from app.models import Project, Task
from app.vendor import zipfly
from app.utils.s3_utils import list_s3_objects, convert_task_path_to_s3
from app.utils.file_utils import ensure_path_exists
from app.classes.task_files_uploader import TaskFilesUploader
from app.classes.task_assets_manager import TaskAssetsManager
from nodeodm import status_codes
from nodeodm.models import ProcessingNode
from webodm import settings
from .celery import app, MockAsyncResult
from .utils.recover_uploads_task_db import RecoverUploadsTaskDb
from .utils import redis_file_cache
from app.raster_utils import (
    export_raster as export_raster_sync,
    extension_for_export_format,
)
from app.pointcloud_utils import export_pointcloud as export_pointcloud_sync
from app.utils.redis_client_factory import get_redis_client
import redis

logger = get_task_logger("app.logger")
redis_client = get_redis_client()

# What class to use for async results, since during testing we need to mock it
TestSafeAsyncResult = MockAsyncResult if settings.TESTING else app.AsyncResult


@app.task(ignore_result=True)
def update_nodes_info():
    if settings.NODE_OPTIMISTIC_MODE:
        return

    processing_nodes = ProcessingNode.objects.all()
    for processing_node in processing_nodes:
        processing_node.update_node_info()

        # Workaround for mysterious "webodm_node-odm-1" or "webodm-node-odm-1" hostname switcharoo on Mac
        # Technically we already check for the correct hostname during setup,
        # but sometimes that doesn't work?
        check_hostname = "webodm_node-odm-1"
        if (
            processing_node.hostname == check_hostname
            and not processing_node.is_online()
        ):
            try:
                socket.gethostbyname(processing_node.hostname)
            except:
                # Hostname was invalid, try renaming
                processing_node.hostname = "webodm-node-odm-1"
                processing_node.update_node_info()
                if processing_node.is_online():
                    logger.info("Found and fixed webodm_node-odm-1 hostname switcharoo")
                else:
                    processing_node.hostname = check_hostname
                processing_node.save()


@app.task(ignore_result=True)
def cleanup_projects():
    # Delete all projects that are marked for deletion
    # and that have no tasks left
    total, count_dict = (
        Project.objects.filter(deleting=True)
        .annotate(tasks_count=Count("task"))
        .filter(tasks_count=0)
        .delete()
    )
    if total > 0 and "app.Project" in count_dict:
        logger.info("Deleted {} projects".format(count_dict["app.Project"]))


@app.task(ignore_result=True)
def cleanup_tmp_directory():
    # Delete files and folder in the tmp directory that are
    # older than 24 hours
    tmpdir = settings.MEDIA_TMP
    time_limit = 60 * 60 * 24

    for f in os.listdir(tmpdir):
        now = time.time()
        filepath = os.path.join(tmpdir, f)
        modified = os.stat(filepath).st_mtime
        if modified < now - time_limit:
            if os.path.isfile(filepath):
                os.remove(filepath)
            else:
                shutil.rmtree(filepath, ignore_errors=True)

            logger.info("Cleaned up: %s (%s)" % (f, modified))


# Based on https://stackoverflow.com/questions/22498038/improve-current-implementation-of-a-setinterval-python/22498708#22498708
def setInterval(interval, func, *args):
    stopped = Event()

    def loop():
        while not stopped.wait(interval):
            func(*args)

    t = Thread(target=loop)
    t.daemon = True
    t.start()
    return stopped.set


@app.task(ignore_result=True)
def process_task(taskId):
    lock_id = "task_lock_{}".format(taskId)
    cancel_monitor = None
    delete_lock = True

    try:
        task_lock_last_update = redis_client.getset(lock_id, time.time())
        if task_lock_last_update is not None:
            # Check if lock has expired
            if time.time() - float(task_lock_last_update) <= 30:
                # Locked
                delete_lock = False
                return
            else:
                # Expired
                logger.warning(
                    "Task {} has an expired lock! This could mean that WebODM is running out of memory. Check your server configuration.".format(
                        taskId
                    )
                )

        # Set lock
        def update_lock():
            redis_client.set(lock_id, time.time())

        cancel_monitor = setInterval(5, update_lock)

        try:
            task = Task.objects.get(pk=taskId)
        except ObjectDoesNotExist:
            logger.info("Task {} has already been deleted.".format(taskId))
            return

        try:
            task.process()
        except Exception as e:
            logger.error(
                "Uncaught error! This is potentially bad. Please report it to http://github.com/OpenDroneMap/WebODM/issues: {} {}".format(
                    e, traceback.format_exc()
                )
            )
            if settings.TESTING:
                raise e
    finally:
        if cancel_monitor is not None:
            cancel_monitor()

        if delete_lock:
            try:
                redis_client.delete(lock_id)
            except redis.exceptions.RedisError:
                # Ignore errors, the lock will expire at some point
                pass


def get_pending_tasks():
    # All tasks that have a processing node assigned
    # Or that need one assigned (via auto)
    # or tasks that need a status update
    # or tasks that have a pending action
    # no partial tasks allowed
    return Task.objects.filter(
        Q(
            processing_node__isnull=True,
            auto_processing_node=True,
        )
        | Q(
            Q(status=None) | Q(status__in=[status_codes.QUEUED, status_codes.RUNNING]),
            processing_node__isnull=False,
        )
        | Q(pending_action__isnull=False)
    ).exclude(
        status=status_codes.COMPLETED,
        pending_action__isnull=True,
        node_error_retry__gte=settings.TASK_MAX_NODE_ERROR_RETRIES,
        node_connection_retry__gte=settings.TASK_MAX_NODE_CONNECTION_RETRIES,
        partial=True,
        upload_in_progress=True,
    )


@app.task(ignore_result=True)
def process_pending_tasks():
    tasks = get_pending_tasks()
    for task in tasks:
        process_task.delay(task.id)


@app.task(ignore_result=True)
def manage_processing_nodes():
    from nodeodm.classes.processing_nodes_manager import ProcessingNodesManager

    ProcessingNodesManager(logger).improve_processing_nodes_performance()


@app.task(bind=True)
def export_raster(self, input, **opts):
    try:
        logger.info(
            "Exporting raster {} with options: {}".format(input, json.dumps(opts))
        )
        tmpfile = tempfile.mktemp(
            "_raster.{}".format(
                extension_for_export_format(opts.get("format", "gtiff"))
            ),
            dir=settings.MEDIA_TMP,
        )
        export_raster_sync(input, tmpfile, **opts)
        result = {"file": tmpfile}

        if settings.TESTING:
            TestSafeAsyncResult.set(self.request.id, result)

        return result
    except Exception as e:
        logger.error(str(e))
        return {"error": str(e)}


@app.task(bind=True)
def export_pointcloud(self, task_id, input: str, **opts):
    try:
        logger.info(
            "Exporting point cloud {} with options: {}".format(input, json.dumps(opts))
        )
        tmpfile = tempfile.mktemp(
            "_pointcloud.{}".format(opts.get("format", "laz")), dir=settings.MEDIA_TMP
        )

        task = Task.objects.get(pk=task_id)
        task_assets_manager = TaskAssetsManager(task)
        tmpfile_input_s3 = task_assets_manager.download_asset_to_temp(input)

        export_pointcloud_sync(tmpfile_input_s3, tmpfile, **opts)
        result = {"file": tmpfile}

        if settings.TESTING:
            TestSafeAsyncResult.set(self.request.id, result)

        return result
    except Exception as e:
        logger.error(str(e))
        return {"error": str(e)}


@app.task(ignore_result=True)
def check_quotas():
    profiles = Profile.objects.filter(quota__gt=-1)
    for p in profiles:
        if p.has_exceeded_quota():
            deadline = p.get_quota_deadline()
            if deadline is None:
                deadline = p.set_quota_deadline(settings.QUOTA_EXCEEDED_GRACE_PERIOD)
            now = time.time()
            if now > deadline:
                # deadline passed, delete tasks until quota is met
                logger.info(
                    "Quota deadline expired for %s, deleting tasks"
                    % str(p.user.username)
                )
                task_count = Task.objects.filter(project__owner=p.user).count()
                c = 0

                while p.has_exceeded_quota():
                    try:
                        last_task = (
                            Task.objects.filter(project__owner=p.user)
                            .order_by("-created_at")
                            .first()
                        )
                        if last_task is None:
                            break
                        logger.info("Deleting %s" % last_task)
                        last_task.delete()
                    except Exception as e:
                        logger.warn(
                            "Cannot delete %s for %s: %s"
                            % (str(last_task), str(p.user.username), str(e))
                        )

                    c += 1
                    if c >= task_count:
                        break
        else:
            p.clear_quota_deadline()


@app.task(bind=True, max_retries=settings.TASK_MAX_UPLOAD_RETRIES, priority=8)
def task_upload_file(self, task_id, files_to_upload, s3_images, upload_type):
    logger.info(
        f"Start upload files {files_to_upload} and s3 images {s3_images} of type {upload_type} to task {task_id}"
    )

    recover_db = RecoverUploadsTaskDb()
    recover_db.set_task(task_id, self.request.id)

    uploader = TaskFilesUploader(task_id)

    try:
        _create_upload_heartbeat(task_id)

        result = uploader.upload_files(files_to_upload, s3_images, upload_type)
        logger.info(f"Upload task finished with result {result}")
        recover_db.remove_by_task(task_id)

        if settings.TESTING:
            TestSafeAsyncResult.set(self.request.id, result)

        return {"output": result}
    except Exception as e:
        _log_error(f"Error on recover uploading task {task_id}", e)

        error = "unknow_error" if not e or not str(e) or _is_uuid(str(e)) else str(e)

        return {"error": error}
    finally:
        _remove_task_upload_heartbeat(task_id)


@app.task(bind=True)
def generate_zip_from_asset(self, task_id, asset):
    try:
        logger.info(f"Start generate zip from {asset}")

        task = Task.objects.get(pk=task_id)
        asset_zip: dict[str, str] = task.ASSETS_MAP.get(asset, None)

        if not asset_zip:
            raise Exception(f"asset '{asset}' nao encontrado!")

        zip_dir = os.path.abspath(
            task.assets_path(asset_zip.get("deferred_compress_dir"))
        )
        result = _generate_zip_from_dir(
            task, zip_dir, asset_zip.get("deferred_exclude_files", tuple())
        )

        if settings.TESTING:
            TestSafeAsyncResult.set(self.request.id, result)

        return result
    except Exception as e:
        logger.error(str(e))
        logger.info(f"generate zip from task finished with error {str(e)}")
        return {"error": str(e)}


@app.task(bind=True)
def generate_backup_zip(self, task_id):
    try:
        logger.info("Start generate backup zip")

        task = Task.objects.get(pk=task_id)
        ensure_path_exists(task.task_path("data"))
        task.write_backup_file()
        zip_dir = os.path.abspath(task.task_path(""))
        result = _generate_zip_from_dir(task, zip_dir)

        if settings.TESTING:
            TestSafeAsyncResult.set(self.request.id, result)

        return result
    except Exception as e:
        logger.error(str(e))
        logger.info(f"backup task finished with error {str(e)}")
        return {"error": str(e)}


@app.task(bind=True)
def recover_uploading_task(self, task_id: str):
    try:
        celery_id = self.request.id
        recover_db = RecoverUploadsTaskDb()

        already_uploading = _has_task_upload(task_id)

        if already_uploading:
            return {}

        _create_upload_heartbeat(task_id)
        recover_db.set_secondary_celery_task(celery_id, task_id)

        uploader = TaskFilesUploader(task_id)
        result = uploader.recover_upload()

        if settings.TESTING:
            TestSafeAsyncResult.set(self.request.id, result)

        return {"output": result}
    except Exception as e:
        _log_error(f"Error on recover uploading task {task_id}", e)

        error = "unknow_error" if not e or not str(e) or _is_uuid(str(e)) else str(e)

        return {"error": error}
    finally:
        _remove_task_upload_heartbeat(task_id)


@app.task()
def manage_recover_uploading_tasks():
    try:
        tasks_uploading = Task.objects.filter(upload_in_progress=True)

        for task in tasks_uploading:
            recover_uploading_task.delay(task.id)
    except Exception as e:
        logger.error(f"Error on recover uploading tasks. Original error: {str(e)}")
        return {"error": str(e)}


@app.task()
def clear_old_tasks_from_files_db():
    try:
        TaskFilesUploader(0).clear_old_tasks_from_db()
        RecoverUploadsTaskDb().clear_old_tasks_from_db()
    except Exception as e:
        logger.error(f"Error on clear old tasks dbs. Original error: {str(e)}")
        return {"error": str(e)}


def _generate_zip_from_dir(task, zip_dir, exclude_files=tuple([])):
    try:
        tmpfile = tempfile.mktemp(".zip", dir=settings.MEDIA_TMP)

        s3_dir = convert_task_path_to_s3(zip_dir)

        s3_objects = list_s3_objects(s3_dir)
        s3_keys = [s3_object["Key"] for s3_object in s3_objects]

        logger.info(f"Found these files to add in zip: {s3_keys}")
        task_assets_manager = TaskAssetsManager(task)
        for s3_key in s3_keys:
            local_path = os.path.join(settings.MEDIA_ROOT, s3_key)
            task_assets_manager.download_asset_to_temp(local_path)

        logger.info("downloaded all files")
        paths = [
            {
                "n": os.path.relpath(os.path.join(dp, f), zip_dir),
                "fs": os.path.join(dp, f),
            }
            for dp, dn, filenames in os.walk(zip_dir)
            for f in filenames
        ]
        if isinstance(exclude_files, tuple):
            paths = [p for p in paths if os.path.basename(p["fs"]) not in exclude_files]
        if len(paths) == 0:
            raise FileNotFoundError("No files available for download")

        logger.info(f"creating zip with {paths}")
        zip_stream = zipfly.ZipStream(paths)

        logger.info(f"writing zip in {tmpfile}")
        with open(tmpfile, "wb") as zip_file:
            zip_stream.lazy_load(1024)
            for zip_data in zip_stream.generator:
                zip_file.write(zip_data)

        result = {"file": tmpfile}

        return result
    except Exception as e:
        logger.error(str(e))
        logger.info(f"upload task finished with error {str(e)}")
        return {"error": str(e)}


def _create_upload_heartbeat(task_id: str):
    return redis_file_cache.create_heartbeat(
        f"upload_file_for_task_{task_id}", settings.UPLOADING_HEARTBEAT_INTERVAL_SECONDS
    )


def _has_task_upload(task_id: str):
    return redis_file_cache.heartbeat_exists(f"upload_file_for_task_{task_id}")


def _remove_task_upload_heartbeat(task_id: str):
    return redis_file_cache.remove_heartbeat(f"upload_file_for_task_{task_id}")


def _is_uuid(value: str):
    try:
        uuid_instance = uuid.UUID(value)
        return uuid_instance is not None
    except:
        return False


def _log_error(prefix: str, error: Exception):
    error_str = str(error)
    need_more_info = (
        not error or not error_str or error_str == "None" or _is_uuid(error_str)
    )
    error_message = f"{prefix}. Original error: {error_str}"
    if need_more_info:
        stack_trace = traceback.TracebackException.from_exception(error)
        error_message += (
            f" {repr(error_str)}. Traceback: {''.join(stack_trace.format())}"
        )

    logger.error(error_message)
