import os
import shutil
import tempfile
import traceback
import json
import socket
import time

from threading import Event, Thread
from celery.utils.log import get_task_logger
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Count
from django.db.models import Q
from app.models import Profile

from app.models import Project, Task
from app.vendor import zipfly
from app.utils.s3_utils import list_s3_objects, convert_task_path_to_s3
from app.classes.task_files_uploader import TaskFilesUploader
from app.classes.task_assets_manager import TaskAssetsManager
from nodeodm import status_codes
from nodeodm.models import ProcessingNode
from webodm import settings
import worker
from .celery import app
from app.raster_utils import export_raster as export_raster_sync, extension_for_export_format
from app.pointcloud_utils import export_pointcloud as export_pointcloud_sync
from django.utils import timezone
from datetime import timedelta
import redis

logger = get_task_logger("app.logger")
redis_client = redis.Redis.from_url(settings.CELERY_BROKER_URL)

# What class to use for async results, since during testing we need to mock it
TestSafeAsyncResult = worker.celery.MockAsyncResult if settings.TESTING else app.AsyncResult

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
        check_hostname = 'webodm_node-odm-1'
        if processing_node.hostname == check_hostname and not processing_node.is_online():
            try:
                socket.gethostbyname(processing_node.hostname)
            except:
                # Hostname was invalid, try renaming
                processing_node.hostname = 'webodm-node-odm-1'
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
    total, count_dict = Project.objects.filter(deleting=True).annotate(
        tasks_count=Count('task')
    ).filter(tasks_count=0).delete()
    if total > 0 and 'app.Project' in count_dict:
        logger.info("Deleted {} projects".format(count_dict['app.Project']))

@app.task(ignore_result=True)
def cleanup_tasks():
    # Delete tasks that are older than 
    if settings.CLEANUP_PARTIAL_TASKS is None:
        return
    
    tasks_to_delete = Task.objects.filter(partial=True, created_at__lte=timezone.now() - timedelta(hours=settings.CLEANUP_PARTIAL_TASKS))
    for t in tasks_to_delete:
        logger.info("Cleaning up partial task {}".format(t))
        t.delete()

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

            logger.info('Cleaned up: %s (%s)' % (f, modified))


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
    lock_id = 'task_lock_{}'.format(taskId)
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
                logger.warning("Task {} has an expired lock! This could mean that WebODM is running out of memory. Check your server configuration.".format(taskId))

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
                    e, traceback.format_exc()))
            if settings.TESTING: raise e
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
    return Task.objects.filter(Q(processing_node__isnull=True, auto_processing_node=True, partial=False) |
                                Q(Q(status=None) | Q(status__in=[status_codes.QUEUED, status_codes.RUNNING]),
                                  processing_node__isnull=False, partial=False) |
                                Q(pending_action__isnull=False, partial=False))

@app.task(ignore_result=True)
def process_pending_tasks():
    tasks = get_pending_tasks()
    for task in tasks:
        process_task.delay(task.id)


@app.task(bind=True)
def export_raster(self, input, **opts):
    try:
        logger.info("Exporting raster {} with options: {}".format(input, json.dumps(opts)))
        tmpfile = tempfile.mktemp('_raster.{}'.format(extension_for_export_format(opts.get('format', 'gtiff'))), dir=settings.MEDIA_TMP)
        export_raster_sync(input, tmpfile, **opts)
        result = {'file': tmpfile}

        if settings.TESTING:
            TestSafeAsyncResult.set(self.request.id, result)

        return result
    except Exception as e:
        logger.error(str(e))
        return {'error': str(e)}

@app.task(bind=True)
def export_pointcloud(self, task_id, input: str, **opts):
    try:
        logger.info("Exporting point cloud {} with options: {}".format(input, json.dumps(opts)))
        tmpfile = tempfile.mktemp('_pointcloud.{}'.format(opts.get('format', 'laz')), dir=settings.MEDIA_TMP)

        task = Task.objects.get(pk=task_id)
        task_assets_manager = TaskAssetsManager(task)
        tmpfile_input_s3 = task_assets_manager.download_asset_to_temp(input)

        export_pointcloud_sync(tmpfile_input_s3, tmpfile, **opts)
        result = {'file': tmpfile}

        if settings.TESTING:
            TestSafeAsyncResult.set(self.request.id, result)

        return result
    except Exception as e:
        logger.error(str(e))
        return {'error': str(e)}

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
                logger.info("Quota deadline expired for %s, deleting tasks" % str(p.user.username))
                task_count = Task.objects.filter(project__owner=p.user).count()
                c = 0

                while p.has_exceeded_quota():
                    try:
                        last_task = Task.objects.filter(project__owner=p.user).order_by("-created_at").first()
                        if last_task is None:
                            break
                        logger.info("Deleting %s" % last_task)
                        last_task.delete()
                    except Exception as e:
                        logger.warn("Cannot delete %s for %s: %s" % (str(last_task), str(p.user.username), str(e)))
                    
                    c += 1
                    if c >= task_count:
                        break
        else:
            p.clear_quota_deadline()


@app.task(bind=True)
def task_upload_file(self, task_id, files_to_upload, s3_images, upload_type):
    try:
        logger.info(f"Start upload files {files_to_upload} and s3 images {s3_images} of type {upload_type} to task {task_id}")

        uploader = TaskFilesUploader(task_id)
        result = uploader.upload_files(files_to_upload, s3_images, upload_type)
        logger.info(f'upload task finished with result {result}')

        if settings.TESTING:
            TestSafeAsyncResult.set(self.request.id, result)

        return { 'output': result }
    except Exception as e:
        logger.error(str(e))
        logger.info(f'upload task finished with error {str(e)}')
        return {'error': str(e)}


@app.task(bind=True)
def generate_zip_from_dir(self, task_id, asset):
    try:
        logger.info(f"Start generate zip from {asset}")
        tmpfile = tempfile.mktemp('.zip', dir=settings.MEDIA_TMP)

        task = Task.objects.get(pk=task_id)
        asset_zip = task.ASSETS_MAP[asset]
        zip_dir = os.path.abspath(task.assets_path(asset_zip['deferred_compress_dir']))
        s3_dir = convert_task_path_to_s3(zip_dir)

        s3_objects = list_s3_objects(s3_dir)
        s3_keys = [s3_object['Key'] for s3_object in s3_objects]
        
        logger.info(f'Found these files to add in zip: {s3_keys}')
        task_assets_manager = TaskAssetsManager(task)
        for s3_key in s3_keys:
            local_path = os.path.join(settings.MEDIA_ROOT, s3_key)
            task_assets_manager.download_asset_to_temp(local_path)

        logger.info('downloaded all files')
        paths = [{'n': os.path.relpath(os.path.join(dp, f), zip_dir), 'fs': os.path.join(dp, f)} for dp, dn, filenames in os.walk(zip_dir) for f in filenames]
        if 'deferred_exclude_files' in asset_zip and isinstance(asset_zip['deferred_exclude_files'], tuple):
            paths = [p for p in paths if os.path.basename(p['fs']) not in asset_zip['deferred_exclude_files']]
        if len(paths) == 0:
            raise FileNotFoundError("No files available for download")

        logger.info(f'creating zip with {paths}')
        zip_stream = zipfly.ZipStream(paths)

        logger.info(f'writing zip in {tmpfile}')
        with open(tmpfile, 'wb') as zip_file:
            zip_stream.lazy_load(1024)
            for zip_data in zip_stream.generator:
                zip_file.write(zip_data)

        result = {'file': tmpfile}

        if settings.TESTING:
            TestSafeAsyncResult.set(self.request.id, result)

        return result
    except Exception as e:
        logger.error(str(e))
        logger.info(f'upload task finished with error {str(e)}')
        return {'error': str(e)}
