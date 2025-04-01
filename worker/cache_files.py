import os
import shutil

from celery.utils.log import get_task_logger
from django.db.models import Q

from .celery import app
from app.utils.file_utils import (
    ensure_path_exists,
    get_file_name,
    get_all_files_in_dir,
    human_readable_size,
    calculate_sha256,
)
from worker.utils.redis_file_cache import (
    get_max_cache_size,
    get_current_cache_size,
    get_files_with_old_accessed_first,
    update_file_in_cache,
    refresh_cache,
    has_file_in_cache,
    get_files_in_cache,
    remove_file_from_cache,
    s3_cache_lock,
)
from nodeodm import status_codes
from webodm import settings

logger = get_task_logger("app.logger")


@app.task()
def download_and_add_to_cache(file: str, overide_local_file=True):
    from app.utils.s3_utils import (
        remove_s3_bucket_prefix,
        download_s3_file,
        get_s3_object_metadata,
    )

    try:
        with s3_cache_lock():
            logger.debug(f"Start to add {file} in cache")
            s3_path = remove_s3_bucket_prefix(file)
            s3_object = get_s3_object_metadata(s3_path)

            if not s3_object:
                logger.debug(f"Not found {s3_path} aborting download and add to cache")
                return

            filename = get_file_name(s3_path)
            file_dir = os.path.join(settings.MEDIA_ROOT, s3_path.replace(filename, ""))
            filepath = os.path.join(file_dir, filename)

            if has_file_in_cache(filepath) and _s3_file_is_equals_to_cache_file(
                s3_path, filepath
            ):
                logger.debug(f"{file} already in cache. Exiting...")
                return

            file_size = s3_object.get("ContentLength", 0)

            can_add_to_cache = _check_cache_has_space(file_size)

            if not can_add_to_cache:
                logger.debug(
                    f"Can not add {file} to cache, not enough space. Exiting..."
                )
                return

            ensure_path_exists(file_dir)

            file_already_exists = os.path.isfile(filepath)

            if (file_already_exists and overide_local_file) or not file_already_exists:
                download_s3_file(s3_path, filepath)

            update_file_in_cache(filepath)

            logger.debug(f"Added {file} to cache with success!")
    except Exception as e:
        logger.error(f"Error on add {file} to cache. Original error: {str(e)}")


@app.task()
def refresh_file_in_cache(file: str):
    from app.utils.s3_utils import remove_s3_bucket_prefix

    s3_path = remove_s3_bucket_prefix(file)

    filename = get_file_name(s3_path)
    file_dir = os.path.join(settings.MEDIA_ROOT, s3_path.replace(filename, ""))
    filepath = os.path.join(file_dir, filename)

    if os.path.exists(filepath):
        with s3_cache_lock():
            update_file_in_cache(filepath)


@app.task()
def move_file_and_add_in_cache(file_s3: str, file_to_move):
    from app.utils.s3_utils import remove_s3_bucket_prefix

    try:
        s3_path = remove_s3_bucket_prefix(file_s3)

        filename = get_file_name(s3_path)
        file_dir = os.path.join(settings.MEDIA_ROOT, s3_path.replace(filename, ""))
        filepath = os.path.join(file_dir, filename)
        file_stat = os.stat(file_to_move)
        file_size = file_stat.st_size

        can_add_to_cache = _check_cache_has_space(file_size)

        if not can_add_to_cache:
            logger.debug(
                f"Can not add {file_s3} to cache, not enough space. Exiting..."
            )
            return

        ensure_path_exists(file_dir)

        shutil.move(file_to_move, filepath)

        with s3_cache_lock():
            new_cache = update_file_in_cache(filepath)

        logger.debug(f"Added {new_cache} on cache with success!")
    except Exception as e:
        logger.error(
            f"Error on move {file_s3} and add to cache. Original error: {str(e)}"
        )


@app.task()
def refresh_file_cache_keys():
    from app.models import Task

    downloads_root = settings.MEDIA_ROOT

    refresh_cache()

    try:
        with s3_cache_lock():
            completed_tasks = Task.objects.filter(
                Q(status=status_codes.COMPLETED)
            ).values_list("pk", flat=True)
            downloaded_files = get_all_files_in_dir(downloads_root)
            tasks_by_project: dict[int, list[int]] = {}
            tasks_downloaded_files = []

            for file in downloaded_files:
                splited_path = [
                    entry
                    for entry in file.replace(downloads_root, "").split(os.sep)
                    if len(entry) > 0
                ]

                if len(splited_path) < 2:
                    continue

                project_entry = splited_path[0]
                task_entry = splited_path[1]

                if (
                    project_entry in tasks_by_project
                    and task_entry in tasks_by_project.get(project_entry)
                ):
                    tasks_downloaded_files.append(
                        {"path": file, "project": project_entry, "task": task_entry}
                    )

            files_to_add_cache = [
                file["path"]
                for file in tasks_downloaded_files
                if file["task"] in completed_tasks
            ]

            for file_to_add in files_to_add_cache:
                update_file_in_cache(file_to_add)

            logger.info(
                f"Found all these files need to be in cache: {str(files_to_add_cache)}"
            )

            removeds_from_cache = []
            for file in get_files_in_cache():
                if not os.path.exists(file):
                    remove_file_from_cache(file)
                    removeds_from_cache.append(file)

        max_cache_size = get_max_cache_size()
        cache_available_size = human_readable_size(
            max_cache_size - get_current_cache_size()
        )
        logger.info(
            f"\n**Cache available space: {cache_available_size} / {human_readable_size(max_cache_size)}\n"
        )
    except:
        pass


@app.task()
def seek_and_populate_redis_cache():
    import shutil
    import os
    from worker.classes.local_files_redis import ProjectDirFiles

    logger.info("Starting to populate redis cache...")

    projects_on_media = list_media_projects()

    if len(projects_on_media) == 0:
        logger.info("Not found projects on media dir, exiting...")
        return

    projects_not_exists = []
    projects_exists: list[ProjectDirFiles] = []
    tasks_not_exists = []

    for project_path in projects_on_media:
        projects_dir_files = get_project_dir_files(project_path)

        if not projects_dir_files.exists_on_db():
            projects_not_exists.append(project_path)
            continue

        tasks_exists = []

        for task in projects_dir_files.tasks:
            if not task.exists_on_db():
                tasks_not_exists.append(os.path.join(project_path, task.task_id))
            else:
                tasks_exists.append(task)

        projects_exists.append(
            ProjectDirFiles(projects_dir_files.project_id, tasks_exists)
        )

    if len(projects_not_exists) > 0:
        logger.info(
            f"Not found these projects on DB {projects_not_exists}, removing paths..."
        )
        for project_path in projects_not_exists:
            shutil.rmtree(project_path)

    if len(tasks_not_exists) > 0:
        logger.info(
            f"Not found these tasks on DB {tasks_not_exists}, removing paths..."
        )
        for task_path in tasks_not_exists:
            shutil.rmtree(task_path)

    for project in projects_exists:
        for file in project.all_files():
            add_local_file_to_redis_cache.delay(file)


@app.task()
def add_local_file_to_redis_cache(file_path: str):
    import os

    logger.info(f"Starting to add {file_path} to redis cache...")

    try:
        file_stat = os.stat(file_path)
        file_size = file_stat.st_size

        can_add_to_cache = _check_cache_has_space(file_size)

        if not can_add_to_cache:
            os.remove(file_path)
            return

        with s3_cache_lock():
            update_file_in_cache(file_path)

    except Exception as e:
        logger.error(f"Error on set file({file_path}) on redis cache. Error: {str(e)}")


def _check_cache_has_space(space_need: int):
    max_cache_size = get_max_cache_size()

    if max_cache_size < space_need:
        return False

    cache_available_size = max_cache_size - get_current_cache_size()

    if cache_available_size < space_need:
        files_to_remove = []

        for cache_file in get_files_with_old_accessed_first():
            if not os.path.exists(cache_file):
                remove_file_from_cache(cache_file)
                continue

            file_stat = os.stat(cache_file)
            cache_available_size += file_stat.st_size
            files_to_remove.append(cache_file)

            if cache_available_size >= space_need:
                break

        for file_to_remove in files_to_remove:
            if os.path.exists(file_to_remove):
                os.remove(file_to_remove)

            remove_file_from_cache(file_to_remove)

    return True


def _s3_file_is_equals_to_cache_file(s3_key: str, cache_filepath: str):
    from app.utils.s3_utils import get_object_checksum

    current_checksum = calculate_sha256(cache_filepath)
    if not current_checksum:
        return False

    object_checksum = get_object_checksum(s3_key)

    return current_checksum == object_checksum


def list_media_projects():
    from app.utils.file_utils import list_dirs_in_dir

    ignore_paths = [
        os.path.join(settings.MEDIA_ROOT, "CACHE"),
        os.path.join(settings.MEDIA_ROOT, "plugins"),
        os.path.join(settings.MEDIA_ROOT, "settings"),
        os.path.join(settings.MEDIA_ROOT, "imports"),
        settings.MEDIA_TMP,
    ]

    try:
        return [
            path
            for path in list_dirs_in_dir(settings.MEDIA_ROOT)
            if not (path in ignore_paths)
        ]
    except Exception as e:
        logger.error(f"Error on get projects on media_dir. Error: {str(e)}")
        return []


def get_project_dir_files(project_path: str):
    from app.utils.file_utils import (
        list_dirs_in_dir,
        get_file_name,
        get_all_files_in_dir,
    )
    from worker.classes.local_files_redis import DirFiles, TaskDirFiles, ProjectDirFiles

    project_tasks: list[TaskDirFiles] = []

    for task_path in list_dirs_in_dir(project_path):
        try:
            task_id = get_file_name(task_path)

            task_files = DirFiles(get_all_files_in_dir(task_path))

            project_tasks.append(TaskDirFiles(task_id, task_files))
        except Exception as e:
            logger.error(f"Error on get file on task dir({task_path}). Error: {str(e)}")

    return ProjectDirFiles(get_file_name(project_path), project_tasks)
