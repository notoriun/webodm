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
            logger.info(f"Start to add {file} in cache")
            s3_path = remove_s3_bucket_prefix(file)
            s3_object = get_s3_object_metadata(s3_path)

            if not s3_object:
                logger.info(f"not found {s3_path} aborting download and add to cache")
                return

            logger.info(f"headed object {file} in s3")
            filename = get_file_name(s3_path)
            file_dir = os.path.join(settings.MEDIA_ROOT, s3_path.replace(filename, ""))
            filepath = os.path.join(file_dir, filename)

            logger.info(f"check if has {file} in cache")
            if has_file_in_cache(filepath) and _s3_file_is_equals_to_cache_file(
                s3_path, filepath
            ):
                return

            file_size = s3_object.get("ContentLength", 0)
            logger.info(f"the {file} has {file_size} B")

            logger.info(f"check can add {file} to cache")
            can_add_to_cache = _check_cache_has_space(file_size)

            if not can_add_to_cache:
                return

            ensure_path_exists(file_dir)

            file_already_exists = os.path.isfile(filepath)

            if (file_already_exists and overide_local_file) or not file_already_exists:
                logger.info(f"downloading {file} to {filepath}")
                download_s3_file(s3_path, filepath)

            new_cache = update_file_in_cache(filepath)

            logger.info(f"new cache after download {new_cache}")
    except Exception as e:
        logger.error(str(e))


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

        logger.info(f"check can add {file_s3} to cache")
        can_add_to_cache = _check_cache_has_space(file_size)

        if not can_add_to_cache:
            return

        ensure_path_exists(file_dir)

        shutil.move(file_to_move, filepath)

        with s3_cache_lock():
            new_cache = update_file_in_cache(filepath)

        logger.info(f"new cache after move {new_cache}")
    except Exception as e:
        logger.error(str(e))


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

            logger.info(f"tasks downloaded files {tasks_downloaded_files}")

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

            logger.info(
                f"refreshed files in cache, current files {get_files_in_cache()}"
            )

        max_cache_size = get_max_cache_size()
        cache_available_size = human_readable_size(
            max_cache_size - get_current_cache_size()
        )
        logger.info(
            f"\n**Cache available space: {cache_available_size} / {human_readable_size(max_cache_size)}\n"
        )
    except:
        pass


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
