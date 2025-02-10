import json
import os

from django.core.cache import caches

from webodm import settings
from time import sleep
from contextlib import contextmanager

cache_files_queue_key = "s3_cache_files_queue"
cache_files_lock_key = "s3_cache_files_lock"


def set_files_in_cache(files: list[str]):
    redis_cache = _get_redis_cache()

    files_cache_str = json.dumps(files)
    redis_cache.set(
        cache_files_queue_key,
        files_cache_str,
        timeout=settings.S3_IMAGES_CACHE_KEYS_REFRESH_SECONDS + 1,
    )


def get_files_in_cache() -> list[str]:
    redis_cache = _get_redis_cache()

    files_in_cache = redis_cache.get(cache_files_queue_key)

    if not files_in_cache:
        return []

    return json.loads(files_in_cache)


def has_file_in_cache(file: str):
    files = get_files_in_cache()
    return file in files


def update_file_in_cache(file: str):
    files_in_cache = get_files_in_cache()

    if file in files_in_cache:
        files_without_new = [f for f in files_in_cache if f != file]
        new_cache = [file] + files_without_new
    else:
        new_cache = [file] + files_in_cache

    set_files_in_cache(new_cache)

    return new_cache


def remove_file_from_cache(file: str):
    files_in_cache = get_files_in_cache()
    cache_without_file = [f for f in files_in_cache if f != file]

    set_files_in_cache(cache_without_file)


def get_current_cache_size():
    current_size = 0

    for filepath in get_files_in_cache():
        try:
            file_stat = os.stat(filepath)
            current_size += file_stat.st_size
        except OSError:
            pass

    return current_size


def get_max_cache_size():
    return settings.S3_CACHE_MAX_SIZE_MB * 1024 * 1024


def get_files_with_old_accessed_first():
    all_files = get_files_in_cache()
    last_index = len(all_files) - 1

    return [all_files[last_index - i] for i in range(last_index + 1)]


def refresh_cache():
    redis_cache = _get_redis_cache()

    redis_cache.touch(
        cache_files_queue_key, timeout=settings.S3_IMAGES_CACHE_KEYS_REFRESH_SECONDS + 1
    )


@contextmanager
def cache_lock(key: str, timeout=10):
    redis_cache = _get_redis_cache()

    try:
        while True:
            acquired = redis_cache.add(key, "locked", timeout=timeout)

            if not acquired:
                sleep(1)
            else:
                yield True
                break
    finally:
        if acquired:
            redis_cache.delete(key)


def s3_cache_lock(timeout=10):
    return cache_lock(cache_files_lock_key, timeout)


def _get_redis_cache():
    return caches["s3_images_cache"]
