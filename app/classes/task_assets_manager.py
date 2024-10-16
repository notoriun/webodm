import os
import logging
import worker.cache_files as worker_cache_files_tasks

from app.models import Task
from app.utils.s3_utils import convert_task_path_to_s3, get_s3_object, append_s3_bucket_prefix

logger = logging.getLogger('app.logger')

class TaskAssetsManager:
    def __init__(self, task: Task):
        self.task = task

    def get_asset_stream(self, path, chunk_size=1024):
        local_path = self.task.assets_path(path)
        return self._generate_stream(local_path, chunk_size=chunk_size)

    def get_image_stream(self, path, chunk_size=1024):
        local_path = self.task.get_image_path(path)
        return self._generate_stream(local_path, chunk_size=chunk_size)

    def _generate_stream(self, path: str, chunk_size=1024):
        s3_key = convert_task_path_to_s3(path)

        if os.path.exists(path):
            return self._stream_file(path, s3_key, chunk_size=chunk_size)

        s3_object = get_s3_object(s3_key)

        if not s3_object:
            return None

        return self._stream_s3_object(s3_object, s3_key, chunk_size=chunk_size)

    def _stream_file(self, filepath: str, s3_key: str, chunk_size=1024):
        with open(filepath, 'rb') as file:
            while chunk := file.read(chunk_size):
                yield chunk

        s3_file = append_s3_bucket_prefix(s3_key)
        worker_cache_files_tasks.refresh_file_in_cache.delay(s3_file)

    def _stream_s3_object(self, s3_object, s3_key: str, chunk_size=1024):
        for chunk in s3_object['Body'].iter_chunks(chunk_size=chunk_size):
            yield chunk

        s3_file = append_s3_bucket_prefix(s3_key)
        worker_cache_files_tasks.download_and_add_to_cache.delay(s3_file)
