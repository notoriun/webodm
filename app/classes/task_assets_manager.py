import os
import logging
import tempfile
import worker.cache_files as worker_cache_files_tasks

from app.models import Task, TaskAsset
from app.utils.s3_utils import (
    convert_task_path_to_s3,
    get_s3_object,
    append_s3_bucket_prefix,
    download_s3_file,
    split_s3_bucket_prefix,
)
from app.utils.file_utils import ensure_sep_at_end, remove_path_from_path
from webodm import settings

logger = logging.getLogger("app.logger")


class TaskAssetsManager:
    def __init__(self, task: Task):
        self.task = task

    def get_asset_stream(self, path: str, chunk_size=1024):
        local_path = self.task.assets_path(path)
        return self._generate_stream(local_path, chunk_size=chunk_size)

    def get_image_stream(self, path, chunk_size=1024):
        local_path = self.task.get_image_path(path)
        return self._generate_stream(local_path, chunk_size=chunk_size)

    def download_asset(self, src_path: str, dst_path: str = None):
        destiny_path = dst_path if dst_path else src_path
        s3_bucket, s3_key = self._asset_s3_key(src_path)

        if os.path.exists(src_path):
            self._refresh_file_in_cache(s3_key, s3_bucket)

            return src_path

        source_path = src_path.replace(ensure_sep_at_end(settings.MEDIA_ROOT), "")
        download_s3_file(source_path, destiny_path)
        downloaded_file = os.path.exists(destiny_path)

        if not downloaded_file:
            return None

        self._move_and_add_to_cache(s3_key, destiny_path, s3_bucket)

        return destiny_path

    def download_asset_to_temp(self, src_path: str):
        tmp_file = tempfile.mktemp(
            f"_{os.path.basename(src_path)}", dir=settings.MEDIA_TMP
        )
        return self.download_asset(src_path, tmp_file)

    def _generate_stream(self, path: str, chunk_size=1024):
        s3_bucket, s3_key = self._asset_s3_key(path)

        if os.path.exists(path):
            return self._stream_file(path, s3_key, s3_bucket, chunk_size=chunk_size)

        s3_object = get_s3_object(s3_key, bucket=s3_bucket)

        if not s3_object:
            return None

        return self._stream_s3_object(
            s3_object, s3_key, s3_bucket, chunk_size=chunk_size
        )

    def _stream_file(self, filepath: str, s3_key: str, s3_bucket: str, chunk_size=1024):
        with open(filepath, "rb") as file:
            while chunk := file.read(chunk_size):
                yield chunk

        self._refresh_file_in_cache(s3_key, s3_bucket)

    def _stream_s3_object(
        self, s3_object, s3_key: str, s3_bucket: str, chunk_size=1024
    ):
        for chunk in s3_object["Body"].iter_chunks(chunk_size=chunk_size):
            yield chunk

        self._download_and_add_to_cache(s3_key, s3_bucket)

    def _refresh_file_in_cache(self, s3_key: str, s3_bucket: str):
        s3_file = append_s3_bucket_prefix(s3_key, bucket=s3_bucket)
        worker_cache_files_tasks.refresh_file_in_cache.delay(s3_file)

    def _download_and_add_to_cache(self, s3_key: str, s3_bucket: str):
        s3_file = append_s3_bucket_prefix(s3_key, bucket=s3_bucket)
        worker_cache_files_tasks.download_and_add_to_cache.delay(s3_file)

    def _move_and_add_to_cache(self, s3_key: str, file_to_move: str, s3_bucket: str):
        s3_file = append_s3_bucket_prefix(s3_key, bucket=s3_bucket)
        worker_cache_files_tasks.move_file_and_add_in_cache.delay(s3_file, file_to_move)

    def _task_asset(self, asset_local_path: str):
        asset_path_after_task_asset = self.task.reverse_parse_asset_path(
            remove_path_from_path(asset_local_path, self.task.assets_path())
        )

        try:
            return TaskAsset.objects.get(
                task=self.task, name=asset_path_after_task_asset
            )
        except TaskAsset.DoesNotExist:
            return None

    def _asset_s3_key(self, asset_local_path: str):
        task_asset = self._task_asset(asset_local_path)

        if task_asset is None:
            return settings.S3_BUCKET, convert_task_path_to_s3(asset_local_path)

        if task_asset.is_from_s3():
            return split_s3_bucket_prefix(task_asset.origin_path)

        return settings.S3_BUCKET, convert_task_path_to_s3(task_asset.path())
