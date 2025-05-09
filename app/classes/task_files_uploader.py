import logging

from app import task_asset_type, task_asset_status
from app.models import Task, TaskAsset
from app.utils.file_utils import get_file_name
from worker import cache_files as worker_cache_files_tasks
from django.utils.translation import gettext_lazy as _

logger = logging.getLogger("app.logger")


class TaskFilesUploader:
    def __init__(self, task_id):
        self._task_id = task_id
        self._task_loaded: Task = None

    @property
    def task(self):
        if not self._task_loaded:
            self._refresh_task()

        return self._task_loaded

    def upload_files(
        self,
        local_files_to_upload: list[dict[str, str]],
        s3_files_to_upload: list[str],
        upload_type: str,
        ignore_upload_to_s3=False,
    ):
        try:
            self.task_upload_in_progress(True)

            all_files_uploadeds = self._parse_uploaded_files(
                local_files_to_upload, s3_files_to_upload
            )
            self.task.console += (
                f"Starting upload images to webodm. Images: {all_files_uploadeds}"
            )

            task_asset_upload_type = self._parse_upload_type(upload_type)
            response = self._create_task_assets(
                all_files_uploadeds, task_asset_upload_type, ignore_upload_to_s3
            )

            if task_asset_upload_type == task_asset_type.ORTHOPHOTO:
                self.task.refresh_from_db()
                self.task.images_count = len(self.task.scan_images())
                self.task.s3_images = list(
                    set(s3_files_to_upload).union(self.task.s3_images)
                )
                self.task.save(update_fields=["s3_images", "images_count"])

            self.task_upload_in_progress(False)
            self.task.console += "Finished upload images to webodm"

            return response
        except Exception as e:
            self.task_upload_in_progress(False)
            raise e

    def task_upload_in_progress(self, in_progress):
        self._update_task(upload_in_progress=in_progress)
        self.task.refresh_from_db()

    def upload_foto360(
        self, local_files: list[str], s3_files: list[str], ignore_upload_to_s3=False
    ):
        files = self._parse_uploaded_files([], local_files + s3_files)
        return self.upload_files(files, [], "foto360")

    def _refresh_task(self):
        self._task_loaded = Task.objects.get(pk=self._task_id)

    def _concat_to_available_assets(self, assets: list[TaskAsset]):
        TaskAsset.objects.filter(pk__in=(asset.pk for asset in assets)).update(
            status=task_asset_status.SUCCESS
        )

    def _update_task(self, *args, **kwargs):
        Task.objects.filter(pk=self.task.pk).update(**kwargs)
        self.task.refresh_from_db()

    def _upload_task_assets_to_s3(self, assets: list[TaskAsset]):
        self.task.upload_and_cache_assets(assets)

    def _upload_task_asset(self, uploaded_file: dict[str, str], asset_type: int):
        task_asset, _ = TaskAsset.objects.update_or_create(
            type=asset_type,
            task=self.task,
            origin_path=uploaded_file["path"],
            defaults={
                "status": task_asset_status.PROCESSING,
            },
        )
        task_asset = task_asset.copy_to_type()

        is_valid_or_error = task_asset.is_valid()

        if is_valid_or_error != True:
            filename = uploaded_file["name"]
            logger.debug(
                f'Error on upload file "{filename}". Original error: {is_valid_or_error}'
            )

            task_asset.status = task_asset_status.ERROR
            task_asset.save(update_fields=("status",))

            return task_asset, is_valid_or_error

        task_asset.generate_name(uploaded_file)

        if task_asset.name is None:
            task_asset.status = task_asset_status.ERROR
            task_asset.save()

            return task_asset, "FILE_NAME_NOT_FOUND"

        file_created = task_asset.create_asset_file_on_task()

        if file_created is None:
            task_asset.status = task_asset_status.ERROR

        task_asset.save()

        return task_asset, None if file_created else "CANNOT_SAVE_FILE_ON_DISK"

    def _parse_upload_type(self, upload_type: str):
        if upload_type == "foto":
            return task_asset_type.FOTO
        elif upload_type == "video":
            return task_asset_type.VIDEO
        elif upload_type == "foto360":
            return task_asset_type.FOTO_360
        elif upload_type == "foto_giga":
            return task_asset_type.FOTO_GIGA
        else:  # Default to 'orthophoto'
            return task_asset_type.ORTHOPHOTO

    def _parse_uploaded_files(
        self, uploadeds_saved_local: list[dict[str, str]], uploadeds_saved_s3: list[str]
    ):
        return uploadeds_saved_local + [
            {
                "path": filepath,
                "name": get_file_name(filepath),
            }
            for filepath in uploadeds_saved_s3
        ]

    def _create_task_assets(
        self,
        all_files_uploadeds: list[dict[str, str]],
        asset_type: int,
        ignore_upload_to_s3=False,
    ):
        assets_uploadeds: list[TaskAsset] = []
        files_success = []
        files_with_error = {}
        percent_per_file = 1.0 / len(all_files_uploadeds) if all_files_uploadeds else 1
        progress = 0

        for file_uploaded in all_files_uploadeds:
            task_asset, upload_error = self._upload_task_asset(
                file_uploaded, asset_type
            )

            filename = file_uploaded["name"]

            if task_asset.status == task_asset_status.ERROR:
                files_with_error[filename] = upload_error or "UNKNOW_ERROR"

                self.task.console += f"[{progress * 100:.2f}%] - Cannot upload file {file_uploaded}. ERROR: {files_with_error[filename]}"
            else:
                progress += percent_per_file
                files_success.append(filename)
                assets_uploadeds.append(task_asset)

                self.task.console += (
                    f"[{progress * 100:.2f}%] - File {file_uploaded} upload success"
                )

        if asset_type != task_asset_type.ORTHOPHOTO:
            if not ignore_upload_to_s3:
                self._upload_task_assets_to_s3(assets_uploadeds)
            else:
                for asset in assets_uploadeds:
                    worker_cache_files_tasks.add_local_file_to_redis_cache.delay(
                        asset.path()
                    )
            self._concat_to_available_assets(assets_uploadeds)

        return {
            "success": len(files_with_error) == 0,
            "uploaded": files_success,
            "files_with_error": files_with_error,
        }
