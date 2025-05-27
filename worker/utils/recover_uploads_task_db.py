import os
from datetime import datetime, timedelta
from webodm import settings
from app.classes.file_dict import FileDict


class RecoverUploadsTaskDb:
    def __init__(self):
        upload_db_path = os.path.join(settings.MEDIA_ROOT, "recover_uploads_task.db")
        self._uploads_db = FileDict(upload_db_path)

    def set_task(self, task_id: str, celery_id: str):
        task_dict = self.get_by_celery(celery_id, {})

        task_dict["task_id"] = task_id
        task_dict["set_task_date"] = task_dict.get(
            "set_task_date", datetime.now().isoformat()
        )

        self._uploads_db.set(celery_id, task_dict)

    def set_secondary_celery_task(self, secondary_celery_task_id: str, task_id: str):
        key, task_dict = self.get_by_task(task_id, None)

        if not task_dict:
            return

        task_dict["secondary_celery_task_id"] = secondary_celery_task_id

        self._uploads_db.set(key, task_dict)

    def remove_by_task(self, task_id: str):
        key, _ = self.get_by_task(task_id)
        self._uploads_db.remove(key)

    def get_task_by_celery(self, celery_id: str, default=None):
        task_dict = self.get_by_celery(celery_id)
        return task_dict.get("task_id", default) if task_dict else default

    def get_secondary_celery_task_by_celery(self, celery_id: str, default=None):
        task_dict = self.get_by_celery(celery_id)
        return (
            task_dict.get("secondary_celery_task_id", default) if task_dict else default
        )

    def get_by_celery(self, celery_id: str, default=None):
        return self._uploads_db.get(celery_id, default)

    def get_by_task(self, task_id: str, default=None):
        db_dict = self._uploads_db.data_dict()

        for celery_id in db_dict:
            celery_dict = db_dict[celery_id]
            if celery_dict.get("task_id", None) == task_id:
                return celery_id, celery_dict

        return None, default

    def clear_old_tasks_from_db(self):
        uploads_dict = self._uploads_db.data_dict()
        for task_id in uploads_dict:
            task_dict = uploads_dict[task_id]
            set_task_date_iso = task_dict.get("set_task_date", None)

            if not set_task_date_iso:
                task_dict["set_task_date"] = datetime.now().isoformat()
                self._uploads_db.set(task_id, task_dict)
                continue

            set_task_date = datetime.fromisoformat(set_task_date_iso)
            agora = datetime.now()

            if (agora - set_task_date) < timedelta(
                minutes=settings.UPLOADING_STORAGE_TASK_RESULT_TTL_MINUTES
            ):
                continue

            self._uploads_db.remove(task_id)
