from app.models import Task, Project
from app.utils import file_utils


class DirFiles:
    def __init__(self, files: list[str]):
        self.files = files


class TaskDirFiles:
    def __init__(self, task_id: str, dir_files: DirFiles):
        self.task_id = task_id
        self.dir_files = dir_files

    def exists_on_db(self):
        return self._task_query().exists()

    def clear_task_dir(self):
        task = self._task_query().first()

        if not task:
            return True

        return task.clear_empty_dirs()

    def _task_query(self):
        return Task.objects.filter(pk=self.task_id)


class ProjectDirFiles:
    def __init__(self, project_path: str, tasks: list[TaskDirFiles]):
        self.project_path = project_path
        self.project_id = file_utils.get_file_name(project_path)
        self.tasks = tasks

    def exists_on_db(self):
        return Project.objects.filter(pk=self.project_id).exists()

    def all_files(self):
        files: list[str] = []

        for task in self.tasks:
            files += task.dir_files.files

        return files

    def clear_project_dir(self):
        return file_utils.delete_empty_dirs(self.project_path)
