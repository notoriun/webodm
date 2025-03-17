from app.models import Task, Project


class DirFiles:
    def __init__(self, files: list[str]):
        self.files = files


class TaskDirFiles:
    def __init__(self, task_id: str, dir_files: DirFiles):
        self.task_id = task_id
        self.dir_files = dir_files

    def exists_on_db(self):
        return Task.objects.filter(pk=self.task_id).exists()


class ProjectDirFiles:
    def __init__(self, project_id: str, tasks: list[TaskDirFiles]):
        self.project_id = project_id
        self.tasks = tasks

    def exists_on_db(self):
        return Project.objects.filter(pk=self.project_id).exists()

    def all_files(self):
        files: list[str] = []

        for task in self.tasks:
            files += task.dir_files.files

        return files
