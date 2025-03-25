from pyodm.types import TaskStatus

# Status code (10 = QUEUED, 20 = RUNNING, 30 = FAILED, 40 = COMPLETED, 50 = CANCELED)

QUEUED = TaskStatus.QUEUED.value
RUNNING = TaskStatus.RUNNING.value
FAILED = TaskStatus.FAILED.value
COMPLETED = TaskStatus.COMPLETED.value
CANCELED = TaskStatus.CANCELED.value
