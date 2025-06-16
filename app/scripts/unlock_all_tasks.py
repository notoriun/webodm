from worker import tasks
from app.utils.redis_client_factory import get_redis_client

redis_client = get_redis_client()

for task in tasks.get_pending_tasks():
    msg = "Unlocking {}... ".format(task)
    res = redis_client.delete("task_lock_{}".format(task.id))
    print(msg + ("OK" if res else "Already unlocked"))
