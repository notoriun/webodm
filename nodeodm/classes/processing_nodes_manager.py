from app import pending_actions
from app.models import Task
from nodeodm import status_codes
from nodeodm.models import ProcessingNode

from django.conf import settings
from django.utils import timezone
from django.db.models import Count, Q
from logging import Logger
from datetime import timedelta


class ProcessingNodesManager:
    def __init__(self, logger: Logger):
        self._logger = logger

    def improve_processing_nodes_performance(self):
        self._logger.info("Start improve processing nodes performance")

        if not self._has_more_than_one_node():
            return

        self._remove_offline_nodes()
        self._update_queued_tasks_to_free_node()

    def _remove_offline_nodes(self):
        self._logger.info("Start to remove offline nodes")

        offline_nodes_id = self._offline_processing_nodes()

        if len(offline_nodes_id) == 0:
            self._logger.info("Not found offline nodes")
            return

        tasks_updated = self._restart_tasks_of_nodes(offline_nodes_id)

        self._logger.info(
            f"Found {tasks_updated} tasks with offline node, and updated to restart without node, for processing choose a new one"
        )

        result = self._delete_processing_nodes(offline_nodes_id)
        self._logger.info(f"Removed nodes {offline_nodes_id}.")

        return result

    def _offline_processing_nodes(self) -> list[str]:
        return [
            node.pk
            for node in ProcessingNode.find_maybe_offline_nodes()
            if node.confirm_is_offline()
        ]

    def _restart_tasks_of_nodes(self, nodes_id: list[str]):
        return Task.objects.filter(
            processing_node__in=nodes_id,
            status__in=(status_codes.RUNNING, status_codes.QUEUED),
        ).update(
            status=None,
            auto_processing_node=True,
            processing_node=None,
            pending_action=pending_actions.RESTART,
            last_error=None,
        )

    def _delete_processing_nodes(self, nodes_id: list[str]):
        return ProcessingNode.objects.filter(pk__in=nodes_id).delete()[0]

    def _update_queued_tasks_to_free_node(self):
        self._logger.info("Start to update node of queued tasks to a free node")

        queued_tasks = self._queued_tasks()
        queued_tasks_count = queued_tasks.count()

        if queued_tasks_count == 0:
            self._logger.info("Not found queued tasks")
            return

        free_nodes = self._free_nodes()
        free_nodes_count = free_nodes.count()

        if free_nodes_count == 0:
            self._logger.info("Not found free nodes")
            return

        self._logger.info(
            f"Found {free_nodes_count} free nodes and {queued_tasks_count} tasks in some queue. Reassinging.."
        )

        offline_nodes = []

        for task in queued_tasks:
            next_free_node = free_nodes.exclude(pk__in=offline_nodes).first()

            while next_free_node and next_free_node.confirm_is_offline():
                offline_nodes.append(next_free_node.pk)
                next_free_node = free_nodes.exclude(pk__in=offline_nodes).first()

            if not next_free_node:
                self._logger.info(
                    f"Not found free nodes more, maybe in some time it received a new task to process, exiting..."
                )
                return

            self._logger.info(f"Assingning {task} to {next_free_node}...")
            self._assign_task_to_node(task, next_free_node)

            removed = self._remove_task_from_your_node(task)

            if not removed:
                self._logger.info(
                    f"Cannot remove {task} from their node, pass to next task"
                )
                continue

            self._logger.info(
                f"Removed {task} from your node and assingning to {next_free_node}..."
            )
            self._assign_task_to_node(task, next_free_node)

    def _queued_tasks(self):
        return Task.objects.filter(status=status_codes.QUEUED)

    def _free_nodes(self):
        return ProcessingNode.objects.annotate(
            running_queued_tasks=Count(
                "task",
                filter=Q(task__status__in=(status_codes.RUNNING, status_codes.QUEUED)),
            ),
        ).filter(
            running_queued_tasks=0,
            last_refreshed__gte=timezone.now()
            - timedelta(minutes=settings.NODE_OFFLINE_MINUTES),
        )

    def _assign_task_to_node(self, task: Task, node: ProcessingNode):
        try:
            task.status = None
            task.auto_processing_node = True
            task.processing_node = node
            task.pending_action = pending_actions.RESTART
            task.last_error = None
            task.uuid = ""
            task.node_connection_retry = 0
            task.save()

            return True
        except:
            return False

    def _remove_task_from_your_node(self, task: Task):
        return task.remove_from_your_node()

    def _has_more_than_one_node(self):
        return ProcessingNode.objects.all().count() > 1
