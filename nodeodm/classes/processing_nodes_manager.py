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

        if self._has_more_than_one_node():
            self._remove_offline_nodes()

        self._clear_old_tasks_of_nodes()
        self._update_queued_tasks_to_free_node()

    def _remove_offline_nodes(self):
        offline_nodes_id = self._offline_processing_nodes()

        if len(offline_nodes_id) == 0:
            return

        tasks_updated = self._restart_tasks_of_nodes(offline_nodes_id)

        result = self._delete_processing_nodes(offline_nodes_id)
        self._logger.info(
            f"Removed offline nodes {offline_nodes_id}. And restarted {tasks_updated} tasks from this nodes."
        )

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
            partial=False,
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
        tasks_needs_other_node = self._queued_tasks_or_without_node()
        tasks_needs_other_node_count = tasks_needs_other_node.count()

        if tasks_needs_other_node_count == 0:
            return

        free_nodes = self._free_nodes()
        free_nodes_count = free_nodes.count()

        if free_nodes_count == 0:
            return

        offline_nodes = []

        for task in tasks_needs_other_node:
            next_free_node = free_nodes.exclude(pk__in=offline_nodes).first()

            while next_free_node and next_free_node.confirm_is_offline():
                offline_nodes.append(next_free_node.pk)
                next_free_node = free_nodes.exclude(pk__in=offline_nodes).first()

            if not next_free_node:
                return

            self._assign_task_to_node(task, next_free_node)

            removed = self._remove_task_from_your_node(task)

            if not removed:
                continue

            self._logger.info(
                f"Removed {task} from your node and assingning to {next_free_node}..."
            )
            self._assign_task_to_node(task, next_free_node)

    def _queued_tasks_or_without_node(self):
        return Task.objects.filter(
            Q(partial=False)
            & (
                Q(status=status_codes.QUEUED)
                | (
                    Q(processing_node__isnull=True)
                    & (Q(status__isnull=True) | Q(status=status_codes.FAILED))
                )
                | Q(
                    node_connection_retry__gte=settings.TASK_MAX_NODE_CONNECTION_RETRIES
                )
            )
        ).exclude(
            pending_action__in=(
                pending_actions.CANCEL,
                pending_actions.REMOVE,
                pending_actions.RESTART,
            )
        )

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
            current_status = task.status
            task.status = None
            task.auto_processing_node = True
            task.processing_node = node
            task.pending_action = (
                pending_actions.RESTART
                if current_status == status_codes.QUEUED
                else task.pending_action
            )
            task.last_error = None
            task.uuid = ""
            task.node_connection_retry = 0
            task.node_error_retry = 0
            task.save()

            return True
        except:
            return False

    def _remove_task_from_your_node(self, task: Task):
        return task.remove_from_your_node()

    def _has_more_than_one_node(self):
        return ProcessingNode.objects.all().count() > 1

    def _clear_old_tasks_of_nodes(self):
        nodes_with_greater_queues = ProcessingNode.objects.filter(
            last_refreshed__gte=timezone.now()
            - timedelta(minutes=settings.NODE_OFFLINE_MINUTES),
            queue_count__gt=0,
        ).order_by("-queue_count")

        for node in nodes_with_greater_queues:
            queued_or_running_tasks = list(node.list_queued_or_running_tasks())

            if len(queued_or_running_tasks) == 0:
                continue

            queued_or_running_uuids: list[str] = [
                task.uuid for task in queued_or_running_tasks
            ]

            webodm_tasks = self._list_tasks_with(queued_or_running_uuids)
            existing_tasks = (task.uuid for task in webodm_tasks)

            uuids_not_exists_on_webodm = [
                id for id in queued_or_running_uuids if id not in existing_tasks
            ]

            if len(uuids_not_exists_on_webodm) == 0:
                continue

            tasks_removeds = []
            for uuid_not_exists in uuids_not_exists_on_webodm:
                try:
                    node.remove_task(uuid_not_exists)

                    tasks_removeds.append(uuid_not_exists)
                except Exception as e:
                    self._logger.warning(
                        f"Warning cannot remove task {uuid_not_exists}. Original error: {str(e)}"
                    )

            self._logger.info(
                f"Removed old tasks of ids ({str(tasks_removeds)}) from node {node}"
            )

    def _list_tasks_with(self, uuids: list[str]):
        return Task.objects.filter(uuid__in=uuids)
