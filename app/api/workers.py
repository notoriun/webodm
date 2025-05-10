import os
import mimetypes

from worker.tasks import TestSafeAsyncResult
from worker.utils.recover_uploads_task_db import RecoverUploadsTaskDb
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, permissions

from django.http import FileResponse
from django.http import HttpResponse
from wsgiref.util import FileWrapper


class CheckTask(APIView):
    permission_classes = (permissions.AllowAny,)

    def get(self, request, celery_task_id=None, **kwargs):
        result_from_recover = _get_status_from_recover_db(celery_task_id)

        if result_from_recover:
            return Response({"ready": result_from_recover["ready"]})

        res = TestSafeAsyncResult(celery_task_id)

        if not res.ready():
            return Response({"ready": False}, status=status.HTTP_200_OK)
        else:
            result = res.get()

            if result.get("error", None) is not None:
                msg = self.on_error(result)
                return Response({"ready": True, "error": msg})

            if self.error_check(result) is not None:
                msg = self.on_error(result)
                return Response({"ready": True, "error": msg})

            return Response({"ready": True})

    def on_error(self, result):
        return result["error"]

    def error_check(self, result):
        pass


class TaskResultOutputError(Exception):
    pass


class GetTaskResult(APIView):
    permission_classes = (permissions.AllowAny,)

    def get(self, request, celery_task_id=None, **kwargs):
        result_from_recover = _get_status_from_recover_db(celery_task_id)

        if result_from_recover:
            return Response(result_from_recover)

        res = TestSafeAsyncResult(celery_task_id)

        if res.failed():
            return Response({"ready": True, "error": str(res.info)})

        if res.ready():
            result = res.get()

            if result is None:
                return Response({"ready": True, "error": None})

            if result.get("error", None) is not None:
                msg = result["error"]
                return Response({"ready": True, "error": msg})

            file = result.get("file", None)  # File path
            output = result.get("output", None)  # String/object
        else:
            return Response({"ready": False, "error": "Task not ready"})

        if file is not None:
            filename = request.query_params.get("filename", os.path.basename(file))
            filesize = os.stat(file).st_size

            f = open(file, "rb")

            # More than 100mb, normal http response, otherwise stream
            # Django docs say to avoid streaming when possible
            stream = filesize > 1e8
            if stream:
                response = FileResponse(f)
            else:
                response = HttpResponse(
                    FileWrapper(f),
                    content_type=(
                        mimetypes.guess_type(filename)[0] or "application/zip"
                    ),
                )

            response["Content-Type"] = (
                mimetypes.guess_type(filename)[0] or "application/zip"
            )
            response["Content-Disposition"] = "attachment; filename={}".format(filename)
            response["Content-Length"] = filesize

            return response
        elif output is not None:
            try:
                output = self.handle_output(output, result, **kwargs)
            except TaskResultOutputError as e:
                return Response({"ready": True, "error": str(e)})

            return Response({"ready": True, "output": output})
        else:
            return Response(
                {"ready": True, "error": "Invalid task output (cannot find valid key)"}
            )

    def handle_output(self, output, result, **kwargs):
        return output


class GetCacheSize(APIView):
    permission_classes = (permissions.AllowAny,)

    def get(self, request, celery_task_id=None, **kwargs):
        from worker.cache_files import get_cache_sizes

        available_size, max_size, current_size = get_cache_sizes()

        return Response(
            {"available": available_size, "max": max_size, "current": current_size}
        )


def _get_status_from_recover_db(celery_id: str):
    from app.models import Task

    recover_db = RecoverUploadsTaskDb()
    secondary_celery_id = recover_db.get_secondary_celery_task_by_celery(
        celery_id, None
    )

    if secondary_celery_id:
        res = TestSafeAsyncResult(secondary_celery_id)

        if not res.ready():
            return {"ready": False, "error": "Task not ready"}

        response = res.get()

        if not response:
            return {"ready": True, "error": None}

        if response.get("error", None) is not None:
            msg = response["error"]
            return Response({"ready": True, "error": msg})

        output = response.get("output", None)  # String/object

        return {"ready": True, "output": output}

    task_id = recover_db.get_task_by_celery(celery_id)

    if not task_id:
        return None

    try:
        task = Task.objects.get(pk=task_id)
    except Task.DoesNotExist:
        return None

    return {"ready": task.upload_in_progress == False}
