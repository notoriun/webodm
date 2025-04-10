import logging

import os
import re
import shutil

import mimetypes

from shutil import copyfileobj
from django.core.exceptions import (
    ObjectDoesNotExist,
    SuspiciousFileOperation,
    ValidationError,
)
from django.core.files.uploadedfile import InMemoryUploadedFile, UploadedFile
from django.db import transaction
from django.http import HttpResponse
from rest_framework import (
    status,
    serializers,
    viewsets,
    filters,
    exceptions,
    permissions,
    parsers,
)
from rest_framework.decorators import action
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from app import models, pending_actions, task_asset_status, task_asset_type
from app.classes.task_assets_manager import TaskAssetsManager
from app.utils.file_utils import get_file_name
from app.utils.request_files_utils import save_request_file
from app.security import path_traversal_check
from nodeodm import status_codes
from nodeodm.models import ProcessingNode
from worker import tasks as worker_tasks
from .common import get_and_check_project, get_asset_download_filename
from .tags import TagsField
from django.utils.translation import gettext_lazy as _
from webodm import settings
from rest_framework.permissions import AllowAny
from PIL import Image
import re
import piexif
import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger("app.logger")

Image.MAX_IMAGE_PIXELS = None


def flatten_files(request_files) -> list[UploadedFile]:
    # MultiValueDict in, flat array of files out
    return [
        file
        for filesList in map(
            lambda key: request_files.getlist(key), [keys for keys in request_files]
        )
        for file in filesList
    ]


def is_360_photo(image_path):
    """
    Verifica se a imagem possui os metadados indicando que é uma foto 360 graus.
    """
    try:
        exif_dict = piexif.load(image_path)
        # Verifique os campos específicos que indicam uma foto 360
        if "Exif" in exif_dict and piexif.ExifIFD.UserComment in exif_dict["Exif"]:
            user_comment = exif_dict["Exif"][piexif.ExifIFD.UserComment]
            if b"360" in user_comment or b"Photo Sphere" in user_comment:
                return True
            logger.info(f"Foto {image_path} não é uma foto 360")
        return False
    except Exception as e:
        # print(f"Erro ao verificar metadados da imagem: {str(e)}")
        logger.error(f"Erro ao verificar metadados da imagem: {str(e)}")
        return False


def save_request_files(request_files: list[UploadedFile]):
    saved_paths: list[dict[str, str]] = []

    for file in request_files:
        saved_path = save_request_file(file)
        saved_paths.append(saved_path)

    return saved_paths


class TaskIDsSerializer(serializers.BaseSerializer):
    permission_classes = [AllowAny]
    authentication_classes = []

    def to_representation(self, obj):
        return obj.id


class TaskSerializer(serializers.ModelSerializer):
    permission_classes = [AllowAny]
    authentication_classes = []
    project = serializers.PrimaryKeyRelatedField(queryset=models.Project.objects.all())
    processing_node = serializers.PrimaryKeyRelatedField(
        queryset=ProcessingNode.objects.all()
    )
    processing_node_name = serializers.SerializerMethodField()
    can_rerun_from = serializers.SerializerMethodField()
    statistics = serializers.SerializerMethodField()
    available_assets = serializers.SerializerMethodField()
    status = serializers.SerializerMethodField()
    tags = TagsField(required=False)

    def get_processing_node_name(self, obj):
        if obj.processing_node is not None:
            return str(obj.processing_node)
        else:
            return None

    def get_statistics(self, obj):
        return obj.get_statistics()

    def get_can_rerun_from(self, obj):
        """
        When a task has been associated with a processing node
        and if the processing node supports the "rerun-from" parameter
        this method returns the valid values for "rerun-from" for that particular
        processing node.

        TODO: this could be improved by returning an empty array if a task was created
        and purged by the processing node (which would require knowing how long a task is being kept
        see https://github.com/OpenDroneMap/NodeODM/issues/32
        :return: array of valid rerun-from parameters
        """
        if obj.processing_node is not None:
            rerun_from_option = list(
                filter(
                    lambda d: "name" in d and d["name"] == "rerun-from",
                    obj.processing_node.available_options,
                )
            )
            if len(rerun_from_option) > 0 and "domain" in rerun_from_option[0]:
                return rerun_from_option[0]["domain"]

        return []

    def get_available_assets(self, obj):
        return (
            asset.name
            for asset in models.TaskAsset.sort_list(
                obj.assets.filter(status=task_asset_status.SUCCESS)
            )
        )

    def get_status(self, obj):
        return obj.status if obj.status is not None else 60

    class Meta:
        model = models.Task
        exclude = ("orthophoto_extent", "dsm_extent", "dtm_extent")
        read_only_fields = (
            "processing_time",
            "status",
            "last_error",
            "created_at",
            "pending_action",
            "available_assets",
            "size",
            "s3_images",
            "upload_in_progress",
        )


class TaskViewSet(viewsets.ViewSet):
    """
    Task get/add/delete/update
    A task represents a set of images and other input to be sent to a processing node.
    Once a processing node completes processing, results are stored in the task.
    """

    queryset = models.Task.objects.all().defer(
        "orthophoto_extent",
        "dsm_extent",
        "dtm_extent",
    )

    parser_classes = (
        parsers.MultiPartParser,
        parsers.JSONParser,
        parsers.FormParser,
    )
    ordering_fields = "__all__"

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        We don't use object level permissions on tasks, relying on
        project's object permissions instead (but standard model permissions still apply)
        and with the exception of 'retrieve' (task GET) for public tasks access
        """
        permission_classes = [permissions.AllowAny]
        # if self.action == 'retrieve':
        #    permission_classes = [permissions.AllowAny]
        # else:
        #    permission_classes = [permissions.DjangoModelPermissions, ]

        return [permission() for permission in permission_classes]

    def set_pending_action(
        self,
        pending_action,
        request,
        pk=None,
        project_pk=None,
        perms=("change_project",),
    ):
        get_and_check_project(request, project_pk, perms)
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        task.pending_action = pending_action
        task.partial = False  # Otherwise this will not be processed
        task.last_error = None
        task.node_connection_retry = 0
        task.node_error_retry = 0
        task.save()

        # Process task right away
        worker_tasks.process_task.delay(task.id)

        return Response({"success": True})

    @action(detail=True, methods=["post"])
    def cancel(self, *args, **kwargs):
        return self.set_pending_action(pending_actions.CANCEL, *args, **kwargs)

    @action(detail=True, methods=["post"])
    def restart(self, *args, **kwargs):
        return self.set_pending_action(pending_actions.RESTART, *args, **kwargs)

    @action(detail=True, methods=["post"])
    def remove(self, *args, **kwargs):
        return self.set_pending_action(
            pending_actions.REMOVE, *args, perms=("delete_project",), **kwargs
        )

    @action(detail=True, methods=["get"])
    def output(self, request, pk=None, project_pk=None):
        """
        Retrieve the console output for this task.
        An optional "line" query param can be passed to retrieve
        only the output starting from a certain line number.
        """
        get_and_check_project(request, project_pk)
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        line_num = max(0, int(request.query_params.get("line", 0)))
        return Response(
            "\n".join(task.console.output().rstrip().split("\n")[line_num:])
        )

    def list(self, request, project_pk=None):
        get_and_check_project(request, project_pk)
        tasks = self.queryset.filter(project=project_pk)
        tasks = filters.OrderingFilter().filter_queryset(self.request, tasks, self)
        serializer = TaskSerializer(tasks, many=True)
        return Response(serializer.data)

    def retrieve(self, request, pk=None, project_pk=None):
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        if not task.public:
            get_and_check_project(request, task.project.id)

        serializer = TaskSerializer(task)
        return Response(serializer.data)

    @action(detail=True, methods=["post"])
    def commit(self, request, pk=None, project_pk=None):
        """
        Commit a task after all images have been uploaded
        """
        get_and_check_project(request, project_pk, ("change_project",))
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        task.partial = False
        task.images_count = len(task.scan_images())

        if (
            task.images_count < 1
            and len(task.s3_images) < 1
            and not task.upload_in_progress
        ):
            raise exceptions.ValidationError(
                detail=_("You need to upload at least 1 file before commit")
            )

        task.update_size()
        task.save()
        worker_tasks.process_task.delay(task.id)

        serializer = TaskSerializer(task)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"])
    def upload(self, request, pk=None, project_pk=None, type=""):
        project = get_and_check_project(request, project_pk, ("change_project",))
        files = flatten_files(request.FILES)
        s3_images: str = request.data.get("images_download_s3", "")
        valid_s3_images = self._sanitize_s3_images(s3_images)

        if len(files) == 0 and len(valid_s3_images) == 0:
            raise exceptions.ValidationError(detail=_("No files uploaded"))

        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        try:
            upload_type = request.data.get("type", "orthophoto")
            files_paths = save_request_files(files)

            # Atualizar outros parâmetros como nó de processamento, nome da tarefa, etc.
            serializer = TaskSerializer(task, data=request.data, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()

            celery_task_id = worker_tasks.task_upload_file.delay(
                pk, files_paths, valid_s3_images, upload_type
            ).task_id
            response = {"celery_task_id": celery_task_id}
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)

        return Response(response, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"], url_path="set-s3-images")
    def set_s3_images(self, request, pk=None, project_pk=None):
        """
        Set s3 images to download on task proccess
        """
        get_and_check_project(request, project_pk, ("change_project",))
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        imagesParam: str = request.data.get("images", "")
        images = imagesParam.split(",")
        task.s3_images += [image.strip() for image in images if len(image.strip()) > 0]
        task.pending_action = (
            pending_actions.IMPORT_FROM_S3_WITH_RESIZE
            if task.pending_action == pending_actions.RESIZE
            else pending_actions.IMPORT_FROM_S3
        )
        task.save()
        return Response(
            {"success": True, "setted": task.s3_images}, status=status.HTTP_200_OK
        )

    @action(detail=True, methods=["post"], url_path="clear-s3-images")
    def clear_s3_images(self, request, pk=None, project_pk=None):
        """
        Set s3 images to download on task proccess
        """
        get_and_check_project(request, project_pk, ("change_project",))
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        task.s3_images = []
        task.save()
        return Response({"success": True}, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"])
    def duplicate(self, request, pk=None, project_pk=None):
        """
        Duplicate a task
        """
        get_and_check_project(request, project_pk, ("change_project",))
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        new_task = task.duplicate()
        if new_task:
            return Response(
                {"success": True, "task": TaskSerializer(new_task).data},
                status=status.HTTP_200_OK,
            )
        else:
            return Response(
                {"error": _("Cannot duplicate task")}, status=status.HTTP_200_OK
            )

    def create(self, request, project_pk=None):
        project = get_and_check_project(request, project_pk, ("change_project",))

        # If this is a partial task, we're going to upload images later
        # for now we just create a placeholder task.
        if request.data.get("partial"):
            task = models.Task.objects.create(
                project=project,
                pending_action=(
                    pending_actions.RESIZE if "resize_to" in request.data else None
                ),
            )
            serializer = TaskSerializer(task, data=request.data, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
        else:
            files = flatten_files(request.FILES)

            if len(files) <= 1:
                raise exceptions.ValidationError(
                    detail=_("Cannot create task, you need at least 2 images")
                )

            with transaction.atomic():
                task = models.Task.objects.create(
                    project=project,
                    pending_action=(
                        pending_actions.RESIZE if "resize_to" in request.data else None
                    ),
                )

                task.handle_images_upload(files)
                task.images_count = len(task.scan_images())

                # Update other parameters such as processing node, task name, etc.
                serializer = TaskSerializer(task, data=request.data, partial=True)
                serializer.is_valid(raise_exception=True)
                serializer.save()

                worker_tasks.process_task.delay(task.id)

        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def update(self, request, pk=None, project_pk=None, partial=False):
        get_and_check_project(request, project_pk, ("change_project",))
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        # Check that a user has access to reassign a project
        if "project" in request.data:
            try:
                get_and_check_project(
                    request, request.data["project"], ("change_project",)
                )
            except exceptions.NotFound:
                raise exceptions.PermissionDenied()

        serializer = TaskSerializer(task, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        serializer.save()

        # Process task right away
        worker_tasks.process_task.delay(task.id)

        return Response(serializer.data)

    def partial_update(self, request, *args, **kwargs):
        kwargs["partial"] = True
        return self.update(request, *args, **kwargs)

    def _sanitize_s3_images(self, s3_images_param: str):
        return [
            image.strip()
            for image in s3_images_param.split(",")
            if image.strip().startswith("s3://")
        ]


class TaskNestedView(APIView):
    queryset = models.Task.objects.all().defer(
        "orthophoto_extent",
        "dtm_extent",
        "dsm_extent",
    )
    permission_classes = (AllowAny,)

    def get_and_check_task(self, request, pk, annotate={}):
        try:
            task = self.queryset.annotate(**annotate).get(pk=pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        # Check for permissions, unless the task is public
        # if not task.public:
        #    get_and_check_project(request, task.project.id)

        return task


def download_file_stream(request, stream, content_disposition, download_filename=None):
    response = HttpResponse(
        stream,
        content_type=(mimetypes.guess_type(download_filename)[0] or "application/zip"),
    )

    response["Content-Type"] = (
        mimetypes.guess_type(download_filename)[0] or "application/zip"
    )
    response["Content-Disposition"] = "{}; filename={}".format(
        content_disposition, download_filename
    )

    # For testing
    response["_stream"] = "yes"

    return response


"""
Task downloads are simply aliases to download the task's assets
(but require a shorter path and look nicer the API user)
"""


class TaskDownloads(TaskNestedView):
    def get(self, request, pk=None, project_pk=None, asset=""):
        """
        Downloads a task asset (if available)
        """
        task = self.get_and_check_task(request, pk)
        asset_manager = TaskAssetsManager(task)

        # Verificar se é um pedido para DZI
        if asset.startswith("foto_giga") and asset.endswith(".dzi"):
            asset_stream = asset_manager.get_asset_stream(task.assets_path(asset))

            if not asset_stream:
                raise exceptions.NotFound(_("Asset does not exist"))

            content_disposition = "inline; filename={}".format(os.path.basename(asset))
            return download_file_stream(
                request, asset_stream, content_disposition, get_file_name(asset)
            )

        download_filename = request.GET.get(
            "filename", get_asset_download_filename(task, asset)
        )

        if task.is_asset_a_zip(asset):
            celery_task_id = worker_tasks.generate_zip_from_asset.delay(
                pk, asset
            ).task_id

            return Response(
                {"celery_task_id": celery_task_id, "filename": download_filename},
                status=status.HTTP_200_OK,
            )

        # Check and download
        try:
            asset_fs = task.get_asset_file_or_stream(asset)
        except FileNotFoundError:
            raise exceptions.NotFound(_("Asset does not exist"))

        asset_stream = asset_manager.get_asset_stream(asset_fs)
        if not asset_stream:
            raise exceptions.NotFound(_("Asset does not exist"))

        content_disposition = "attachment; filename={}".format(download_filename)
        return download_file_stream(
            request, asset_stream, content_disposition, get_file_name(asset)
        )


"""
Raw access to the task's asset folder resources
Useful when accessing a textured 3d model, or the Potree point cloud data
"""


class TaskAssets(TaskNestedView):
    def get(self, request, pk=None, project_pk=None, unsafe_asset_path=""):
        """
        Downloads a task asset (if available)
        """
        task = self.get_and_check_task(request, pk)

        # Check for directory traversal attacks
        try:
            asset_path = path_traversal_check(
                task.assets_path(unsafe_asset_path), task.assets_path("")
            )
        except SuspiciousFileOperation:
            raise exceptions.NotFound(_("Asset does not exist"))

        asset_manager = TaskAssetsManager(task)
        asset = asset_manager.get_asset_stream(asset_path)

        if not asset:
            raise exceptions.NotFound(_("Asset does not exist"))

        content_disposition = "inline; filename={}".format(os.path.basename(asset_path))
        return download_file_stream(
            request, asset, content_disposition, get_file_name(unsafe_asset_path)
        )


class TaskMetadataAssets(TaskNestedView):
    def get(self, request, pk=None, project_pk=None, asset_type=""):
        """
        Downloads a task asset (if available)
        """
        task = self.get_and_check_task(request, pk)

        asset_types = {
            "fotos": task_asset_type.FOTO,
            "fotos_360": task_asset_type.FOTO_360,
            "videos": task_asset_type.VIDEO,
        }

        if asset_type not in asset_types:
            raise exceptions.ValidationError({"error": "Invalid asset type."})

        assets = models.TaskAsset.objects.filter(
            status=task_asset_status.SUCCESS, type=asset_types[asset_type], task=task
        ).order_by("name")
        metadata = {}

        # Busca dados do dynamodb
        if settings.DYNAMODB_TABLE:
            # Verifica se a tabela existe
            try:
                dynamodb = boto3.resource('dynamodb',
                                          region_name=settings.DYNAMODB_REGION,
                                          aws_access_key_id=settings.DYNAMODB_ACCESS_KEY,
                                          aws_secret_access_key=settings.DYNAMODB_SECRET_KEY)
                table = dynamodb.Table(settings.DYNAMODB_TABLE)
            except Exception as e:
                logger.error(f"Erro ao conectar ao DynamoDB: {str(e)}")



        for asset in models.TaskAsset.sort_list(list(assets)):
            asset_name = asset.name.split("/")[1] if "/" in asset.name else asset.name
            metadata[asset_name] = {
                "latitude": asset.latitude,
                "longitude": asset.longitude,
                "altitude": asset.altitude,
                "created_at": asset.created_at,
                "description": '',
                "field_label": ''
            }

            # Extração do nome do arquivo do campo origin_path
            if asset.origin_path:
                file_name = os.path.basename(asset.origin_path)
                print(f"Nome do arquivo: {file_name}")

                # Busca no DynamoDB
                response = table.query(
                    KeyConditionExpression=Key('id').eq(file_name)
                )
                print(f"Response do DynamoDB: {response}")
                if 'Items' in response and len(response['Items']) > 0:
                    item = response['Items']
                    metadata[asset_name]["description"] = item[0].get("description")
                    metadata[asset_name]["field_label"] = item[0].get("field_label")

        return Response(metadata, status=status.HTTP_200_OK)


"""
Task backup endpoint
"""


class TaskBackup(TaskNestedView):
    def get(self, request, pk=None, project_pk=None):
        """
        Downloads a task's backup
        """
        task = self.get_and_check_task(request, pk)

        # Check and download
        try:
            celery_task_id = worker_tasks.generate_backup_zip.delay(pk).task_id
        except FileNotFoundError:
            raise exceptions.NotFound(_("Asset does not exist"))

        download_filename = request.GET.get(
            "filename", get_asset_download_filename(task, "backup.zip")
        )

        return Response(
            {"celery_task_id": celery_task_id, "filename": download_filename},
            status=status.HTTP_200_OK,
        )


"""
Task assets import
"""


class TaskAssetsImport(APIView):
    permission_classes = (permissions.AllowAny,)
    parser_classes = (
        parsers.MultiPartParser,
        parsers.JSONParser,
        parsers.FormParser,
    )

    def post(self, request, project_pk=None):
        project = get_and_check_project(request, project_pk, ("change_project",))

        files = flatten_files(request.FILES)
        import_url = request.data.get("url", None)
        task_name = request.data.get("name", _("Imported Task"))

        if not import_url and len(files) != 1:
            raise exceptions.ValidationError(
                detail=_("Cannot create task, you need to upload 1 file")
            )

        if import_url and len(files) > 0:
            raise exceptions.ValidationError(
                detail=_("Cannot create task, either specify a URL or upload 1 file.")
            )

        chunk_index = request.data.get("dzchunkindex")
        uuid = request.data.get("dzuuid")
        total_chunk_count = request.data.get("dztotalchunkcount", None)

        # Chunked upload?
        tmp_upload_file = None
        if (
            len(files) > 0
            and chunk_index is not None
            and uuid is not None
            and total_chunk_count is not None
        ):
            byte_offset = request.data.get("dzchunkbyteoffset", 0)

            try:
                chunk_index = int(chunk_index)
                byte_offset = int(byte_offset)
                total_chunk_count = int(total_chunk_count)
            except ValueError:
                raise exceptions.ValidationError(
                    detail="Some parameters are not integers"
                )
            uuid = re.sub("[^0-9a-zA-Z-]+", "", uuid)

            tmp_upload_file = os.path.join(
                settings.FILE_UPLOAD_TEMP_DIR, f"{uuid}.upload"
            )
            if os.path.isfile(tmp_upload_file) and chunk_index == 0:
                os.unlink(tmp_upload_file)

            with open(tmp_upload_file, "ab") as fd:
                fd.seek(byte_offset)
                if isinstance(files[0], InMemoryUploadedFile):
                    for chunk in files[0].chunks():
                        fd.write(chunk)
                else:
                    with open(files[0].temporary_file_path(), "rb") as file:
                        fd.write(file.read())

            if chunk_index + 1 < total_chunk_count:
                return Response({"uploaded": True}, status=status.HTTP_200_OK)

        # Ready to import
        with transaction.atomic():
            task = models.Task.objects.create(
                project=project,
                auto_processing_node=False,
                name=task_name,
                import_url=import_url if import_url else "file://all.zip",
                status=status_codes.RUNNING,
                pending_action=pending_actions.IMPORT,
            )
            task.create_task_directories()
            destination_file = task.assets_path("all.zip")

            # Non-chunked file import
            if tmp_upload_file is None and len(files) > 0:
                with open(destination_file, "wb+") as fd:
                    if isinstance(files[0], InMemoryUploadedFile):
                        for chunk in files[0].chunks():
                            fd.write(chunk)
                    else:
                        with open(files[0].temporary_file_path(), "rb") as file:
                            copyfileobj(file, fd)
            elif tmp_upload_file is not None:
                # Move
                shutil.move(tmp_upload_file, destination_file)

            worker_tasks.process_task.delay(task.id)

        serializer = TaskSerializer(task)
        return Response(serializer.data, status=status.HTTP_201_CREATED)
