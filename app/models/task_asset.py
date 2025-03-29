import uuid as uuid_module
import tempfile

from io import BytesIO
from typing import Literal, Union
from django.db import models
from django.db.models.functions import Cast
from django.utils import timezone
from django.utils.translation import gettext_lazy as _, gettext

from app import task_asset_type, task_asset_status
from app.utils import file_utils, s3_utils


class TaskAsset(models.Model):
    TYPES = (
        (task_asset_type.FOTO, "FOTO"),
        (task_asset_type.FOTO_360, "FOTO_360"),
        (task_asset_type.FOTO_GIGA, "FOTO_GIGA"),
        (task_asset_type.ORTHOPHOTO, "ORTHOPHOTO"),
        (task_asset_type.VIDEO, "VIDEO"),
    )
    STATUS_CODES = (
        (task_asset_status.PROCESSING, "PROCESSING"),
        (task_asset_status.ERROR, "ERROR"),
        (task_asset_status.SUCCESS, "SUCCESS"),
    )

    id = models.UUIDField(
        primary_key=True,
        default=uuid_module.uuid4,
        unique=True,
        serialize=False,
        editable=False,
        verbose_name=_("Id"),
    )
    type = models.IntegerField(
        choices=TYPES,
        db_index=True,
        null=False,
        blank=False,
        help_text=_("The type of asset."),
        verbose_name=_("Type of asset"),
    )
    name = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text=_("The task asset path after task path"),
        verbose_name=_("Name"),
    )
    task = models.ForeignKey(
        "app.Task",
        on_delete=models.CASCADE,
        null=False,
        blank=False,
        help_text=_("The task of this asset"),
        verbose_name=_("Task"),
        related_name="assets",
    )
    created_at = models.DateTimeField(
        default=timezone.now, help_text=_("Creation date"), verbose_name=_("Created at")
    )
    status = models.IntegerField(
        choices=TYPES,
        db_index=True,
        null=False,
        blank=False,
        help_text=_("Current status of asset."),
        verbose_name=_("Status of asset"),
    )
    latitude = models.FloatField(
        default=None,
        help_text=_("Latitude"),
        verbose_name=_("Latitude"),
        blank=True,
        null=True,
    )
    longitude = models.FloatField(
        default=None,
        help_text=_("Longitude"),
        verbose_name=_("Longitude"),
        blank=True,
        null=True,
    )
    altitude = models.FloatField(
        default=None,
        help_text=_("Altitude"),
        verbose_name=_("Altitude"),
        blank=True,
        null=True,
    )
    origin_path = models.TextField(
        null=False,
        blank=False,
        help_text=_(
            "The origin path of asset. If starts with 's3://' that means it's from S3"
        ),
        verbose_name=_("Origin path"),
    )

    class Meta:
        verbose_name = _("Asset Task")
        verbose_name_plural = _("Asset Tasks")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if hasattr(self, "_asset_type"):
            self.type = self._asset_type

        self._s3_object_body_cache = None

    def __str__(self):
        name = self.name if self.name is not None else gettext("unnamed")

        return "Asset [{}] ({})".format(name, self.id)

    def save(self, *args, **kwargs):
        if hasattr(self, "_asset_type"):
            self.type = self._asset_type

        return super().save(*args, **kwargs)

    def generate_name(self, original_file_uploaded: dict[str, str]) -> str:
        raise Exception(f'Not implemented "generate_name" on {self.__class__}')

    def copy_to_type(self):
        return TaskAsset.build_from_type(self.type, self)

    def create_asset_file_on_task(self):
        asset_filename = file_utils.get_file_name(self.name)
        destiny_path = self.path()
        destiny_dir = destiny_path.replace(asset_filename, "")

        file_utils.ensure_path_exists(destiny_dir)
        file_utils.move_stream(self.file_stream(), destiny_path)

        return destiny_path

    def is_valid(self) -> Union[Literal[True], str]:
        return True

    def is_from_s3(self):
        return self.origin_path.startswith("s3://")

    def path(self) -> str:
        return self.task.assets_path(self.name)

    def file_stream(self):
        if self.status == task_asset_status.SUCCESS:
            return open(self.path(), "rb")

        if self.is_from_s3():
            return BytesIO(self._s3_object_body())

        return open(self.origin_path, "rb")

    def _s3_object_body(self):
        if self._s3_object_body_cache is None:
            s3_object = s3_utils.get_s3_object(
                s3_utils.remove_s3_bucket_prefix(self.origin_path)
            )
            self._s3_object_body_cache = s3_object["Body"].read()

        return self._s3_object_body_cache

    @staticmethod
    def get_query_with_numero(name_prefix: str, name_suffix: str):
        return TaskAsset.objects.annotate(
            numero=Cast(
                models.Func(
                    models.Func(
                        models.F("name"),
                        models.Value(name_prefix),
                        models.Value(""),
                        function="REGEXP_REPLACE",
                    ),
                    models.Value(name_suffix),
                    models.Value(""),
                    function="REGEXP_REPLACE",
                ),
                models.IntegerField(),
            )
        )

    @staticmethod
    def class_from_type(type: int):
        classes = {
            task_asset_type.FOTO: TaskAssetFoto,
            task_asset_type.FOTO_360: TaskAssetFoto360,
            task_asset_type.FOTO_360_THUMB: TaskAssetFoto360Thumbnail,
            task_asset_type.FOTO_GIGA: TaskAssetFotoGiga,
            task_asset_type.VIDEO: TaskAssetVideo,
            task_asset_type.ORTHOPHOTO: TaskAssetOrthophoto,
        }

        return classes[type] if type in classes else None

    @staticmethod
    def build_from_type(type: int, task_asset: "TaskAsset") -> "TaskAsset":
        kwargs = {
            "id": task_asset.id,
            "name": task_asset.name,
            "task": task_asset.task,
            "created_at": task_asset.created_at,
            "status": task_asset.status,
            "latitude": task_asset.latitude,
            "longitude": task_asset.longitude,
            "altitude": task_asset.altitude,
            "origin_path": task_asset.origin_path,
        }

        cls = TaskAsset.class_from_type(type)

        return cls(**kwargs) if cls else None


class TaskAssetFoto(TaskAsset):
    _asset_type = task_asset_type.FOTO

    class Meta:
        proxy = True

    def generate_name(self, original_file_uploaded: dict[str, str]):
        last_foto_number = last_task_asset_name_number(
            "fotos/foto_", ".jpg", task_asset_type.FOTO, self.task_id
        )

        self.name = f"fotos/test_foto_{last_foto_number + 1}.jpg"
        return self.name

    def is_valid(self):
        return self._update_location() or "NOT_HAS_LAT_LON"

    def _update_location(self):
        lat_lon_alt = file_utils.get_image_location(self.file_stream())

        if not lat_lon_alt:
            return False

        self.latitude = lat_lon_alt[0]
        self.longitude = lat_lon_alt[1]
        self.altitude = lat_lon_alt[2] or 0

        return True


class TaskAssetFoto360(TaskAsset):
    _asset_type = task_asset_type.FOTO_360

    class Meta:
        proxy = True

    def generate_name(self, original_file_uploaded: dict[str, str]):
        last_foto_number = last_task_asset_name_number(
            "fotos_360/foto_360_", ".jpg", task_asset_type.FOTO_360, self.task_id
        )

        self.name = f"fotos_360/foto_360_{last_foto_number + 1}.jpg"
        return self.name

    def is_valid(self):
        return self._update_location() or "NOT_HAS_LAT_LON"

    def _update_location(self):
        lat_lon_alt = file_utils.get_image_location(self.file_stream())

        if not lat_lon_alt:
            return False

        self.latitude = lat_lon_alt[0]
        self.longitude = lat_lon_alt[1]
        self.altitude = lat_lon_alt[2] or 0

        return True


class TaskAssetFoto360Thumbnail(TaskAsset):
    _asset_type = task_asset_type.FOTO_360_THUMB

    class Meta:
        proxy = True


class TaskAssetFotoGiga(TaskAsset):
    _asset_type = task_asset_type.FOTO_GIGA

    class Meta:
        proxy = True


class TaskAssetVideo(TaskAsset):
    _asset_type = task_asset_type.VIDEO

    class Meta:
        proxy = True

    def generate_name(self, original_file_uploaded: dict[str, str]):
        last_foto_number = last_task_asset_name_number(
            "videos/video_", ".mp4", task_asset_type.VIDEO, self.task_id
        )

        self.name = f"videos/video_{last_foto_number + 1}.mp4"
        return self.name

    def is_valid(self):
        return self._update_location() or "NOT_HAS_LAT_LON"

    def _update_location(self):
        with tempfile.NamedTemporaryFile(delete=True, suffix=".mp4") as temp_file:
            file_utils.move_stream(self.file_stream(), temp_file.name)

            lat_lon_alt = file_utils.get_video_location(temp_file.name)

        if not lat_lon_alt:
            return False

        self.latitude = lat_lon_alt[0]
        self.longitude = lat_lon_alt[1]
        self.altitude = lat_lon_alt[2] or 0

        return True


class TaskAssetOrthophoto(TaskAsset):
    _asset_type = task_asset_type.ORTHOPHOTO

    class Meta:
        proxy = True

    def generate_name(self, original_file_uploaded: dict[str, str]):
        self.name = original_file_uploaded.get("name", None)

        return self.name


def get_common_task_assets_ordered_query(name_prefix: str, name_suffix: str):
    return (
        TaskAsset.get_query_with_numero(name_prefix, name_suffix)
        .filter(
            models.Q(name__icontains=name_prefix)
            & models.Q(name__icontains=name_suffix)
            & models.Q(name__isnull=False)
        )
        .exclude(name__icontains="metadata.json")
        .order_by("numero")
    )


def last_task_asset_name_number(
    name_prefix: str, name_suffix: str, type: int, task_id: uuid_module.UUID
):
    last_asset = (
        get_common_task_assets_ordered_query(name_prefix, name_suffix)
        .filter(type=type, task_id=task_id)
        .last()
    )

    if not last_asset:
        return 0

    return last_asset.numero
