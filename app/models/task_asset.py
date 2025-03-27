import uuid as uuid_module

from django.db import models
from django.db.models.functions import Cast
from django.utils import timezone
from django.utils.translation import gettext_lazy as _, gettext

from app import task_asset_type, task_asset_status


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

    class Meta:
        verbose_name = _("Asset Task")
        verbose_name_plural = _("Asset Tasks")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if hasattr(self, "_asset_type"):
            self.type = self._asset_type

    def __str__(self):
        name = self.name if self.name is not None else gettext("unnamed")

        return "Asset [{}] ({})".format(name, self.id)

    def save(self, force_insert=..., force_update=..., using=..., update_fields=...):
        if hasattr(self, "_asset_type"):
            self.type = self._asset_type

        return super().save(force_insert, force_update, using, update_fields)

    def copy_to_type(self):
        return TaskAsset.build_from_type(self.type, self)

    def generate_name(self):
        raise Exception(f'Not implemented "create_next_name" on {self.__class__}')

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
        }

        cls = TaskAsset.class_from_type(type)

        return cls(**kwargs) if cls else None


class TaskAssetFoto(TaskAsset):
    _asset_type = task_asset_type.FOTO

    def generate_name(self):
        last_foto_number = last_task_asset_name_number(
            "fotos/foto_", ".jpg", task_asset_type.FOTO, self.task_id
        )

        self.name = f"foto_{last_foto_number + 1}.jpg"
        return self.name


class TaskAssetFoto360(TaskAsset):
    _asset_type = task_asset_type.FOTO_360

    def generate_name(self):
        last_foto_number = last_task_asset_name_number(
            "fotos_360/foto_360_", ".jpg", task_asset_type.FOTO_360, self.task_id
        )

        self.name = f"foto_360_{last_foto_number + 1}.jpg"
        return self.name


class TaskAssetFoto360Thumbnail(TaskAsset):
    _asset_type = task_asset_type.FOTO_360_THUMB


class TaskAssetFotoGiga(TaskAsset):
    _asset_type = task_asset_type.FOTO_GIGA


class TaskAssetVideo(TaskAsset):
    _asset_type = task_asset_type.VIDEO

    def generate_name(self):
        last_foto_number = last_task_asset_name_number(
            "videos/video_", ".mp4", task_asset_type.VIDEO, self.task_id
        )

        self.name = f"video_{last_foto_number + 1}.mp4"
        return self.name


class TaskAssetOrthophoto(TaskAsset):
    _asset_type = task_asset_type.ORTHOPHOTO


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
