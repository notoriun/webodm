import uuid as uuid_module
import xml.etree.ElementTree as ET
import tempfile
import logging
import gc
import os

from io import BytesIO
from typing import Literal, Union, Iterable
from django.db import models
from django.db.models.functions import Cast
from django.utils import timezone
from django.utils.translation import gettext_lazy as _, gettext
from PIL import Image

from app import task_asset_type, task_asset_status
from app.utils import file_utils, s3_utils

logger = logging.getLogger("app.logger")


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
        choices=STATUS_CODES,
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
        try:
            file_utils.move_stream(self.file_stream(), destiny_path)
        except s3_utils.S3ObjectGetError:
            return None

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

    def need_upload_to_s3(self):
        return not self.is_from_s3()

    def sort_name_value(self):
        if not hasattr(self, "_name_prefix") and not hasattr(self, "_name_suffix"):
            return self.name

        name_number = self.name.replace(getattr(self, "_name_prefix", ""), "").replace(
            getattr(self, "_name_suffix", ""), ""
        )

        try:
            return int(name_number)
        except:
            return name_number

    def _s3_object_body(self):
        if self._s3_object_body_cache is None:
            s3_key = s3_utils.remove_s3_bucket_prefix(self.origin_path)
            s3_object = s3_utils.get_s3_object(s3_key)

            if s3_object is None:
                raise s3_utils.S3ObjectGetError(s3_key)

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

    @staticmethod
    def sort_list(assets: Iterable["TaskAsset"]):
        def sort_asset(asset: "TaskAsset"):
            asset_typed = asset.copy_to_type()
            name_sort_value = asset_typed.sort_name_value()
            return (asset_typed.type, isinstance(name_sort_value, str), name_sort_value)

        return sorted(assets, key=sort_asset)


class TaskAssetFoto(TaskAsset):
    _asset_type = task_asset_type.FOTO

    _name_prefix = "fotos/foto_"
    _name_suffix = ".jpg"

    class Meta:
        proxy = True

    def generate_name(self, original_file_uploaded: dict[str, str]):
        last_foto_number = last_task_asset_name_number(
            self._name_prefix, self._name_suffix, task_asset_type.FOTO, self.task_id
        )

        self.name = self._name_prefix + str(last_foto_number + 1) + self._name_suffix
        return self.name

    def is_valid(self):
        try:
            return self._update_location() or "NOT_HAS_LAT_LON"
        except s3_utils.S3ObjectGetError:
            return "S3_GET_OBJECT_ERROR"
        except Exception:
            return "UNKNOW_ERROR"

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

    _name_prefix = "fotos_360/foto_360_"
    _name_suffix = ".jpg"

    class Meta:
        proxy = True

    def generate_name(self, original_file_uploaded: dict[str, str]):
        last_foto_number = last_task_asset_name_number(
            self._name_prefix, self._name_suffix, task_asset_type.FOTO_360, self.task_id
        )

        self.name = self._name_prefix + str(last_foto_number + 1) + self._name_suffix
        return self.name

    def is_valid(self):
        try:
            return self._update_location() or "NOT_HAS_LAT_LON"
        except s3_utils.S3ObjectGetError:
            return "S3_GET_OBJECT_ERROR"
        except Exception:
            return "UNKNOW_ERROR"

    def create_asset_file_on_task(self):
        destiny_path = super().create_asset_file_on_task()

        self._create_thumbnail()

        return destiny_path

    def _create_thumbnail(self):
        thumbnail_asset = TaskAsset.objects.create(
            type=task_asset_type.FOTO_360_THUMB,
            task=self.task,
            status=task_asset_status.PROCESSING,
            latitude=self.latitude,
            longitude=self.longitude,
            altitude=self.altitude,
            origin_path=self.origin_path,
        )

        try:
            thumbnail_path = file_utils.create_thumbnail(self.path())

            thumbnail_asset.status = task_asset_status.SUCCESS
            thumbnail_asset.name = file_utils.remove_path_from_path(
                thumbnail_path, self.task.assets_path()
            )
            thumbnail_asset.save(update_fields=("status", "name"))
        except Exception as e:
            logger.error(
                f'Error on create thumbnail of "{self.origin_path}". Original error: {e}'
            )
            try:
                thumbnail_asset.save(update_fields=("status",))
            except Exception as save_error:
                logger.error(
                    f"Error save error changes of thumbnail after other error. Save error: {save_error}"
                )

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

    _name_prefix = "fotos_360/foto_360_"
    _name_suffix = "_thumb.jpg"

    class Meta:
        proxy = True


class TaskAssetFotoGiga(TaskAsset):
    _asset_type = task_asset_type.FOTO_GIGA

    _name_prefix = "foto_giga/foto_giga_"
    _name_suffix = ".jpg"

    def generate_name(self, original_file_uploaded: dict[str, str]):
        self.name = "foto_giga/foto_giga_1.jpg"
        return self.name

    def create_asset_file_on_task(self):
        foto_giga_file = super().create_asset_file_on_task()

        foto_giga_dir = self.task.assets_path("foto_giga")
        dzi_path = self._create_dzi(foto_giga_dir)
        file_utils.remove_path_from_path(dzi_path, self.task.assets_path())

        try:
            os.remove(foto_giga_file)
        except Exception as e:
            logger.error(
                f"Error on remove foto giga uploaded file. Original error: {e}"
            )

        return dzi_path

    def _create_dzi(self, output_dir):
        """
        Converte uma imagem para o formato DZI e salva no diretório especificado.
        """
        image = Image.open(self.path())
        img_width, img_height = image.size

        max_level = int(image.size[0].bit_length())

        dzi_dir = os.path.join(output_dir)
        files_dir = os.path.join(dzi_dir, "metadata_files")
        tile_size = 512
        overlap = 1

        for level in range(max_level + 1):
            level_dir = os.path.join(files_dir, str(level))
            if not os.path.exists(level_dir):
                os.makedirs(level_dir, exist_ok=True)

            scale = 2 ** (max_level - level)
            new_width = max(img_width // scale, 1)
            new_height = max(img_height // scale, 1)
            resized_image = image.resize((new_width, new_height), Image.LANCZOS)

            for x in range(0, resized_image.width, tile_size):
                for y in range(0, resized_image.height, tile_size):
                    box = (x, y, x + tile_size + overlap, y + tile_size + overlap)
                    tile = resized_image.crop(box)
                    tile.save(
                        os.path.join(
                            level_dir, f"{x // tile_size}_{y // tile_size}.jpg"
                        )
                    )
                    tile.close()
                    gc.collect()

            resized_image.close()
            gc.collect()

        # Criar arquivo XML DZI
        root = ET.Element(
            "Image",
            TileSize=str(tile_size),
            Overlap=str(overlap),
            Format="jpg",
            xmlns="http://schemas.microsoft.com/deepzoom/2008",
        )
        ET.SubElement(root, "Size", Width=str(img_width), Height=str(img_height))
        tree = ET.ElementTree(root)
        path_metadata = os.path.join(dzi_dir, "metadata.dzi")
        with open(path_metadata, "wb") as f:
            f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
            tree.write(f, encoding="utf-8", xml_declaration=False)
        image.close()
        gc.collect()

        return path_metadata

    class Meta:
        proxy = True


class TaskAssetVideo(TaskAsset):
    _asset_type = task_asset_type.VIDEO

    _name_prefix = "videos/video_"
    _name_suffix = ".mp4"

    class Meta:
        proxy = True

    def generate_name(self, original_file_uploaded: dict[str, str]):
        last_video_number = last_task_asset_name_number(
            self._name_prefix, self._name_suffix, task_asset_type.VIDEO, self.task_id
        )

        self.name = self._name_prefix + str(last_video_number + 1) + self._name_suffix
        return self.name

    def is_valid(self):
        try:
            return self._update_location() or "NOT_HAS_LAT_LON"
        except s3_utils.S3ObjectGetError:
            return "S3_GET_OBJECT_ERROR"
        except Exception:
            return "UNKNOW_ERROR"

    def _update_location(self):
        with tempfile.NamedTemporaryFile(delete=True, suffix=".mp4") as temp_file:
            file_utils.move_stream(self.file_stream(), temp_file.name)

            lat_lon_alt = file_utils.get_video_location(temp_file.name)

        if not lat_lon_alt:
            return False

        self.latitude = lat_lon_alt[0]
        self.longitude = lat_lon_alt[1]
        self.altitude = 0

        return True


class TaskAssetOrthophoto(TaskAsset):
    _asset_type = task_asset_type.ORTHOPHOTO

    class Meta:
        proxy = True

    def generate_name(self, original_file_uploaded: dict[str, str]):
        self.name = original_file_uploaded.get("name", None)

        if self.name is None:
            self.name = file_utils.get_file_name(self.origin_path)

        return self.name

    def path(self) -> str:
        if self.status == task_asset_status.PROCESSING:
            return self.task.task_path(self.name)

        try:
            return self.task.get_asset_download_path(self.name)
        except:
            return self.task.assets_path(self.name)

    def need_upload_to_s3(self):
        return (not self.is_from_s3()) and (self.task.assets_path() in self.path())


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
