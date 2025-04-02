import os
import shutil
import ffmpeg
import re
import logging
import tempfile

from app import task_asset_type, task_asset_status
from app.models import Task, TaskAsset
from app.utils.s3_utils import (
    download_s3_file,
    get_s3_client,
)
from app.utils.file_utils import ensure_path_exists, get_file_name
from webodm import settings
from rest_framework import exceptions
from django.db.models import Q
from django.utils.translation import gettext_lazy as _
from django.db import transaction
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS

logger = logging.getLogger("app.logger")


class TaskFilesUploader:
    def __init__(self, task_id):
        self._task_id = task_id
        self._task_loaded: Task = None

    @property
    def task(self):
        if not self._task_loaded:
            self._refresh_task()

        return self._task_loaded

    def upload_files(
        self,
        local_files_to_upload: list[dict[str, str]],
        s3_files_to_upload: list[str],
        upload_type: str,
        ignore_upload_to_s3=False,
    ):
        try:
            self.task_upload_in_progress(True)

            all_files_uploadeds = self._parse_uploaded_files(
                local_files_to_upload, s3_files_to_upload
            )
            task_asset_upload_type = self._parse_upload_type(upload_type)
            response = self._create_task_assets(
                all_files_uploadeds, task_asset_upload_type, ignore_upload_to_s3
            )

            if task_asset_upload_type == task_asset_type.ORTHOPHOTO:
                self.task.refresh_from_db()
                self.task.images_count = len(self.task.scan_images())
                self.task.s3_images = list(
                    set(s3_files_to_upload).union(self.task.s3_images)
                )
                self.task.save(update_fields=["s3_images", "images_count"])

            # elif upload_type == "foto360":
            #     response = self.upload_foto360(local_files_path, s3_downloaded_files)
            # elif upload_type == "foto_giga":
            #     response = self._upload_foto_giga(local_files_path, s3_downloaded_files)
            # else:  # Default to 'orthophoto'
            #     response = self._upload_images(
            #         local_files_to_upload, s3_downloaded_files, s3_files_to_upload
            #     )

            self.task_upload_in_progress(False)

            return response
        except Exception as e:
            self.task_upload_in_progress(False)
            raise e

    def task_upload_in_progress(self, in_progress):
        self._update_task(upload_in_progress=in_progress)
        self.task.refresh_from_db()

    def upload_foto360(
        self, local_files: list[str], s3_files: list[str], ignore_upload_to_s3=False
    ):
        files = self._parse_uploaded_files([], local_files + s3_files)
        return self.upload_files(files, [], "foto360")

    def task_already_uploading(self):
        return self.task.upload_in_progress

    def _refresh_task(self):
        self._task_loaded = Task.objects.get(pk=self._task_id)

    def _upload_images(
        self,
        local_files: list[dict[str, str]],
        s3_files: list[str],
        s3_images_with_bucket: list[str],
    ):
        logger.info("Upload images")
        files = local_files + [
            {"path": filepath, "name": get_file_name(s3_images_with_bucket[i])}
            for i, filepath in enumerate(s3_files)
        ]
        if len(files) == 0:
            raise exceptions.ValidationError(detail=_("No files uploaded"))

        uploaded = self.task.handle_images_upload(files)

        self.task.refresh_from_db()
        self.task.images_count = len(self.task.scan_images())
        self.task.s3_images += s3_images_with_bucket
        self.task.save(update_fields=["s3_images", "images_count"])

        return {"success": True, "uploaded": uploaded}

    def _upload_foto_giga(self, local_files: list[str], s3_files: list[str]):
        logger.info("Upload foto giga")

        filepath = (local_files + s3_files)[0]
        try:
            filename = "foto_giga_1.jpg"
            foto_giga_dir = os.path.join(self.task.assets_path("foto_giga"))

            task_asset = TaskAsset.objects.get_or_create(
                type=task_asset_type.FOTO_GIGA,
                name=f"foto_giga/metadata.dzi",
                task=self.task,
            )
            task_asset.status = task_asset_status.PROCESSING
            task_asset.save()

            if os.path.exists(foto_giga_dir):
                shutil.rmtree(foto_giga_dir)

            ensure_path_exists(foto_giga_dir)

            dst_path = os.path.join(foto_giga_dir, filename)

            # Gravar o arquivo no diretório de destino
            shutil.move(filepath, dst_path)

            # Criar arquivos DZI
            create_dzi(dst_path, foto_giga_dir)

            os.remove(dst_path)  # Remover o arquivo se não contiver metadados GPS
        except Exception as e:
            return {"success": False}

        self._upload_task_assets_to_s3([task_asset])

        return {"success": True, "uploaded": [filename]}

    def _download_files_from_s3(self, s3_images: list[str]):
        downloaded_s3_images = []
        s3_client = get_s3_client()

        if not s3_client:
            logger.error(
                "Could not download any image from s3, because is missing some s3 configuration variable"
            )
            return []

        ensure_path_exists(settings.MEDIA_TMP)

        for image in s3_images:
            try:
                s3_image_name = image.split("/")[-1]
                destiny_path = tempfile.mktemp(
                    f"_{s3_image_name}", dir=settings.MEDIA_TMP
                )
                download_s3_file(image, destiny_path, s3_client)
                downloaded_s3_images.append(destiny_path)
            except Exception as e:
                for downloaded_file in downloaded_s3_images:
                    try:
                        os.remove(downloaded_file)
                    except:
                        pass
                raise Exception(
                    f"Error at download '{image}', maybe not found or not have permission. \nOriginal error: {str(e)}"
                )

        logger.info(f"downloaded files {downloaded_s3_images}")
        return downloaded_s3_images

    def _get_file_index(
        self, asset_type: task_asset_type, name_prefix: str, name_suffix: str
    ):
        last_asset = (
            TaskAsset.get_query_with_numero(name_prefix, name_suffix)
            .filter(
                task=self.task,
                type=asset_type,
            )
            .filter(Q(name__icontains=name_prefix) & Q(name__icontains=name_suffix))
            .exclude(name__icontains="metadata.json")
            .order_by("-numero")
            .first()
        )

        if not last_asset:
            return 0

        return last_asset.numero

    def _concat_to_available_assets(self, assets: list[TaskAsset]):
        TaskAsset.objects.filter(pk__in=(asset.pk for asset in assets)).update(
            status=task_asset_status.SUCCESS
        )

    def _update_task(self, *args, **kwargs):
        Task.objects.filter(pk=self.task.pk).update(**kwargs)
        self.task.refresh_from_db()

    def _create_thumbnail(self, image_path: str, tamanho=(200, 200)):
        imagem = Image.open(image_path)
        imagem.thumbnail(tamanho)

        name, ext = os.path.splitext(image_path)
        thumbnail_path = f"{name}_thumb{ext}"

        imagem.save(thumbnail_path, format=imagem.format)

        return thumbnail_path

    def _upload_task_assets_to_s3(self, assets: list[TaskAsset]):
        self.task.upload_and_cache_assets(
            [asset.path() for asset in assets if asset.need_upload_to_s3()]
        )

    def _upload_task_asset(self, uploaded_file: dict[str, str], asset_type: int):
        task_asset = TaskAsset.objects.create(
            type=asset_type,
            task=self.task,
            status=task_asset_status.PROCESSING,
            origin_path=uploaded_file["path"],
        ).copy_to_type()

        is_valid_or_error = task_asset.is_valid()

        if is_valid_or_error != True:
            filename = uploaded_file["name"]
            logger.debug(
                f'Error on upload file "{filename}". Original error: {is_valid_or_error}'
            )

            task_asset.status = task_asset_status.ERROR
            task_asset.save(update_fields=("status",))

            return task_asset, is_valid_or_error

        task_asset.generate_name(uploaded_file)
        file_created = task_asset.create_asset_file_on_task()

        if file_created is None:
            task_asset.status = task_asset_status.ERROR

        task_asset.save()

        return task_asset, None

    def _parse_upload_type(self, upload_type: str):
        if upload_type == "foto":
            return task_asset_type.FOTO
        elif upload_type == "video":
            return task_asset_type.VIDEO
        elif upload_type == "foto360":
            return task_asset_type.FOTO_360
        elif upload_type == "foto_giga":
            return task_asset_type.FOTO_GIGA
        else:  # Default to 'orthophoto'
            return task_asset_type.ORTHOPHOTO

    def _parse_uploaded_files(
        self, uploadeds_saved_local: list[dict[str, str]], uploadeds_saved_s3: list[str]
    ):
        return uploadeds_saved_local + [
            {
                "path": filepath,
                "name": get_file_name(filepath),
            }
            for filepath in uploadeds_saved_s3
        ]

    def _create_task_assets(
        self,
        all_files_uploadeds: list[dict[str, str]],
        asset_type: int,
        ignore_upload_to_s3=False,
    ):
        assets_uploadeds = []
        files_success = []
        files_with_error = {}

        for file_uploaded in all_files_uploadeds:
            task_asset, upload_error = self._upload_task_asset(
                file_uploaded, asset_type
            )

            filename = file_uploaded["name"]

            if task_asset.status == task_asset_status.ERROR:
                files_with_error[filename] = upload_error or "UNKNOW_ERROR"
            else:
                files_success.append(filename)
                assets_uploadeds.append(task_asset)

        if asset_type != task_asset_type.ORTHOPHOTO:
            if not ignore_upload_to_s3:
                self._upload_task_assets_to_s3(assets_uploadeds)
            self._concat_to_available_assets(assets_uploadeds)

        return {
            "success": len(files_with_error) == 0,
            "uploaded": files_success,
            "files_with_error": files_with_error,
        }


def get_exif_data(image):
    exif_data = {}
    info = image._getexif()
    if info:
        for tag, value in info.items():
            decoded = TAGS.get(tag, tag)
            if decoded == "GPSInfo":
                gps_data = {}
                for t in value:
                    sub_decoded = GPSTAGS.get(t, t)
                    gps_data[sub_decoded] = value[t]
                exif_data[decoded] = gps_data
            else:
                exif_data[decoded] = value

    return exif_data


def get_lat_lon_alt(exif_data):
    gps_info = exif_data.get("GPSInfo")
    if not gps_info:
        return None

    def get_if_exist(data, key):
        return data[key] if key in data else None

    lat = get_if_exist(gps_info, GPSTAGS.get(2))  # GPSLatitude
    lat_ref = get_if_exist(gps_info, GPSTAGS.get(1))  # GPSLatitudeRef
    lon = get_if_exist(gps_info, GPSTAGS.get(4))  # GPSLongitude
    lon_ref = get_if_exist(gps_info, GPSTAGS.get(3))  # GPSLongitudeRef
    alt = get_if_exist(gps_info, GPSTAGS.get(6))  # GPSAltitude
    alt_ref = get_if_exist(gps_info, GPSTAGS.get(5))  # GPSAltitudeRef

    if lat and lon and lat_ref and lon_ref:
        lat = convert_to_degrees(lat, lat_ref)
        lon = convert_to_degrees(lon, lon_ref)

        if alt:
            alt = float(alt)
            if alt_ref and alt_ref != 0:
                alt = -alt

        return lat, lon, alt
    return None


def convert_to_degrees(value, ref):
    def to_degrees(val):
        d = float(val[0])
        m = float(val[1])
        s = float(val[2])
        return d + (m / 60.0) + (s / 3600.0)

    degrees = to_degrees(value)
    if ref in ["S", "W"]:
        degrees = -degrees
    return degrees


def get_video_gps(file_path):
    try:
        probe = ffmpeg.probe(file_path)
        tags = probe.get("format", {}).get("tags", {})
        location = tags.get("location")
        if not location:  # Se location estiver em branco ou não existir
            gps_latitude = tags.get("gps_latitude")
            gps_longitude = tags.get("gps_longitude")
            if gps_latitude and gps_longitude:
                location = f"{gps_latitude},{gps_longitude}"
        if location:
            # Remove a barra no final da string, se houver
            location = location.rstrip("/")

            # Definir a expressão regular para capturar latitude e longitude
            match = re.match(r"([+-]?\d+\.\d+),?\s*([+-]?\d+\.\d+)", location)
            if not match:
                raise ValueError("Formato inválido para a string de localização")

            # Extrair latitude e longitude da correspondência
            lat_str, lon_str = match.groups()

            # Converter para float
            latitude = float(lat_str)
            longitude = float(lon_str)
            return latitude, longitude
    except ffmpeg.Error as e:
        print(e)
        return None
    return None


def merge_metadatas(metadata1: dict[str, any], metadata2: dict[str, any]):
    result: dict[str, any] = {}

    for key1 in metadata1:
        result[key1] = metadata1[key1]

    for key2 in metadata2:
        if not (key2 in result):
            result[key2] = metadata2[key2]
        else:
            if not isinstance(result[key2], dict) or not isinstance(
                metadata2[key2], dict
            ):
                result[key2] = metadata2[key2]
            else:
                result[key2] = merge_metadatas(result[key2], metadata2[key2])

    return result


class AssetNotHasLatLonError(Exception):
    def __init__(self, asset_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.asset_path = asset_path
