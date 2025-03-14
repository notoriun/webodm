import os
import shutil
import json
import ffmpeg
import re
import gc
import logging
import tempfile
import xml.etree.ElementTree as ET

from app.models import Task
from app.utils.s3_utils import (
    get_s3_object,
    download_s3_file,
    get_s3_client,
    convert_task_path_to_s3,
)
from app.utils.file_utils import ensure_path_exists, get_file_name
from worker.utils.redis_file_cache import cache_lock
from webodm import settings
from rest_framework import exceptions
from django.utils.translation import gettext_lazy as _
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
    ):
        try:
            self.task.status = None
            self.task.last_error = None
            self.task.pending_action = None
            self.task_upload_in_progress(True)
            local_files_path = [file["path"] for file in local_files_to_upload]
            s3_downloaded_files = self._download_files_from_s3(s3_files_to_upload)

            if upload_type == "foto":
                response = self._upload_fotos(local_files_path, s3_downloaded_files)
            elif upload_type == "video":
                response = self._upload_videos(local_files_path, s3_downloaded_files)
            elif upload_type == "foto360":
                response = self._upload_foto360(local_files_path, s3_downloaded_files)
            elif upload_type == "foto_giga":
                response = self._upload_foto_giga(local_files_path, s3_downloaded_files)
            else:  # Default to 'orthophoto'
                response = self._upload_images(
                    local_files_to_upload, s3_downloaded_files, s3_files_to_upload
                )

            self.task_upload_in_progress(False)

            return response
        except Exception as e:
            self.task.set_failure(str(e))
            self.task_upload_in_progress(False)
            raise e

    def task_upload_in_progress(self, in_progress):
        self.task.upload_in_progress = in_progress
        self.task.save()

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
            {"path": filepath, "name": get_file_name(filepath)} for filepath in s3_files
        ]
        if len(files) == 0:
            raise exceptions.ValidationError(detail=_("No files uploaded"))

        uploaded = self.task.handle_images_upload(files)
        result = {}

        for filename, value in uploaded.items():
            result[filename] = value["size"]

        self.task.refresh_from_db()
        self.task.images_count = len(self.task.scan_images())
        self.task.s3_images += s3_images_with_bucket
        self.task.save()

        return {"success": True, "uploaded": result}

    def _upload_fotos(self, local_files: list[str], s3_files: list[str]):
        # Garantir que o diretório assets/fotos existe
        logger.info("Upload fotos")
        fotos_dir = self.task.assets_path("fotos")
        ensure_path_exists(fotos_dir)

        # Carregar o metadata.json existente, se existir
        metadata_path = os.path.join(fotos_dir, "metadata.json")
        metadata = self._read_metadata_json(metadata_path)

        uploaded_files = []

        # Identificar o índice inicial para novos arquivos
        max_index = self._get_file_index(metadata, "foto_", ".jpg")

        # Salvar os novos arquivos na pasta assets/fotos com nomes sequenciais
        files = local_files + s3_files
        assets_uploaded = []
        files_error = {}

        for idx, filepath in enumerate(files):
            try:
                # Para arquivos temporários, abra o arquivo diretamente do caminho temporário
                image = Image.open(filepath)
                exif_data = get_exif_data(image)
                lat_lon_alt = get_lat_lon_alt(exif_data)

                if not lat_lon_alt:
                    files_error[filepath] = "NOT_HAS_LAT_LON"
                    continue

                filename = f"foto_{max_index + idx + 1}.jpg"
                dst_path = os.path.join(fotos_dir, filename)
                logger.info("Saving file uploaded on: {}".format(dst_path))

                # Gravar o arquivo no diretório de destino
                shutil.copyfile(filepath, dst_path)

                # Guardar asset para o available_assets
                assets_uploaded.append(f"fotos/{filename}")

                # Adicionar informações de metadados
                metadata[filename] = {
                    "latitude": lat_lon_alt[0],
                    "longitude": lat_lon_alt[1],
                    "altitude": lat_lon_alt[2] or 0,
                }
                uploaded_files.append(get_file_name(filepath))
            except Exception as e:
                logger.error(str(e))
                continue

        # Atualizar o arquivo metadata.json
        self._update_metadata_json(metadata_path, metadata)

        # Adicionar metadata.json em available_assets
        assets_uploaded.append("fotos/metadata.json")

        self.task.refresh_from_db()
        self._concat_to_available_assets(assets_uploaded)
        self.task.upload_and_cache_assets()
        self.task.save()

        return {
            "success": True,
            "uploaded": uploaded_files,
            "files_with_error": files_error,
        }

    def _upload_videos(self, local_files: list[str], s3_files: list[str]):
        logger.info("Upload videos")
        # Garantir que o diretório assets/videos existe
        videos_dir = self.task.assets_path("videos")
        ensure_path_exists(videos_dir)

        # Carregar o metadata.json existente, se existir
        metadata_path = os.path.join(videos_dir, "metadata.json")
        metadata = self._read_metadata_json(metadata_path)

        uploaded_files = []

        # Identificar o índice inicial para novos arquivos
        max_index = self._get_file_index(metadata, "video_", ".mp4")

        # Salvar os novos arquivos na pasta assets/videos com nomes sequenciais
        files = local_files + s3_files
        assets_uploaded = []
        files_error = {}

        for idx, filepath in enumerate(files):
            try:
                # Extrair metadados GPS do vídeo
                lat_lon = get_video_gps(filepath)

                if not lat_lon:
                    files_error[filepath] = "NOT_HAS_LAT_LON"
                    continue

                filename = f"video_{max_index + idx + 1}.mp4"
                dst_path = os.path.join(videos_dir, filename)

                # Gravar o arquivo no diretório de destino
                shutil.copyfile(filepath, dst_path)

                metadata[filename] = {
                    "latitude": lat_lon[0],
                    "longitude": lat_lon[1],
                }

                # Guardar path para available_assets
                assets_uploaded.append(f"videos/{filename}")

                uploaded_files.append(get_file_name(filepath))
            except Exception as e:
                logger.error(str(e))
                continue

        # Atualizar o arquivo metadata.json
        self._update_metadata_json(metadata_path, metadata)

        # Adicionar metadata.json em available_assets
        assets_uploaded.append("videos/metadata.json")

        self.task.refresh_from_db()
        self._concat_to_available_assets(assets_uploaded)
        self.task.upload_and_cache_assets()
        self.task.save()

        return {
            "success": True,
            "uploaded": uploaded_files,
            "files_with_error": files_error,
        }

    def _upload_foto360(self, local_files: list[str], s3_files: list[str]):
        logger.info("Upload foto360")
        # Garantir que o diretório assets existe
        assets_dir = self.task.assets_path()
        ensure_path_exists(assets_dir)
        files = local_files + s3_files

        filepath = files[0] if len(files) > 0 else None
        file_uploaded_size = 0

        if filepath:
            # Salvar o arquivo na pasta assets com o nome foto360.jpg
            file_uploaded = self.task.assets_path("foto360.jpg")
            shutil.copyfile(filepath, file_uploaded)

            self.task.refresh_from_db()

            # Adicionar "foto360.jpg" ao campo available_assets
            if "foto360.jpg" not in self.task.available_assets:
                self.task.available_assets.append("foto360.jpg")

            self.task.upload_and_cache_assets()

            self.task.save()

            file_uploaded_size = os.path.getsize(file_uploaded)

        return {"success": True, "uploaded": {"foto360.jpg": file_uploaded_size}}

    def _upload_foto_giga(self, local_files: list[str], s3_files: list[str]):
        logger.info("Upload foto giga")

        # Salvar o novo arquivo na pasta assets/foto_giga
        filepath = (local_files + s3_files)[0]
        try:
            filename = "foto_giga_1.jpg"
            # Garantir que o diretório assets/foto_giga existe
            foto_giga_dir = os.path.join(self.task.assets_path("foto_giga"))

            if os.path.exists(foto_giga_dir):
                shutil.rmtree(foto_giga_dir)

            ensure_path_exists(foto_giga_dir)

            dst_path = os.path.join(foto_giga_dir, filename)

            # Gravar o arquivo no diretório de destino
            shutil.copyfile(filepath, dst_path)

            # Criar arquivos DZI
            create_dzi(dst_path, foto_giga_dir)

            os.remove(dst_path)  # Remover o arquivo se não contiver metadados GPS
        except Exception as e:
            return {"success": False}

        # Adicionar a informação em available_assets
        self.task.refresh_from_db()
        asset_info = f"foto_giga/metadata.dzi"
        self.task.available_assets = [
            asset
            for asset in self.task.available_assets
            if not ("foto_giga" in asset or "metadata.dzi" in asset)
        ]
        if asset_info not in self.task.available_assets:
            self.task.available_assets.append(asset_info)

        self.task.upload_and_cache_assets()
        self.task.save()

        return {"success": True, "uploaded": [get_file_name(filepath)]}

    def _read_metadata_json(self, metadata_path: str, use_cache_lock=True):
        s3_key = convert_task_path_to_s3(metadata_path)
        s3_metadata = self._read_s3_metadata_json(s3_key)
        local_metadata = self._read_local_metadata_json(metadata_path, use_cache_lock)

        merged_metadata = merge_metadatas(local_metadata, s3_metadata)
        logger.info(f"Readed metadata: {merged_metadata}")

        return merged_metadata

    def _update_metadata_json(self, metadata_path: str, new_metadata: dict):
        with cache_lock(metadata_path):
            current_metadata = self._read_metadata_json(
                metadata_path, use_cache_lock=False
            )
            metadata = merge_metadatas(current_metadata, new_metadata)

            with open(metadata_path, "w") as metadata_file:
                file_data = json.dumps(metadata)
                metadata_file.write(file_data)

        logger.info(f"Updated metadata with: {metadata}")
        return metadata

    def _read_s3_metadata_json(self, metadata_key: str) -> dict[str, any]:
        try:
            metadata_object = get_s3_object(metadata_key)
            if not metadata_object:
                return {}

            object_body = metadata_object.get("Body", None)

            if not object_body:
                return {}

            metadata_str = object_body.read().decode("utf-8")
            return json.loads(metadata_str)
        except Exception as e:
            logger.warning(f"S3 Metadata read error: {e}")
            return {}

    def _read_local_metadata_json(
        self, metadata_path: str, use_cache_lock: bool
    ) -> dict[str, any]:
        if not os.path.exists(metadata_path):
            return {}

        def load_metadata_file():
            with open(metadata_path) as metadata_file:
                metadata = metadata_file.read()
                return json.loads(metadata)

        try:
            if use_cache_lock:
                with cache_lock(metadata_path):
                    return load_metadata_file()
            else:
                return load_metadata_file()
        except Exception as e:
            logger.warning(f"Local Metadata read error: {e}")
            return {}

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

    def _get_file_index(self, metadata: dict[str, any], prefix: str, suffix: str):
        existing_files_index = [
            int(k.split(prefix)[1].split(suffix)[0])
            for k in metadata
            if k.startswith(prefix) and k.endswith(suffix)
        ]
        return existing_files_index[-1] if len(existing_files_index) > 0 else 0

    def _concat_to_available_assets(self, assets: list[str]):
        self.task.available_assets = [
            asset
            for asset in (self.task.available_assets + assets)
            if asset not in self.task.available_assets
        ]


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


def create_dzi(image_path, output_dir):
    """
    Converte uma imagem para o formato DZI e salva no diretório especificado.
    """
    image = Image.open(image_path)
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
                    os.path.join(level_dir, f"{x // tile_size}_{y // tile_size}.jpg")
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

    return os.path.join(dzi_dir, "metadata.dzi")


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
