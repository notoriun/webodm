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
from app.utils.s3_utils import get_s3_object, list_s3_objects, download_s3_file, get_s3_client
from app.utils.file_utils import ensure_path_exists, get_file_name
from nodeodm import status_codes
from webodm import settings
from rest_framework import exceptions
from django.utils.translation import gettext_lazy as _
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS

logger = logging.getLogger('app.logger')


class TaskFilesUploader:
    def __init__(self, task_id):
        self._task_id = task_id
        self._task_loaded: Task = None

    @property
    def task(self):
        if not self._task_loaded:
            self._refresh_task()

        return self._task_loaded

    def upload_files(self, local_files_to_upload: list[dict[str, str]], s3_files_to_upload: list[str], upload_type: str):
        try:
            self.task_upload_in_progress(True)
            files_paths = [file['path'] for file in local_files_to_upload]
            s3_downloaded_files = self._download_files_from_s3(s3_files_to_upload)
            all_files_to_upload = files_paths + s3_downloaded_files
            
            if upload_type == 'foto':
                response = self._upload_fotos(all_files_to_upload)
            elif upload_type == 'video':
                response = self._upload_videos(all_files_to_upload)
            elif upload_type == 'foto360':
                response = self._upload_foto360(all_files_to_upload)
            elif upload_type == 'foto_giga':
                response = self._upload_foto_giga(all_files_to_upload)
            else:  # Default to 'orthophoto'
                response = self._upload_images(
                    [{
                        'path': filepath,
                        'name': get_file_name(filepath)
                    } for filepath in s3_downloaded_files] + local_files_to_upload
                )

            self.task_upload_in_progress(False)

            return response
        except Exception as e:
            self.task.set_failure(str(e))
            self.task_upload_in_progress(False)
            raise e

    def _refresh_task(self):
        self._task_loaded = Task.objects.get(pk=self._task_id)

    def _upload_images(self, files: list[str]):
        if len(files) == 0:
            raise exceptions.ValidationError(detail=_("No files uploaded"))

        uploaded = self.task.handle_images_upload(files)
        self.task.images_count = len(self.task.scan_images())
        self.task.save()

        return {'success': True, 'uploaded': uploaded }

    def _upload_fotos(self, files: list[str]):
        # Garantir que o diretório assets/fotos existe
        logger.info('upload fotos')
        fotos_dir = self.task.assets_path("fotos")
        ensure_path_exists(fotos_dir)

        # Carregar o metadata.json existente, se existir
        metadata_path = os.path.join(fotos_dir, 'metadata.json')
        metadata = self._read_metadata_json(metadata_path)
        logger.info(f'lido metadados {metadata}')

        uploaded_files = []

        # Identificar o índice inicial para novos arquivos
        foto_prefix = os.path.join(fotos_dir, "foto_")
        existing_files_index = self._list_file_indexes_with(foto_prefix, ".jpg")
        max_index = existing_files_index[-1] if len(existing_files_index) > 0 else 0
        logger.info(f'obtido index max: {max_index}')

        # Salvar os novos arquivos na pasta assets/fotos com nomes sequenciais

        for idx, filepath in enumerate(files):
            try:
                logger.info(f'abrindo {filepath} no PIL')
                # Para arquivos temporários, abra o arquivo diretamente do caminho temporário
                image = Image.open(filepath)
                exif_data = get_exif_data(image)
                lat_lon_alt = get_lat_lon_alt(exif_data)
                logger.info(f'obtido lat lon: {lat_lon_alt}')


                if not lat_lon_alt:
                    continue

                filename = f"foto_{max_index + idx + 1}.jpg"
                dst_path = os.path.join(fotos_dir, filename)
                logger.info('saving file uploaded on: {}'.format(dst_path))

                # Gravar o arquivo no diretório de destino
                shutil.copyfile(filepath, dst_path)

                # Adicionar a informação em available_assets
                asset_info = f"fotos/{filename}"
                if asset_info not in self.task.available_assets:
                    self.task.available_assets.append(asset_info)

                # Adicionar informações de metadados
                metadata[filename] = {'latitude': lat_lon_alt[0], 'longitude': lat_lon_alt[1], 'altitude': lat_lon_alt[2] or 0}
                uploaded_files.append(get_file_name(filepath))
            except Exception as e:
                logger.error(str(e))
                continue

        # Atualizar o arquivo metadata.json
        with open(metadata_path, 'w') as metadata_file:
            json.dump(metadata, metadata_file)

        # Adicionar metadata.json em available_assets
        metadata_asset = 'fotos/metadata.json'
        if metadata_asset not in self.task.available_assets:
            self.task.available_assets.append(metadata_asset)

        self.task.upload_and_cache_assets(True)
        self.task.images_count = len(self.task.scan_s3_assets())
        self.task.save()

        return {'success': True, 'uploaded': uploaded_files}

    def _upload_videos(self, files: list[str]):
        # Garantir que o diretório assets/videos existe
        videos_dir = self.task.assets_path("videos")
        ensure_path_exists(videos_dir)

        # Carregar o metadata.json existente, se existir
        metadata_path = os.path.join(videos_dir, 'metadata.json')
        metadata = self._read_metadata_json(metadata_path)

        uploaded_files = []

        # Identificar o índice inicial para novos arquivos
        video_prefix = os.path.join(videos_dir, "video_")
        existing_files_index = self._list_file_indexes_with(video_prefix, ".mp4")
        max_index = existing_files_index[-1] if len(existing_files_index) > 0 else 0

        # Salvar os novos arquivos na pasta assets/videos com nomes sequenciais
        for idx, filepath in enumerate(files):
            try:
                filename = f"video_{max_index + idx + 1}.mp4"
                dst_path = os.path.join(videos_dir, filename)

                # Gravar o arquivo no diretório de destino
                shutil.copyfile(filepath, dst_path)

                # Extrair metadados GPS do vídeo
                lat_lon = get_video_gps(dst_path)
                if lat_lon:
                    metadata[filename] = {'latitude': lat_lon[0], 'longitude': lat_lon[1]}
                    # Adicionar a informação em available_assets
                    asset_info = f"videos/{filename}"
                    if asset_info not in self.task.available_assets:
                        self.task.available_assets.append(asset_info)
                    uploaded_files.append(get_file_name(filepath))
                else:
                    os.remove(dst_path)  # Remover o arquivo se não contiver metadados GPS

            except Exception as e:
                logger.error(str(e))
                continue

        # Atualizar o arquivo metadata.json
        with open(metadata_path, 'w') as metadata_file:
            json.dump(metadata, metadata_file)

        # Adicionar metadata.json em available_assets
        metadata_asset = 'videos/metadata.json'
        if metadata_asset not in self.task.available_assets:
            self.task.available_assets.append(metadata_asset)

        self.task.upload_and_cache_assets(True)
        self.task.images_count = len(self.task.scan_s3_assets())
        self.task.save()

        return {'success': True, 'uploaded': uploaded_files}

    def _upload_foto360(self, files: list[str]):
        # Garantir que o diretório assets existe
        assets_dir = self.task.assets_path()
        ensure_path_exists(assets_dir)
    
        filepath = files[0] if len(files) > 0 else None
        file_uploaded = None

        if filepath:
            # Salvar o arquivo na pasta assets com o nome foto360.jpg
            file_uploaded = self.task.assets_path("foto360.jpg")
            shutil.copyfile(filepath, file_uploaded)

            # Adicionar "foto360.jpg" ao campo available_assets
            if "foto360.jpg" not in self.task.available_assets:
                self.task.available_assets.append("foto360.jpg")

        self.task.upload_and_cache_assets(True)
        self.task.images_count = len(self.task.scan_s3_assets())
        self.task.save()

        return {'success': True, 'uploaded': {'foto360.jpg': os.path.getsize(file_uploaded)}}

    def _upload_foto_giga(self, files: list[str]):
        uploaded_files = []

        # Salvar os novos arquivos na pasta assets/foto_giga com nomes sequenciais
        for idx, filepath in enumerate(files):
            try:
                filename = f"foto_giga_{idx + 1}.jpg"
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

                # Adicionar a informação em available_assets
                asset_info = f"foto_giga/metadata.dzi"
                self.task.available_assets = [asset for asset in self.task.available_assets if not ("foto_giga" in asset or "metadata.dzi" in asset)]
                self.task.save()
                if asset_info not in self.task.available_assets:
                    self.task.available_assets.append(asset_info)

                try:
                    os.remove(dst_path)  # Remover o arquivo se não contiver metadados GPS
                except:
                    pass

                uploaded_files.append(get_file_name(filepath))

            except Exception as e:
                continue

        self.task.upload_and_cache_assets(True)
        self.task.images_count = len(self.task.scan_s3_assets())
        self.task.save()

        return {'success': True, 'uploaded': uploaded_files}

    def _read_metadata_json(self, metadata_path):
        try:
            metadata_object = get_s3_object(metadata_path)
            if metadata_object:
                metadata = json.load(metadata_object['Body'])
            else:
                metadata = {}
        except Exception as e:
            print(e)
            metadata = {}
        
        return metadata
    
    def _list_file_indexes_with(self, prefix, suffix):
        existing_files = list_s3_objects(prefix)
        files_indexes = []

        for file in existing_files:
            filename = file['Key']
            if filename.endswith(suffix):
                index = int(filename.split(prefix)[1].split(suffix)[0])
                files_indexes.append(index)

        files_indexes.sort()

        return files_indexes
    
    def _download_files_from_s3(self, s3_images):
        downloaded_s3_images = []
        s3_client = get_s3_client()

        if not s3_client:
            logger.error('Could not download any image from s3, because is missing some s3 configuration variable')
            return []

        for image in s3_images:
            try:
                s3_image_name = image.split('/')[-1]
                destiny_path = tempfile.mktemp(f'_{s3_image_name}', dir=settings.MEDIA_TMP)
                download_s3_file(image, destiny_path, s3_client)
                downloaded_s3_images.append(destiny_path)
            except Exception as e:
                raise Exception(f"Error at download '{image}', maybe not found or not have permission. \nOriginal error: {str(e)}")

        logger.info(f'downloaded files {downloaded_s3_images}')
        return downloaded_s3_images
    
    def task_upload_in_progress(self, in_progress):
        self.task.upload_in_progress = in_progress
        self.task.save()


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
    gps_info = exif_data.get('GPSInfo')
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
    if ref in ['S', 'W']:
        degrees = -degrees
    return degrees


def get_video_gps(file_path):
    try:
        probe = ffmpeg.probe(file_path)
        tags = probe.get('format', {}).get('tags', {})
        location = tags.get('location')
        if location:
            # Remove a barra no final da string, se houver
            location = location.rstrip('/')

            # Definir a expressão regular para capturar latitude e longitude
            match = re.match(r'([+-]\d+\.\d+)([+-]\d+\.\d+)', location)
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
    tile_size=512
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
                tile.save(os.path.join(level_dir, f"{x // tile_size}_{y // tile_size}.jpg"))
                tile.close()
                gc.collect()

        resized_image.close()
        gc.collect()

    # Criar arquivo XML DZI
    root = ET.Element("Image", TileSize=str(tile_size), Overlap=str(overlap), Format="jpg", xmlns="http://schemas.microsoft.com/deepzoom/2008")
    ET.SubElement(root, "Size", Width=str(img_width), Height=str(img_height))
    tree = ET.ElementTree(root)
    path_metadata = os.path.join(dzi_dir, "metadata.dzi")
    with open(path_metadata, 'wb') as f:
            f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
            tree.write(f, encoding='utf-8', xml_declaration=False)
    image.close()
    gc.collect()

    return os.path.join(dzi_dir, "metadata.dzi")
