import os
import hashlib
import base64
import ffmpeg
import re

from typing import Union
from io import BytesIO, BufferedReader
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS


def ensure_path_exists(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def get_file_name(path: str):
    return os.path.basename(path)


def remove_path_from_path(complete_path: str, path_to_remove: str):
    path_to_remove_with_sep = (
        path_to_remove + os.sep if path_to_remove[-1] != os.sep else path_to_remove
    )

    return complete_path.replace(path_to_remove_with_sep, "")


def get_all_files_in_dir(dir):
    all_files: list[str] = []
    for entry in os.scandir(dir):
        if entry.is_dir():
            all_files += get_all_files_in_dir(entry.path)
        else:
            all_files.append(entry.path)

    return all_files


def list_dirs_in_dir(dir: str) -> list[str]:
    return [entry.path for entry in os.scandir(dir) if entry.is_dir()]


def ensure_sep_at_end(path: str):
    return path + os.sep if path[-1] != os.sep else path


def remove_sep_from_start(path: str):
    return path[1:] if path[-1] == os.sep else path


def human_readable_size(size_in_bytes: int):
    if size_in_bytes == 0:
        return "0 B"

    # Define as unidades de medida
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    power = 1024  # 1 KB = 1024 Bytes
    n = 0

    # Divida o valor até chegar na unidade adequada
    while size_in_bytes >= power and n < len(units) - 1:
        size_in_bytes /= power
        n += 1

    # Retorna o valor formatado com 2 casas decimais
    return f"{size_in_bytes:.2f} {units[n]}"


def calculate_sha256(path: str):
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return base64.b64encode(sha256.digest()).decode("utf-8")


def get_image_location(image_path_or_stream):
    image = Image.open(image_path_or_stream)
    exif_data = _get_exif_data(image)
    return _get_lat_lon_alt(exif_data)


def get_video_location(file_path):
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


def move_stream(source_stream: Union[BytesIO, BufferedReader], destiny_path: str):
    with open(destiny_path, "wb") as f:
        f.write(source_stream.read())
        f.flush()


def create_thumbnail(image_path: str, tamanho=(200, 200)):
    image = Image.open(image_path)
    image.thumbnail(tamanho)

    name, ext = os.path.splitext(image_path)
    thumbnail_path = f"{name}_thumb{ext}"

    image.save(thumbnail_path, format=image.format)

    return thumbnail_path


def delete_path(path: str):
    path_parts = path.split(os.sep)
    removed_all_empty_dirs = False

    while not removed_all_empty_dirs:
        path = os.sep.join(path_parts)

        if os.path.isfile(path):
            os.remove(path)
            path_parts.pop()
        elif os.path.isdir(path):
            if len(os.listdir(path)) == 0:
                os.rmdir(path)
                path_parts.pop()
            else:
                removed_all_empty_dirs = True
        else:
            return False

    return True


def _get_exif_data(image):
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


def _get_lat_lon_alt(exif_data):
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
        lat = _convert_to_degrees(lat, lat_ref)
        lon = _convert_to_degrees(lon, lon_ref)

        if alt:
            alt = float(alt)
            if alt_ref and alt_ref != 0:
                alt = -alt

        return lat, lon, alt
    return None


def _convert_to_degrees(value, ref):
    def to_degrees(val):
        d = float(val[0])
        m = float(val[1])
        s = float(val[2])
        return d + (m / 60.0) + (s / 3600.0)

    degrees = to_degrees(value)
    if ref in ["S", "W"]:
        degrees = -degrees
    return degrees
