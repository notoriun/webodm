import os
import re
import shutil
from wsgiref.util import FileWrapper

import mimetypes
import gc

from shutil import copyfileobj, move
from django.core.exceptions import ObjectDoesNotExist, SuspiciousFileOperation, ValidationError
from django.core.files.uploadedfile import InMemoryUploadedFile
from django.db import transaction
from django.http import FileResponse
from django.http import HttpResponse
from rest_framework import status, serializers, viewsets, filters, exceptions, permissions, parsers
from rest_framework.decorators import action
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from app import models, pending_actions, image_origins
from app.s3_utils import get_s3_object
from nodeodm import status_codes
from nodeodm.models import ProcessingNode
from worker import tasks as worker_tasks
from .common import get_and_check_project, get_asset_download_filename
from .tags import TagsField
from app.security import path_traversal_check
from django.utils.translation import gettext_lazy as _
from webodm import settings
from rest_framework.permissions import AllowAny
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
import json
import ffmpeg
import re
import xml.etree.ElementTree as ET
import piexif

Image.MAX_IMAGE_PIXELS = None

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


def flatten_files(request_files):
    # MultiValueDict in, flat array of files out
    return [file for filesList in map(
        lambda key: request_files.getlist(key),
        [keys for keys in request_files])
     for file in filesList]

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
        return False
    except Exception as e:
        print(f"Erro ao verificar metadados da imagem: {str(e)}")
        return False

class TaskIDsSerializer(serializers.BaseSerializer):
    permission_classes = [AllowAny]
    authentication_classes = []
    def to_representation(self, obj):
        return obj.id

class TaskSerializer(serializers.ModelSerializer):
    permission_classes = [AllowAny]
    authentication_classes = []
    project = serializers.PrimaryKeyRelatedField(queryset=models.Project.objects.all())
    processing_node = serializers.PrimaryKeyRelatedField(queryset=ProcessingNode.objects.all())
    processing_node_name = serializers.SerializerMethodField()
    can_rerun_from = serializers.SerializerMethodField()
    statistics = serializers.SerializerMethodField()
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
            rerun_from_option = list(filter(lambda d: 'name' in d and d['name'] == 'rerun-from', obj.processing_node.available_options))
            if len(rerun_from_option) > 0 and 'domain' in rerun_from_option[0]:
                return rerun_from_option[0]['domain']

        return []

    class Meta:
        model = models.Task
        exclude = ('orthophoto_extent', 'dsm_extent', 'dtm_extent', )
        read_only_fields = ('processing_time', 'status', 'last_error', 'created_at', 'pending_action', 'available_assets', 'size', )

class TaskViewSet(viewsets.ViewSet):
    """
    Task get/add/delete/update
    A task represents a set of images and other input to be sent to a processing node.
    Once a processing node completes processing, results are stored in the task.
    """
    queryset = models.Task.objects.all().defer('orthophoto_extent', 'dsm_extent', 'dtm_extent', )

    parser_classes = (parsers.MultiPartParser, parsers.JSONParser, parsers.FormParser, )
    ordering_fields = '__all__'

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        We don't use object level permissions on tasks, relying on
        project's object permissions instead (but standard model permissions still apply)
        and with the exception of 'retrieve' (task GET) for public tasks access
        """
        permission_classes = [permissions.AllowAny]
        #if self.action == 'retrieve':
        #    permission_classes = [permissions.AllowAny]
        #else:
        #    permission_classes = [permissions.DjangoModelPermissions, ]

        return [permission() for permission in permission_classes]

    def set_pending_action(self, pending_action, request, pk=None, project_pk=None, perms=('change_project', )):
        get_and_check_project(request, project_pk, perms)
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        task.pending_action = pending_action
        task.partial = False # Otherwise this will not be processed
        task.last_error = None
        task.save()

        # Process task right away
        worker_tasks.process_task.delay(task.id)

        return Response({'success': True})

    @action(detail=True, methods=['post'])
    def cancel(self, *args, **kwargs):
        return self.set_pending_action(pending_actions.CANCEL, *args, **kwargs)

    @action(detail=True, methods=['post'])
    def restart(self, *args, **kwargs):
        return self.set_pending_action(pending_actions.RESTART, *args, **kwargs)

    @action(detail=True, methods=['post'])
    def remove(self, *args, **kwargs):
        return self.set_pending_action(pending_actions.REMOVE, *args, perms=('delete_project', ), **kwargs)

    @action(detail=True, methods=['get'])
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

        line_num = max(0, int(request.query_params.get('line', 0)))
        return Response('\n'.join(task.console.output().rstrip().split('\n')[line_num:]))

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

    @action(detail=True, methods=['post'])
    def commit(self, request, pk=None, project_pk=None):
        """
        Commit a task after all images have been uploaded
        """
        get_and_check_project(request, project_pk, ('change_project', ))
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        task.partial = False
        task.images_count = len(task.scan_images())

        if task.images_count < 1:
            raise exceptions.ValidationError(detail=_("You need to upload at least 1 file before commit"))

        task.update_size()
        task.save()
        worker_tasks.process_task.delay(task.id)

        serializer = TaskSerializer(task)
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    def upload_images(self, task, files):
        if len(files) == 0:
            raise exceptions.ValidationError(detail=_("No files uploaded"))

        uploaded = task.handle_images_upload(files)
        task.images_count = len(task.scan_images())
        task.save()

        return {'success': True, 'uploaded': uploaded }

    def upload_fotos(self, task, files):
        # Garantir que o diretório assets/fotos existe
        fotos_dir = task.assets_path("fotos")
        if not os.path.exists(fotos_dir):
            os.makedirs(fotos_dir, exist_ok=True)

        # Carregar o metadata.json existente, se existir
        metadata_path = os.path.join(fotos_dir, 'metadata.json')
        try:
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as metadata_file:
                    metadata = json.load(metadata_file)
            else:
                metadata = {}
        except Exception as e:
            print(e)
            metadata = {}

        uploaded_files = []

        # Identificar o índice inicial para novos arquivos
        existing_files = os.listdir(fotos_dir)
        max_index = 0
        for file in existing_files:
            if file.startswith("foto_") and file.endswith(".jpg"):
                index = int(file.split('_')[1].split('.')[0])
                if index > max_index:
                    max_index = index

        # Salvar os novos arquivos na pasta assets/fotos com nomes sequenciais

        for idx, file in enumerate(files):
            try:
                # Manter o arquivo aberto e acessar diretamente os dados de memória
                if isinstance(file, InMemoryUploadedFile):
                    image = Image.open(file)
                    exif_data = get_exif_data(image)
                    lat_lon_alt = get_lat_lon_alt(exif_data)

                    if not lat_lon_alt:
                        continue

                    filename = f"foto_{max_index + idx + 1}.jpg"
                    dst_path = os.path.join(fotos_dir, filename)

                    # Gravar o arquivo no diretório de destino
                    with open(dst_path, 'wb+') as fd:
                        for chunk in file.chunks():
                            fd.write(chunk)
                else:
                    # Para arquivos temporários, abra o arquivo diretamente do caminho temporário
                    with open(file.temporary_file_path(), 'rb') as f:
                        image = Image.open(f)
                        exif_data = get_exif_data(image)
                        lat_lon_alt = get_lat_lon_alt(exif_data)


                        if not lat_lon_alt:
                            continue

                        filename = f"foto_{max_index + idx + 1}.jpg"
                        dst_path = os.path.join(fotos_dir, filename)

                        # Gravar o arquivo no diretório de destino
                        with open(dst_path, 'wb+') as fd:
                            f.seek(0)
                            shutil.copyfileobj(f, fd)

                # Adicionar a informação em available_assets
                asset_info = f"fotos/{filename}"
                if asset_info not in task.available_assets:
                    task.available_assets.append(asset_info)

                # Adicionar informações de metadados
                metadata[filename] = {'latitude': lat_lon_alt[0], 'longitude': lat_lon_alt[1], 'altitude': lat_lon_alt[2] or 0}
                uploaded_files.append(file.name)

            except Exception as e:
                print(e)
                continue

        # Atualizar o arquivo metadata.json
        with open(metadata_path, 'w') as metadata_file:
            json.dump(metadata, metadata_file)

        # Adicionar metadata.json em available_assets
        metadata_asset = 'fotos/metadata.json'
        if metadata_asset not in task.available_assets:
            task.available_assets.append(metadata_asset)

        task.images_count = len(task.scan_images())
        task.save()
        return {'success': True, 'uploaded': uploaded_files}

    def upload_videos(self, task, files):
        # Garantir que o diretório assets/videos existe
        videos_dir = task.assets_path("videos")
        if not os.path.exists(videos_dir):
            os.makedirs(videos_dir, exist_ok=True)

        # Carregar o metadata.json existente, se existir
        metadata_path = os.path.join(videos_dir, 'metadata.json')
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as metadata_file:
                metadata = json.load(metadata_file)
        else:
            metadata = {}

        uploaded_files = []

        # Identificar o índice inicial para novos arquivos
        existing_files = os.listdir(videos_dir)
        max_index = 0
        for file in existing_files:
            if file.startswith("video_") and file.endswith(".mp4"):
                index = int(file.split('_')[1].split('.')[0])
                if index > max_index:
                    max_index = index

        # Salvar os novos arquivos na pasta assets/videos com nomes sequenciais
        for idx, file in enumerate(files):
            try:
                filename = f"video_{max_index + idx + 1}.mp4"
                dst_path = os.path.join(videos_dir, filename)

                # Gravar o arquivo no diretório de destino
                with open(dst_path, 'wb+') as fd:
                    if isinstance(file, InMemoryUploadedFile):
                        for chunk in file.chunks():
                            fd.write(chunk)
                    else:
                        with open(file.temporary_file_path(), 'rb') as f:
                            shutil.copyfileobj(f, fd)

                # Extrair metadados GPS do vídeo
                lat_lon = get_video_gps(dst_path)
                if lat_lon:
                    metadata[filename] = {'latitude': lat_lon[0], 'longitude': lat_lon[1]}
                    # Adicionar a informação em available_assets
                    asset_info = f"videos/{filename}"
                    if asset_info not in task.available_assets:
                        task.available_assets.append(asset_info)
                    uploaded_files.append(file.name)
                else:
                    os.remove(dst_path)  # Remover o arquivo se não contiver metadados GPS

            except Exception as e:
                continue

        # Atualizar o arquivo metadata.json
        with open(metadata_path, 'w') as metadata_file:
            json.dump(metadata, metadata_file)

        # Adicionar metadata.json em available_assets
        metadata_asset = 'videos/metadata.json'
        if metadata_asset not in task.available_assets:
            task.available_assets.append(metadata_asset)

        task.images_count = len(task.scan_images())
        task.save()
        return {'success': True, 'uploaded': uploaded_files}

    def upload_foto360(self, task, files):
        print("upload_foto360")
    
        for file in files:
            print("file", file)
            
            # Salvar o arquivo temporariamente para verificar os metadados
            temp_path = os.path.join('/tmp', file.name)
            with open(temp_path, 'wb+') as temp_file:
                for chunk in file.chunks():
                    temp_file.write(chunk)
            
            #if not is_360_photo(temp_path):
            #    os.remove(temp_path)
            #    raise ValidationError("O arquivo não é uma foto 360")
            
            # Garantir que o diretório assets existe
            assets_dir = task.assets_path("")
            if not os.path.exists(assets_dir):
                os.makedirs(assets_dir, exist_ok=True)

            # Salvar o arquivo na pasta assets com o nome foto360.jpg
            dst_path = task.assets_path("foto360.jpg")
            print("dst_path", dst_path)
            with open(dst_path, 'wb+') as fd:
                for chunk in file.chunks():
                    print("chunk")
                    fd.write(chunk)

            # Remover o arquivo temporário
            os.remove(temp_path)

            # Adicionar "foto360.jpg" ao campo available_assets
            if "foto360.jpg" not in task.available_assets:
                task.available_assets.append("foto360.jpg")

            task.images_count = len(task.scan_images())
            task.save()
            
            return {'success': True, 'uploaded': {'foto360.jpg': os.path.getsize(dst_path)}}

    def upload_foto_giga(self, task, files):


        uploaded_files = []

        # Salvar os novos arquivos na pasta assets/foto_giga com nomes sequenciais
        for idx, file in enumerate(files):
            try:
                filename = f"foto_giga_{idx + 1}.jpg"
                # Garantir que o diretório assets/foto_giga existe
                foto_giga_dir = os.path.join(task.assets_path("foto_giga"))
                if os.path.exists(foto_giga_dir):
                    shutil.rmtree(foto_giga_dir)
                if not os.path.exists(foto_giga_dir):
                    os.makedirs(foto_giga_dir, exist_ok=True)
                dst_path = os.path.join(foto_giga_dir, filename)

                 # Manter o arquivo aberto e acessar diretamente os dados de memória
                #if isinstance(file, InMemoryUploadedFile):
                #    image = Image.open(file)
                #    # Converter para JPG e salvar
                #    image = image.convert("RGB")
                #    image.save(dst_path, "JPEG")
                #else:
                #    # Para arquivos temporários, abra o arquivo diretamente do caminho temporário
                #    with open(file.temporary_file_path(), 'rb') as f:
                #        image = Image.open(f)
                #        # Converter para JPG e salvar
                #        image = image.convert("RGB")
                #        image.save(dst_path, "JPEG")
                # Gravar o arquivo no diretório de destino
                with open(dst_path, 'wb+') as fd:
                    if isinstance(file, InMemoryUploadedFile):
                        for chunk in file.chunks():
                            fd.write(chunk)
                    else:
                        with open(file.temporary_file_path(), 'rb') as f:
                            shutil.copyfileobj(f, fd)

                # Criar arquivos DZI
                create_dzi(dst_path, foto_giga_dir)

                # Adicionar a informação em available_assets
                asset_info = f"foto_giga/metadata.dzi"
                task.available_assets = [asset for asset in task.available_assets if not ("foto_giga" in asset or "metadata.dzi" in asset)]
                task.save()
                if asset_info not in task.available_assets:
                    task.available_assets.append(asset_info)

                uploaded_files.append(file.name)

            except Exception as e:
                continue
        task.images_count = len(task.scan_images())
        task.save()
        return {'success': True, 'uploaded': uploaded_files}

    @action(detail=True, methods=['post'])
    def upload(self, request, pk=None, project_pk=None, type=""):
        project = get_and_check_project(request, project_pk, ('change_project', ))
        files = flatten_files(request.FILES)
        if len(files) == 0:
            raise exceptions.ValidationError(detail=_("No files uploaded"))

        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        try:

            upload_type = request.data.get('type', 'orthophoto')

            if upload_type == 'foto':
                response = self.upload_fotos(task, files)
            elif upload_type == 'video':
                response = self.upload_videos(task, files)
            elif upload_type == 'foto360':
                response = self.upload_foto360(task, files)
            elif upload_type == 'foto_giga':
                response = self.upload_foto_giga(task, files)
            else:  # Default to 'orthophoto'
                response = self.upload_images(task, files)

            # Atualizar outros parâmetros como nó de processamento, nome da tarefa, etc.
            serializer = TaskSerializer(task, data=request.data, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
        except Exception as e:
            print(e)
            return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

        return Response(response, status=status.HTTP_200_OK)

    @action(detail=True, methods=['post'], url_path="start-download-from-s3")
    def start_download_from_s3(self, request, pk=None, project_pk=None):
        """
        Download images from s3 to task
        """
        get_and_check_project(request, project_pk, ('change_project', ))
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        imagesParam: str = request.data.get('images', '')
        images = imagesParam.split(',')
        task.s3_images = [image.strip() for image in images if len(image.strip()) > 0]
        task.pending_action = pending_actions.IMPORT_FROM_S3_WITH_RESIZE if task.pending_action == pending_actions.RESIZE else pending_actions.IMPORT_FROM_S3
        task.partial = False
        task.image_origin = image_origins.S3
        task.save()
        worker_tasks.process_task.delay(task.id)
        return Response({'success': True, 'uploaded': images}, status=status.HTTP_200_OK)

    @action(detail=True, methods=['post'])
    def duplicate(self, request, pk=None, project_pk=None):
        """
        Duplicate a task
        """
        get_and_check_project(request, project_pk, ('change_project', ))
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        new_task = task.duplicate()
        if new_task:
            return Response({'success': True, 'task': TaskSerializer(new_task).data}, status=status.HTTP_200_OK)
        else:
            return Response({'error': _("Cannot duplicate task")}, status=status.HTTP_200_OK)

    def create(self, request, project_pk=None):
        project = get_and_check_project(request, project_pk, ('change_project', ))

        # If this is a partial task, we're going to upload images later
        # for now we just create a placeholder task.
        if request.data.get('partial'):
            task = models.Task.objects.create(project=project,
                                              pending_action=pending_actions.RESIZE if 'resize_to' in request.data else None)
            serializer = TaskSerializer(task, data=request.data, partial=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
        else:
            files = flatten_files(request.FILES)

            if len(files) <= 1:
                raise exceptions.ValidationError(detail=_("Cannot create task, you need at least 2 images"))

            with transaction.atomic():
                task = models.Task.objects.create(project=project,
                                                  pending_action=pending_actions.RESIZE if 'resize_to' in request.data else None)

                task.handle_images_upload(files)
                task.images_count = len(task.scan_images())

                # Update other parameters such as processing node, task name, etc.
                serializer = TaskSerializer(task, data=request.data, partial=True)
                serializer.is_valid(raise_exception=True)
                serializer.save()

                worker_tasks.process_task.delay(task.id)

        return Response(serializer.data, status=status.HTTP_201_CREATED)


    def update(self, request, pk=None, project_pk=None, partial=False):
        get_and_check_project(request, project_pk, ('change_project', ))
        try:
            task = self.queryset.get(pk=pk, project=project_pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        # Check that a user has access to reassign a project
        if 'project' in request.data:
            try:
                get_and_check_project(request, request.data['project'], ('change_project', ))
            except exceptions.NotFound:
                raise exceptions.PermissionDenied()

        serializer = TaskSerializer(task, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        serializer.save()

        # Process task right away
        worker_tasks.process_task.delay(task.id)

        return Response(serializer.data)

    def partial_update(self, request, *args, **kwargs):
        kwargs['partial'] = True
        return self.update(request, *args, **kwargs)


class TaskNestedView(APIView):
    queryset = models.Task.objects.all().defer('orthophoto_extent', 'dtm_extent', 'dsm_extent', )
    permission_classes = (AllowAny, )

    def get_and_check_task(self, request, pk, annotate={}):
        try:
            task = self.queryset.annotate(**annotate).get(pk=pk)
        except (ObjectDoesNotExist, ValidationError):
            raise exceptions.NotFound()

        # Check for permissions, unless the task is public
        #if not task.public:
        #    get_and_check_project(request, task.project.id)

        return task


def download_file_response(s3_object, content_disposition):
    file = s3_object['Body']

    # More than 100mb, normal http response, otherwise stream
    # Django docs say to avoid streaming when possible
    # stream = filesize > 1e8 or request.GET.get('_force_stream', False)
    content_type = s3_object['ContentType']
    response = HttpResponse(file,
                            content_type=content_type)

    response['Content-Type'] = content_type
    response['Content-Disposition'] = content_disposition
    response['Content-Length'] = s3_object['ContentLength']
    response['_stream'] = 'yes'

    return response


def download_file_stream(request, stream, content_disposition, download_filename=None):
    response = HttpResponse(FileWrapper(stream),
                            content_type=(mimetypes.guess_type(download_filename)[0] or "application/zip"))

    response['Content-Type'] = mimetypes.guess_type(download_filename)[0] or "application/zip"
    response['Content-Disposition'] = "{}; filename={}".format(content_disposition, download_filename)

    # For testing
    response['_stream'] = 'yes'

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

        # Verificar se é um pedido para DZI
        if asset.startswith("foto_giga") and asset.endswith(".dzi"):
            dzi_file_path = task.assets_path(asset)
            s3_object = get_s3_object(dzi_file_path)

            if not s3_object:
                raise exceptions.NotFound(_("Asset does not exist"))

            content_disposition = 'inline; filename={}'.format(os.path.basename(dzi_file_path))
            return download_file_response(s3_object, content_disposition)

        # Check and download
        try:
            asset_fs = task.get_asset_file_or_stream(asset)
        except FileNotFoundError:
            raise exceptions.NotFound(_("Asset does not exist"))

        is_stream = not isinstance(asset_fs, str)
        s3_object = get_s3_object(asset_fs) if not is_stream else None
        if not is_stream and not s3_object:
            raise exceptions.NotFound(_("Asset does not exist"))

        download_filename = request.GET.get('filename', get_asset_download_filename(task, asset))

        if is_stream:
            return download_file_stream(request, asset_fs, 'attachment', download_filename=download_filename)
        else:
            content_disposition = 'attachment; filename={}'.format(download_filename)
            return download_file_response(s3_object, content_disposition)

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
            asset_path = path_traversal_check(task.assets_path(unsafe_asset_path), task.assets_path(""))
        except SuspiciousFileOperation:
            raise exceptions.NotFound(_("Asset does not exist"))

        s3_key = task.assets_path(unsafe_asset_path)
        s3_object = get_s3_object(s3_key)

        if not s3_object:
            raise exceptions.NotFound(_("Asset does not exista"))
        
        content_disposition = 'inline; filename={}'.format(os.path.basename(asset_path))
        return download_file_response(s3_object, content_disposition)

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
            asset_fs = task.get_task_backup_stream()
        except FileNotFoundError:
            raise exceptions.NotFound(_("Asset does not exist"))

        download_filename = request.GET.get('filename', get_asset_download_filename(task, "backup.zip"))

        return download_file_stream(request, asset_fs, 'attachment', download_filename=download_filename)

"""
Task assets import
"""
class TaskAssetsImport(APIView):
    permission_classes = (permissions.AllowAny,)
    parser_classes = (parsers.MultiPartParser, parsers.JSONParser, parsers.FormParser,)

    def post(self, request, project_pk=None):
        project = get_and_check_project(request, project_pk, ('change_project',))

        files = flatten_files(request.FILES)
        import_url = request.data.get('url', None)
        task_name = request.data.get('name', _('Imported Task'))

        if not import_url and len(files) != 1:
            raise exceptions.ValidationError(detail=_("Cannot create task, you need to upload 1 file"))

        if import_url and len(files) > 0:
            raise exceptions.ValidationError(detail=_("Cannot create task, either specify a URL or upload 1 file."))

        chunk_index = request.data.get('dzchunkindex')
        uuid = request.data.get('dzuuid')
        total_chunk_count = request.data.get('dztotalchunkcount', None)

        # Chunked upload?
        tmp_upload_file = None
        if len(files) > 0 and chunk_index is not None and uuid is not None and total_chunk_count is not None:
            byte_offset = request.data.get('dzchunkbyteoffset', 0)

            try:
                chunk_index = int(chunk_index)
                byte_offset = int(byte_offset)
                total_chunk_count = int(total_chunk_count)
            except ValueError:
                raise exceptions.ValidationError(detail="Some parameters are not integers")
            uuid = re.sub('[^0-9a-zA-Z-]+', "", uuid)

            tmp_upload_file = os.path.join(settings.FILE_UPLOAD_TEMP_DIR, f"{uuid}.upload")
            if os.path.isfile(tmp_upload_file) and chunk_index == 0:
                os.unlink(tmp_upload_file)

            with open(tmp_upload_file, 'ab') as fd:
                fd.seek(byte_offset)
                if isinstance(files[0], InMemoryUploadedFile):
                    for chunk in files[0].chunks():
                        fd.write(chunk)
                else:
                    with open(files[0].temporary_file_path(), 'rb') as file:
                        fd.write(file.read())

            if chunk_index + 1 < total_chunk_count:
                return Response({'uploaded': True}, status=status.HTTP_200_OK)

        # Ready to import
        with transaction.atomic():
            task = models.Task.objects.create(project=project,
                                            auto_processing_node=False,
                                            name=task_name,
                                            import_url=import_url if import_url else "file://all.zip",
                                            status=status_codes.RUNNING,
                                            pending_action=pending_actions.IMPORT)
            task.create_task_directories()
            destination_file = task.assets_path("all.zip")

            # Non-chunked file import
            if tmp_upload_file is None and len(files) > 0:
                with open(destination_file, 'wb+') as fd:
                    if isinstance(files[0], InMemoryUploadedFile):
                        for chunk in files[0].chunks():
                            fd.write(chunk)
                    else:
                        with open(files[0].temporary_file_path(), 'rb') as file:
                            copyfileobj(file, fd)
            elif tmp_upload_file is not None:
                # Move
                shutil.move(tmp_upload_file, destination_file)

            worker_tasks.process_task.delay(task.id)

        serializer = TaskSerializer(task)
        return Response(serializer.data, status=status.HTTP_201_CREATED)
