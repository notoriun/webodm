import logging
import os
import shutil
import time
import struct
import json
import errno
import piexif
import re
import zipfile
import rasterio
import uuid as uuid_module
import worker.cache_files as worker_cache_files_tasks

from datetime import datetime
from app.vendor import zipfly


from shutil import copyfile
import requests
from PIL import Image
from django.contrib.gis.gdal import GDALRaster
from django.contrib.gis.gdal import OGRGeometry
from django.contrib.gis.geos import GEOSGeometry
from django.contrib.postgres import fields
from django.core.files.uploadedfile import InMemoryUploadedFile
from django.core.exceptions import ValidationError, SuspiciousFileOperation
from django.db import models
from django.db import transaction
from django.db import connection
from django.utils import timezone
from urllib3.exceptions import ReadTimeoutError

from app import pending_actions
from django.contrib.gis.db.models.fields import GeometryField

from app.cogeo import assure_cogeo
from app.pointcloud_utils import is_pointcloud_georeferenced
from app.testwatch import testWatch
from app.security import path_traversal_check
from nodeodm import status_codes
from nodeodm.models import ProcessingNode
from pyodm.exceptions import (
    NodeResponseError,
    NodeConnectionError,
    NodeServerError,
    OdmError,
)
from webodm import settings
from app.classes.gcp import GCPFile
from .project import Project
from django.utils.translation import gettext_lazy as _, gettext

from functools import partial
from app.classes.console import Console
from app.utils.s3_utils import (
    get_s3_client,
    list_s3_objects,
    download_s3_file,
    get_s3_object_metadata,
    append_s3_bucket_prefix,
    convert_task_path_to_s3,
    get_object_checksum,
)
from app.utils.file_utils import (
    ensure_path_exists,
    remove_path_from_path,
    get_all_files_in_dir,
    get_file_name,
    ensure_sep_at_end,
    calculate_sha256,
)

logger = logging.getLogger("app.logger")


class TaskInterruptedException(Exception):
    pass


def task_directory_path(taskId, projectId):
    # return 'project/{0}/task/{1}/'.format(projectId, taskId)
    return "{0}/{1}/".format(projectId, taskId)


def full_task_directory_path(taskId, projectId, *args):
    return os.path.join(
        settings.MEDIA_ROOT, task_directory_path(taskId, projectId), *args
    )
    # return os.path.join(task_directory_path(taskId, projectId), *args)


def assets_directory_path(taskId, projectId, filename):
    # files will be uploaded to MEDIA_ROOT/project/<id>/task/<id>/<filename>
    return "{0}{1}".format(task_directory_path(taskId, projectId), filename)


def gcp_directory_path(task, filename):
    return assets_directory_path(task.id, task.project.id, filename)


def validate_task_options(value):
    """
    Make sure that the format of this options field is valid
    """
    if len(value) == 0:
        return

    try:
        for option in value:
            if not option["name"]:
                raise ValidationError("Name key not found in option")
            if not option["value"]:
                raise ValidationError("Value key not found in option")
    except:
        raise ValidationError("Invalid options")


def resize_image(image_path, resize_to, done=None):
    """
    :param image_path: path to the image
    :param resize_to: target size to resize this image to (largest side)
    :param done: optional callback
    :return: path and resize ratio
    """
    try:
        can_resize = False

        # Check if this image can be resized
        # There's no easy way to resize multispectral 16bit images
        # (Support should be added to PIL)
        is_jpeg = re.match(r".*\.jpe?g$", image_path, re.IGNORECASE)

        if is_jpeg:
            # We can always resize these
            can_resize = True
        else:
            try:
                bps = piexif.load(image_path)["0th"][piexif.ImageIFD.BitsPerSample]
                if isinstance(bps, int):
                    # Always resize single band images
                    can_resize = True
                elif isinstance(bps, tuple) and len(bps) > 1:
                    # Only resize multiband images if depth is 8bit
                    can_resize = bps == (8,) * len(bps)
                else:
                    logger.warning(
                        "Cannot determine if image %s can be resized, hoping for the best!"
                        % image_path
                    )
                    can_resize = True
            except KeyError:
                logger.warning("Cannot find BitsPerSample tag for %s" % image_path)

        if not can_resize:
            logger.warning("Cannot resize %s" % image_path)
            return {"path": image_path, "resize_ratio": 1}

        im = Image.open(image_path)
        path, ext = os.path.splitext(image_path)
        resized_image_path = os.path.join(path + ".resized" + ext)

        width, height = im.size
        max_side = max(width, height)
        if max_side < resize_to:
            logger.warning(
                "You asked to make {} bigger ({} --> {}), but we are not going to do that.".format(
                    image_path, max_side, resize_to
                )
            )
            im.close()
            return {"path": image_path, "resize_ratio": 1}

        ratio = float(resize_to) / float(max_side)
        resized_width = int(width * ratio)
        resized_height = int(height * ratio)

        im = im.resize((resized_width, resized_height), Image.LANCZOS)
        params = {}
        if is_jpeg:
            params["quality"] = 100

        if "exif" in im.info:
            exif_dict = piexif.load(im.info["exif"])
            # exif_dict['Exif'][piexif.ExifIFD.PixelXDimension] = resized_width
            # exif_dict['Exif'][piexif.ExifIFD.PixelYDimension] = resized_height
            im.save(resized_image_path, exif=piexif.dump(exif_dict), **params)
        else:
            im.save(resized_image_path, **params)

        im.close()

        # Delete original image, rename resized image to original
        os.remove(image_path)
        os.rename(resized_image_path, image_path)

        logger.info(
            "Resized {} to {}x{}".format(image_path, resized_width, resized_height)
        )
    except (IOError, ValueError, struct.error) as e:
        logger.warning("Cannot resize {}: {}.".format(image_path, str(e)))
        if done is not None:
            done()
        return None

    retval = {"path": image_path, "resize_ratio": ratio}

    if done is not None:
        done(retval)

    return retval


class Task(models.Model):
    ASSETS_MAP = {
        "all.zip": {"deferred_path": "all.zip", "deferred_compress_dir": "."},
        "orthophoto.tif": os.path.join("odm_orthophoto", "odm_orthophoto.tif"),
        "orthophoto.png": os.path.join("odm_orthophoto", "odm_orthophoto.png"),
        "orthophoto.mbtiles": os.path.join("odm_orthophoto", "odm_orthophoto.mbtiles"),
        "orthophoto.kmz": os.path.join("odm_orthophoto", "odm_orthophoto.kmz"),
        "georeferenced_model.las": os.path.join(
            "odm_georeferencing", "odm_georeferenced_model.las"
        ),
        "georeferenced_model.laz": os.path.join(
            "odm_georeferencing", "odm_georeferenced_model.laz"
        ),
        "georeferenced_model.ply": os.path.join(
            "odm_georeferencing", "odm_georeferenced_model.ply"
        ),
        "georeferenced_model.csv": os.path.join(
            "odm_georeferencing", "odm_georeferenced_model.csv"
        ),
        "textured_model.zip": {
            "deferred_path": "textured_model.zip",
            "deferred_compress_dir": "odm_texturing",
            "deferred_exclude_files": ("odm_textured_model_geo.glb",),
        },
        "textured_model.glb": os.path.join(
            "odm_texturing", "odm_textured_model_geo.glb"
        ),
        "3d_tiles_model.zip": {
            "deferred_path": "3d_tiles_model.zip",
            "deferred_compress_dir": os.path.join("3d_tiles", "model"),
        },
        "3d_tiles_pointcloud.zip": {
            "deferred_path": "3d_tiles_pointcloud.zip",
            "deferred_compress_dir": os.path.join("3d_tiles", "pointcloud"),
        },
        "dtm.tif": os.path.join("odm_dem", "dtm.tif"),
        "dsm.tif": os.path.join("odm_dem", "dsm.tif"),
        "dtm_tiles.zip": {
            "deferred_path": "dtm_tiles.zip",
            "deferred_compress_dir": "dtm_tiles",
        },
        "dsm_tiles.zip": {
            "deferred_path": "dsm_tiles.zip",
            "deferred_compress_dir": "dsm_tiles",
        },
        "orthophoto_tiles.zip": {
            "deferred_path": "orthophoto_tiles.zip",
            "deferred_compress_dir": "orthophoto_tiles",
        },
        "cameras.json": "cameras.json",
        "shots.geojson": os.path.join("odm_report", "shots.geojson"),
        "report.pdf": os.path.join("odm_report", "report.pdf"),
        "ground_control_points.geojson": os.path.join(
            "odm_georeferencing", "ground_control_points.geojson"
        ),
    }

    STATUS_CODES = (
        (status_codes.QUEUED, "QUEUED"),
        (status_codes.RUNNING, "RUNNING"),
        (status_codes.FAILED, "FAILED"),
        (status_codes.COMPLETED, "COMPLETED"),
        (status_codes.CANCELED, "CANCELED"),
    )

    PENDING_ACTIONS = (
        (pending_actions.CANCEL, "CANCEL"),
        (pending_actions.REMOVE, "REMOVE"),
        (pending_actions.RESTART, "RESTART"),
        (pending_actions.RESIZE, "RESIZE"),
        (pending_actions.IMPORT, "IMPORT"),
        (pending_actions.IMPORT_FROM_S3, "IMPORT_FROM_S3"),
        (pending_actions.IMPORT_FROM_S3_WITH_RESIZE, "IMPORT_FROM_S3_WITH_RESIZE"),
        (pending_actions.UPLOAD_TO_S3, "UPLOAD_TO_S3"),
    )

    TASK_PROGRESS_LAST_VALUE = 0.85

    id = models.UUIDField(
        primary_key=True,
        default=uuid_module.uuid4,
        unique=True,
        serialize=False,
        editable=False,
        verbose_name=_("Id"),
    )

    uuid = models.CharField(
        max_length=255,
        db_index=True,
        default="",
        blank=True,
        help_text=_("Identifier of the task (as returned by NodeODM API)"),
        verbose_name=_("UUID"),
    )
    project = models.ForeignKey(
        Project,
        on_delete=models.CASCADE,
        help_text=_("Project that this task belongs to"),
        verbose_name=_("Project"),
    )
    name = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text=_("A label for the task"),
        verbose_name=_("Name"),
    )
    processing_time = models.IntegerField(
        default=-1,
        help_text=_(
            "Number of milliseconds that elapsed since the beginning of this task (-1 indicates that no information is available)"
        ),
        verbose_name=_("Processing Time"),
    )
    processing_node = models.ForeignKey(
        ProcessingNode,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        help_text=_(
            "Processing node assigned to this task (or null if this task has not been associated yet)"
        ),
        verbose_name=_("Processing Node"),
    )
    auto_processing_node = models.BooleanField(
        default=True,
        help_text=_(
            "A flag indicating whether this task should be automatically assigned a processing node"
        ),
        verbose_name=_("Auto Processing Node"),
    )
    status = models.IntegerField(
        choices=STATUS_CODES,
        db_index=True,
        null=True,
        blank=True,
        help_text=_("Current status of the task"),
        verbose_name=_("Status"),
    )
    last_error = models.TextField(
        null=True,
        blank=True,
        help_text=_("The last processing error received"),
        verbose_name=_("Last Error"),
    )
    options = fields.JSONField(
        default=dict,
        blank=True,
        help_text=_("Options that are being used to process this task"),
        validators=[validate_task_options],
        verbose_name=_("Options"),
    )
    available_assets = fields.ArrayField(
        models.CharField(max_length=80),
        default=list,
        blank=True,
        help_text=_("List of available assets to download"),
        verbose_name=_("Available Assets"),
    )

    orthophoto_extent = GeometryField(
        null=True,
        blank=True,
        srid=4326,
        help_text=_("Extent of the orthophoto"),
        verbose_name=_("Orthophoto Extent"),
    )
    dsm_extent = GeometryField(
        null=True,
        blank=True,
        srid=4326,
        help_text="Extent of the DSM",
        verbose_name=_("DSM Extent"),
    )
    dtm_extent = GeometryField(
        null=True,
        blank=True,
        srid=4326,
        help_text="Extent of the DTM",
        verbose_name=_("DTM Extent"),
    )

    # mission
    created_at = models.DateTimeField(
        default=timezone.now, help_text=_("Creation date"), verbose_name=_("Created at")
    )
    pending_action = models.IntegerField(
        choices=PENDING_ACTIONS,
        db_index=True,
        null=True,
        blank=True,
        help_text=_(
            "A requested action to be performed on the task. The selected action will be performed by the worker at the next iteration."
        ),
        verbose_name=_("Pending Action"),
    )

    public = models.BooleanField(
        default=False,
        help_text=_("A flag indicating whether this task is available to the public"),
        verbose_name=_("Public"),
    )
    resize_to = models.IntegerField(
        default=-1,
        help_text=_(
            "When set to a value different than -1, indicates that the images for this task have been / will be resized to the size specified here before processing."
        ),
        verbose_name=_("Resize To"),
    )

    upload_progress = models.FloatField(
        default=0.0,
        help_text=_(
            "Value between 0 and 1 indicating the upload progress of this task's files to the processing node"
        ),
        verbose_name=_("Upload Progress"),
        blank=True,
    )
    resize_progress = models.FloatField(
        default=0.0,
        help_text=_(
            "Value between 0 and 1 indicating the resize progress of this task's images"
        ),
        verbose_name=_("Resize Progress"),
        blank=True,
    )
    running_progress = models.FloatField(
        default=0.0,
        help_text=_(
            "Value between 0 and 1 indicating the running progress (estimated) of this task"
        ),
        verbose_name=_("Running Progress"),
        blank=True,
    )
    downloading_s3_progress = models.FloatField(
        default=0.0,
        help_text=_(
            "Value between 0 and 1 indicating the downloading images from s3 progress (estimated) of this task"
        ),
        verbose_name=_("Downloading Images from S3 Progress"),
        blank=True,
    )
    uploading_s3_progress = models.FloatField(
        default=0.0,
        help_text=_(
            "Value between 0 and 1 indicating the uploading images to s3 progress (estimated) of this task"
        ),
        verbose_name=_("Uploading Images to S3 Progress"),
        blank=True,
    )
    import_url = models.TextField(
        null=False,
        default="",
        blank=True,
        help_text=_("URL this task is imported from (only for imported tasks)"),
        verbose_name=_("Import URL"),
    )
    images_count = models.IntegerField(
        null=False,
        blank=True,
        default=0,
        help_text=_("Number of images associated with this task"),
        verbose_name=_("Images Count"),
    )
    partial = models.BooleanField(
        default=False,
        help_text=_(
            "A flag indicating whether this task is currently waiting for information or files to be uploaded before being considered for processing."
        ),
        verbose_name=_("Partial"),
    )
    potree_scene = fields.JSONField(
        default=dict,
        blank=True,
        help_text=_(
            "Serialized potree scene information used to save/load measurements and camera view angle"
        ),
        verbose_name=_("Potree Scene"),
    )
    epsg = models.IntegerField(
        null=True,
        default=None,
        blank=True,
        help_text=_("EPSG code of the dataset (if georeferenced)"),
        verbose_name="EPSG",
    )
    tags = models.TextField(
        db_index=True,
        default="",
        blank=True,
        help_text=_("Task tags"),
        verbose_name=_("Tags"),
    )
    orthophoto_bands = fields.JSONField(
        default=list,
        blank=True,
        help_text=_("List of orthophoto bands"),
        verbose_name=_("Orthophoto Bands"),
    )
    size = models.FloatField(
        default=0.0,
        blank=True,
        help_text=_("Size of the task on disk in megabytes"),
        verbose_name=_("Size"),
    )
    s3_images = fields.JSONField(
        default=list,
        blank=True,
        help_text=_("List s3 buckets with images"),
        verbose_name=_("S3 buckets images"),
    )
    upload_in_progress = models.BooleanField(
        default=False,
        help_text=_("A flag indicating whether this task is in upload progress"),
        verbose_name=_("Upload In Progress"),
    )
    s3_assets = fields.JSONField(
        default=list,
        blank=True,
        help_text=_("List s3 buckets with assets"),
        verbose_name=_("S3 assets buckets"),
    )

    class Meta:
        verbose_name = _("Task")
        verbose_name_plural = _("Tasks")

    def __init__(self, *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

        # To help keep track of changes to the project id
        self.__original_project_id = self.project.id

        self.console = Console(self.data_path("console_output.txt"))

    def __str__(self):
        name = self.name if self.name is not None else gettext("unnamed")

        return "Task [{}] ({})".format(name, self.id)

    def move_assets(self, old_project_id, new_project_id):
        """
        Moves the task's folder, update ImageFields and orthophoto files to a new project
        """
        old_task_folder = full_task_directory_path(self.id, old_project_id)
        new_task_folder = full_task_directory_path(self.id, new_project_id)
        new_task_folder_parent = os.path.abspath(
            os.path.join(new_task_folder, os.pardir)
        )

        try:
            if os.path.exists(old_task_folder) and not os.path.exists(new_task_folder):
                # Use parent, otherwise we get a duplicate directory in there
                if not os.path.exists(new_task_folder_parent):
                    os.makedirs(new_task_folder_parent)

                shutil.move(old_task_folder, new_task_folder_parent)

                logger.info(
                    "Moved task folder from {} to {}".format(
                        old_task_folder, new_task_folder
                    )
                )
            else:
                logger.warning(
                    "Project changed for task {}, but either {} doesn't exist, or {} already exists. This doesn't look right, so we will not move any files.".format(
                        self, old_task_folder, new_task_folder
                    )
                )
        except shutil.Error as e:
            logger.warning(
                "Could not move assets folder for task {}. We're going to proceed anyway, but you might experience issues: {}".format(
                    self, e
                )
            )

    def save(self, *args, **kwargs):
        if self.project.id != self.__original_project_id:
            self.move_assets(self.__original_project_id, self.project.id)
            self.__original_project_id = self.project.id

        # Manually validate the fields we want,
        # since Django's clean_fields() method obliterates
        # our foreign keys without explanation :/
        errors = {}
        for f in self._meta.fields:
            if f.attname in ["options"]:
                raw_value = getattr(self, f.attname)
                if f.blank and raw_value in f.empty_values:
                    continue

                try:
                    setattr(self, f.attname, f.clean(raw_value, self))
                except ValidationError as e:
                    errors[f.name] = e.error_list

        if errors:
            raise ValidationError(errors)

        self.clean()
        self.validate_unique()

        super(Task, self).save(*args, **kwargs)

    def assets_path(self, *args):
        """
        Get a path relative to the place where assets are stored
        """
        return self.task_path("assets", *args)

    def data_path(self, *args):
        """
        Path to task data that does not fit in database fields (e.g. console output)
        """
        return self.task_path("data", *args)

    def task_path(self, *args):
        """
        Get path relative to the root task directory
        """
        return os.path.join(
            settings.MEDIA_ROOT,
            assets_directory_path(self.id, self.project.id, ""),
            *args,
        )

    def is_asset_available_slow(self, asset):
        """
        Checks whether a particular asset is available in the file system
        Generally this should never be used directly, as it's slow. Use the available_assets field
        in the database instead.
        :param asset: one of ASSETS_MAP keys
        :return: boolean
        """
        if asset in self.ASSETS_MAP:
            value = self.ASSETS_MAP[asset]
            if isinstance(value, str):
                if os.path.exists(self.assets_path(value)):
                    return True
            elif isinstance(value, dict):
                if "deferred_compress_dir" in value:
                    if os.path.exists(self.assets_path(value["deferred_compress_dir"])):
                        return True

        # Additional checks for 'foto360.jpg' and files in 'fotos' or 'videos' directories
        if asset == "foto360.jpg":
            return os.path.exists(self.assets_path("foto360.jpg"))
        if asset.startswith("fotos/") or asset.startswith("videos/"):
            return os.path.exists(self.assets_path(asset))

        return False

    def get_statistics(self):
        """
        Parse ODM's stats.json if available
        """
        stats_json = self.assets_path("odm_report", "stats.json")
        if os.path.exists(stats_json):
            try:
                with open(stats_json) as f:
                    j = json.loads(f.read())
            except Exception as e:
                logger.warning("Malformed JSON {}: {}".format(stats_json, str(e)))
                return {}

            points = None
            if j.get("point_cloud_statistics", {}).get("dense", False):
                points = (
                    j.get("point_cloud_statistics", {})
                    .get("stats", {})
                    .get("statistic", [{}])[0]
                    .get("count")
                )
            else:
                points = j.get("reconstruction_statistics", {}).get(
                    "reconstructed_points_count"
                )

            return {
                "pointcloud": {
                    "points": points,
                },
                "gsd": j.get("odm_processing_statistics", {}).get("average_gsd"),
                "area": j.get("processing_statistics", {}).get("area"),
                "start_date": j.get("processing_statistics", {}).get("start_date"),
                "end_date": j.get("processing_statistics", {}).get("end_date"),
            }
        else:
            return {}

    def duplicate(self, set_new_name=True):
        try:
            with transaction.atomic():
                task = Task.objects.get(pk=self.pk)
                task.pk = None
                if set_new_name:
                    task.name = gettext("Copy of %(task)s") % {"task": self.name}
                task.created_at = timezone.now()
                task.save()
                task.refresh_from_db()

                logger.info("Duplicating {} to {}".format(self, task))

                if os.path.isdir(self.task_path()):
                    try:
                        # Try to use hard links first
                        shutil.copytree(
                            self.task_path(), task.task_path(), copy_function=os.link
                        )
                    except Exception as e:
                        logger.warning(
                            "Cannot duplicate task using hard links, will use normal copy instead: {}".format(
                                str(e)
                            )
                        )
                        shutil.copytree(self.task_path(), task.task_path())
                else:
                    logger.warning(
                        "Task {} doesn't have folder, will skip copying".format(self)
                    )

                self.project.owner.profile.clear_used_quota_cache()

                from app.plugins import signals as plugin_signals

                plugin_signals.task_duplicated.send_robust(
                    sender=self.__class__, task_id=task.id
                )

            return task
        except Exception as e:
            logger.warning("Cannot duplicate task: {}".format(str(e)))

        return False

    def write_backup_file(self):
        """Dump this tasks's fields to a backup file"""
        with open(self.data_path("backup.json"), "w") as f:
            f.write(
                json.dumps(
                    {
                        "name": self.name,
                        "processing_time": self.processing_time,
                        "options": self.options,
                        "created_at": self.created_at.astimezone(
                            timezone.utc
                        ).timestamp(),
                        "public": self.public,
                        "resize_to": self.resize_to,
                        "potree_scene": self.potree_scene,
                        "tags": self.tags,
                    }
                )
            )

    def read_backup_file(self):
        """Set this tasks fields based on the backup file (but don't save)"""
        backup_file = self.data_path("backup.json")
        if os.path.isfile(backup_file):
            try:
                with open(backup_file, "r") as f:
                    backup = json.loads(f.read())

                    self.name = backup.get("name", self.name)
                    self.processing_time = backup.get(
                        "processing_time", self.processing_time
                    )
                    self.options = backup.get("options", self.options)
                    self.created_at = datetime.fromtimestamp(
                        backup.get(
                            "created_at",
                            self.created_at.astimezone(timezone.utc).timestamp(),
                        ),
                        tz=timezone.utc,
                    )
                    self.public = backup.get("public", self.public)
                    self.resize_to = backup.get("resize_to", self.resize_to)
                    self.potree_scene = backup.get("potree_scene", self.potree_scene)
                    self.tags = backup.get("tags", self.tags)

            except Exception as e:
                logger.warning("Cannot read backup file: %s" % str(e))

    def get_task_backup_stream(self):
        self._download_all_s3_images()
        self.write_backup_file()
        zip_dir = self.task_path("")
        paths = [
            {
                "n": os.path.relpath(os.path.join(dp, f), zip_dir),
                "fs": os.path.join(dp, f),
            }
            for dp, dn, filenames in os.walk(zip_dir)
            for f in filenames
        ]
        if len(paths) == 0:
            raise FileNotFoundError("No files available for export")

        def on_streamed_backup():
            logger.info("streamou o zip de backup")

        return zipfly.ZipStream(paths, on_streamed_backup)

    def is_asset_a_zip(self, asset):
        if not (asset in self.ASSETS_MAP):
            return False

        value = self.ASSETS_MAP[asset]
        return (
            isinstance(value, dict)
            and "deferred_path" in value
            and "deferred_compress_dir" in value
        )

    def get_asset_file_or_stream(self, asset):
        """
        Get a stream to an asset
        :param asset: one of ASSETS_MAP keys
        :return: (path|stream)
        """
        if asset in self.ASSETS_MAP:
            value = self.ASSETS_MAP[asset]
            if isinstance(value, str):
                return self.assets_path(value)

            elif isinstance(value, dict):
                if "deferred_path" in value and "deferred_compress_dir" in value:
                    zip_dir = self.assets_path(value["deferred_compress_dir"])
                    paths = [
                        {
                            "n": os.path.relpath(os.path.join(dp, f), zip_dir),
                            "fs": os.path.join(dp, f),
                        }
                        for dp, dn, filenames in os.walk(zip_dir)
                        for f in filenames
                    ]
                    if "deferred_exclude_files" in value and isinstance(
                        value["deferred_exclude_files"], tuple
                    ):
                        paths = [
                            p
                            for p in paths
                            if os.path.basename(p["fs"])
                            not in value["deferred_exclude_files"]
                        ]
                    if len(paths) == 0:
                        raise FileNotFoundError("No files available for download")
                    return zipfly.ZipStream(paths)
                else:
                    raise FileNotFoundError(
                        "{} is not a valid asset (invalid dict values)".format(asset)
                    )
            else:
                raise FileNotFoundError(
                    "{} is not a valid asset (invalid map)".format(asset)
                )
        else:
            raise FileNotFoundError("{} is not a valid asset".format(asset))

    def get_asset_download_path(self, asset):
        """
        Get the path to an asset download
        :param asset: one of ASSETS_MAP keys
        :return: path
        """
        if asset in self.ASSETS_MAP:
            value = self.ASSETS_MAP[asset]
            if isinstance(value, str):
                return self.assets_path(value)

            elif isinstance(value, dict):
                if "deferred_path" in value and "deferred_compress_dir" in value:
                    return value["deferred_path"]
                else:
                    raise FileNotFoundError(
                        "{} is not a valid asset (invalid dict values)".format(asset)
                    )
            else:
                raise FileNotFoundError(
                    "{} is not a valid asset (invalid map)".format(asset)
                )
        else:
            raise FileNotFoundError("{} is not a valid asset".format(asset))

    def handle_import(self):
        self.console += gettext("Importing assets...") + "\n"
        self.save()

        zip_path = self.assets_path("all.zip")
        # Import assets file from mounted system volume (media-dir)/imports by relative path.
        # Import file from relative path.
        if self.import_url and not os.path.exists(zip_path):
            if self.import_url.startswith("file://"):
                imports_folder_path = os.path.join(settings.MEDIA_ROOT, "imports")
                unsafe_path_to_import_file = os.path.join(
                    settings.MEDIA_ROOT,
                    "imports",
                    self.import_url.replace("file://", ""),
                )
                # check is file placed in shared media folder in /imports directory without traversing
                try:
                    checked_path_to_file = path_traversal_check(
                        unsafe_path_to_import_file, imports_folder_path
                    )
                    if os.path.isfile(checked_path_to_file):
                        copyfile(checked_path_to_file, zip_path)
                except SuspiciousFileOperation as e:
                    logger.error(
                        "Error due importing assets from {} for {} in cause of path checking error".format(
                            self.import_url, self
                        )
                    )
                    raise NodeServerError(e)
            else:
                try:
                    # TODO: this is potentially vulnerable to a zip bomb attack
                    #       mitigated by the fact that a valid account is needed to
                    #       import tasks
                    logger.info(
                        "Importing task assets from {} for {}".format(
                            self.import_url, self
                        )
                    )
                    download_stream = requests.get(
                        self.import_url, stream=True, timeout=10
                    )
                    content_length = download_stream.headers.get("content-length")
                    total_length = (
                        int(content_length) if content_length is not None else None
                    )
                    downloaded = 0
                    last_update = 0

                    with open(zip_path, "wb") as fd:
                        for chunk in download_stream.iter_content(4096):
                            downloaded += len(chunk)

                            if time.time() - last_update >= 2:
                                # Update progress
                                if total_length is not None:
                                    Task.objects.filter(pk=self.id).update(
                                        running_progress=(
                                            float(downloaded) / total_length
                                        )
                                        * 0.9
                                    )

                                self.check_if_canceled()
                                last_update = time.time()

                            fd.write(chunk)

                except (
                    requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError,
                    ReadTimeoutError,
                ) as e:
                    raise NodeServerError(e)

        self.refresh_from_db()

        try:
            self.extract_assets_and_complete()
        except zipfile.BadZipFile:
            raise NodeServerError(gettext("Invalid zip file"))

        images_json = self.assets_path("images.json")
        if os.path.exists(images_json):
            try:
                with open(images_json) as f:
                    images = json.load(f)
                    self.images_count = len(images)
            except:
                logger.warning(
                    "Cannot read images count from imported task {}".format(self)
                )
                pass

        if self.pending_action != pending_actions.UPLOAD_TO_S3:
            self.pending_action = None
        self.save()

    def handle_s3_import(self):
        logger.info("donwloading images from s3")
        self.downloading_s3_progress = 0.0
        self.save()

        class DownloadProgressCallback(object):
            def __init__(self, task: Task, downloaded_images, total_size):
                self._size = total_size
                self._downloaded_bytes = 0.0
                self._task = task
                self._downloaded_images = downloaded_images

            def __call__(self, bytes_transferred):
                self._downloaded_bytes += bytes_transferred
                progress = self._downloaded_bytes / self._size if self._size > 0 else 0
                self._task._update_download_progress(self._downloaded_images, progress)
                logger.info(
                    "Download from S3 percent {}%".format(
                        self._task.downloading_s3_progress * 100
                    )
                )

        try:
            self._remove_root_images()
            s3_client = get_s3_client()
            downloaded_images = []

            if not s3_client:
                logger.error(
                    "Could not download any image from s3, because is missing some s3 configuration variable"
                )
            else:
                fotos_s3_dir = self._create_task_s3_download_dir()

                for image in self.s3_images:
                    bucket, image_path = image.replace("s3://", "").split("/", 1)
                    image_index = self.s3_images.index(image)
                    original_image_filename = image_path.rsplit("/")[-1]
                    destiny_image_filename = "{}/{}-{}".format(
                        fotos_s3_dir, image_index, original_image_filename
                    )

                    s3_object = get_s3_object_metadata(
                        bucket=bucket, key=image_path, s3_client=s3_client
                    )
                    total_size = s3_object["ContentLength"] if s3_object else 0
                    download_s3_file(
                        image_path,
                        destiny_image_filename,
                        s3_client,
                        bucket,
                        Callback=DownloadProgressCallback(
                            self, downloaded_images, total_size
                        ),
                    )
                    downloaded_images.append(destiny_image_filename)

            self.images_count = len(downloaded_images)
            self.pending_action = (
                pending_actions.RESIZE
                if self.pending_action == pending_actions.IMPORT_FROM_S3_WITH_RESIZE
                else None
            )
            self.update_size()
            self.save()
        except Exception as e:
            self.set_failure(str(e))
            raise NodeServerError(e)

    def process(self):
        """
        This method contains the logic for processing tasks asynchronously
        from a background thread or from a worker. Here tasks that are
        ready to be processed execute some logic. This could be communication
        with a processing node or executing a pending action.
        """

        try:
            if self.upload_in_progress:
                return

            if self.pending_action == pending_actions.UPLOAD_TO_S3:
                self.upload_and_cache_assets()
                self.pending_action = None
                self.save()
                return

            if self.pending_action in [
                pending_actions.IMPORT_FROM_S3,
                pending_actions.IMPORT_FROM_S3_WITH_RESIZE,
            ]:
                self.handle_s3_import()

            if self.pending_action == pending_actions.IMPORT:
                self.handle_import()

            if self.pending_action == pending_actions.RESIZE:
                resized_images = self.resize_images()
                self.refresh_from_db()
                self.resize_gcp(resized_images)
                self.pending_action = None
                self.save()

            if self.auto_processing_node and not self.status in [
                status_codes.FAILED,
                status_codes.CANCELED,
            ]:
                # No processing node assigned and need to auto assign
                if self.processing_node is None:
                    # Assign first online node with lowest queue count
                    self.processing_node = ProcessingNode.find_best_available_node()
                    if self.processing_node:
                        self.processing_node.queue_count += 1  # Doesn't have to be accurate, it will get overridden later
                        self.processing_node.save()

                        logger.info(
                            "Automatically assigned processing node {} to {}".format(
                                self.processing_node, self
                            )
                        )
                        self.save()

                # Processing node assigned, but is offline and no errors
                if self.processing_node and not self.processing_node.is_online():
                    # If we are queued up
                    # detach processing node, and reassignment
                    # will be processed at the next tick
                    if self.status == status_codes.QUEUED:
                        logger.info(
                            "Processing node {} went offline, reassigning {}...".format(
                                self.processing_node, self
                            )
                        )
                        self.uuid = ""
                        self.processing_node = None
                        self.status = None
                        self.save()

                    elif self.status == status_codes.RUNNING:
                        # Task was running and processing node went offline
                        # It could have crashed due to low memory
                        # or perhaps it went offline due to network errors.
                        # We can't easily differentiate between the two, so we need
                        # to notify the user because if it crashed due to low memory
                        # the user might need to take action (or be stuck in an infinite loop)
                        raise NodeServerError(
                            "Processing node went offline. This could be due to insufficient memory or a network error."
                        )

            if self.processing_node:
                # Need to process some images (UUID not yet set and task doesn't have pending actions)?
                if (
                    not self.uuid
                    and self.pending_action is None
                    and self.status is None
                ):
                    logger.info("Processing... {}".format(self))

                    images_path = self.task_path()
                    images = [os.path.join(images_path, i) for i in self.scan_images()]

                    # Track upload progress, but limit the number of DB updates
                    # to every 2 seconds (and always record the 100% progress)
                    last_update = 0

                    def callback(progress):
                        nonlocal last_update

                        time_has_elapsed = time.time() - last_update >= 2
                        if time_has_elapsed:
                            testWatch.manual_log_call("Task.process.callback")
                            self.check_if_canceled()
                            Task.objects.filter(pk=self.id).update(
                                upload_progress=float(progress) / 100.0
                            )
                            last_update = time.time()

                    # This takes a while
                    try:
                        uuid = self.processing_node.process_new_task(
                            images, self.name, self.options, callback
                        )
                    except NodeConnectionError as e:
                        # If we can't create a task because the node is offline
                        # We want to fail instead of trying again
                        raise NodeServerError(
                            gettext("Connection error: %(error)s") % {"error": str(e)}
                        )

                    # Refresh task object before committing change
                    self.refresh_from_db()
                    self.upload_progress = 1.0
                    self.uuid = uuid
                    self.save()

                    # TODO: log process has started processing

            if self.pending_action is not None:
                if self.pending_action == pending_actions.CANCEL:
                    # Do we need to cancel the task on the processing node?
                    logger.info("Canceling {}".format(self))
                    if self.processing_node and self.uuid:
                        # Attempt to cancel the task on the processing node
                        # We don't care if this fails (we tried)
                        try:
                            self.processing_node.cancel_task(self.uuid)
                        except OdmError:
                            logger.warning(
                                "Could not cancel {} on processing node. We'll proceed anyway...".format(
                                    self
                                )
                            )

                        self.status = status_codes.CANCELED
                        self.pending_action = None
                        self.save()
                    else:
                        # Tasks with no processing node or UUID need no special action
                        self.status = status_codes.CANCELED
                        self.pending_action = None
                        self.save()

                elif self.pending_action == pending_actions.RESTART:
                    logger.info("Restarting {}".format(self))

                    if self.processing_node:
                        if len(self.s3_images) > 0:
                            self.pending_action = pending_actions.IMPORT_FROM_S3
                            self.save()
                            self.handle_s3_import()

                        self._try_download_root_images_from_s3()

                        # Check if the UUID is still valid, as processing nodes purge
                        # results after a set amount of time, the UUID might have been eliminated.
                        uuid_still_exists = False

                        if self.uuid:
                            try:
                                info = self.processing_node.get_task_info(self.uuid)
                                uuid_still_exists = info.uuid == self.uuid
                            except OdmError:
                                pass

                        need_to_reprocess = False

                        if uuid_still_exists:
                            # Good to go

                            try:
                                self.processing_node.restart_task(
                                    self.uuid, self.options
                                )
                            except (NodeServerError, NodeResponseError) as e:
                                # Something went wrong
                                logger.warning(
                                    "Could not restart {}, will start a new one".format(
                                        self
                                    )
                                )
                                need_to_reprocess = True
                        else:
                            need_to_reprocess = True

                        if need_to_reprocess:
                            logger.info("{} needs to be reprocessed".format(self))

                            # Task has been purged (or processing node is offline)
                            # Process this as a new task
                            # Removing its UUID will cause the scheduler
                            # to process this the next tick
                            self.uuid = ""

                            # We also remove the "rerun-from" parameter if it's set
                            self.options = list(
                                filter(
                                    lambda d: d["name"] != "rerun-from", self.options
                                )
                            )
                            self.upload_progress = 0

                        self.console.reset()
                        self.processing_time = -1
                        self.status = None
                        self.last_error = None
                        self.pending_action = None
                        self.running_progress = 0
                        self.save()
                    else:
                        raise NodeServerError(
                            gettext("Cannot restart a task that has no processing node")
                        )

                elif self.pending_action == pending_actions.REMOVE:
                    logger.info("Removing {}".format(self))
                    if self.processing_node and self.uuid:
                        # Attempt to delete the resources on the processing node
                        # We don't care if this fails, as resources on processing nodes
                        # Are expected to be purged on their own after a set amount of time anyway
                        try:
                            self.processing_node.remove_task(self.uuid)
                        except OdmError:
                            pass

                    # What's more important is that we delete our task properly here
                    self.delete()

                    # Stop right here!
                    return

            if self.processing_node:
                # Need to update status (first time, queued or running?)
                if self.uuid and self.status in [
                    None,
                    status_codes.QUEUED,
                    status_codes.RUNNING,
                ]:
                    # Update task info from processing node
                    if not self.console.output():
                        current_lines_count = 0
                    else:
                        current_lines_count = len(self.console.output().split("\n"))

                    # Not exists task on pyodm node, maybe the node crashed, so restart
                    if not self.processing_node.task_exists(self.uuid):
                        self.pending_action = pending_actions.RESTART
                        self.last_error = None
                        self.save()
                        return

                    info = self.processing_node.get_task_info(
                        self.uuid, current_lines_count
                    )

                    self.processing_time = info.processing_time
                    self.status = info.status.value

                    if len(info.output) > 0:
                        self.console += "\n".join(info.output) + "\n"

                    # Update running progress
                    self.running_progress = (
                        info.progress / 100.0
                    ) * self.TASK_PROGRESS_LAST_VALUE

                    if info.last_error != "":
                        self.last_error = info.last_error

                    # Has the task just been canceled, failed, or completed?
                    if self.status in [
                        status_codes.FAILED,
                        status_codes.COMPLETED,
                        status_codes.CANCELED,
                    ]:
                        logger.info(
                            "Processing status: {} for {}".format(self.status, self)
                        )

                        if self.status == status_codes.COMPLETED:
                            assets_dir = self.assets_path("")

                            # Remove previous assets directory
                            # if os.path.exists(assets_dir):
                            #    logger.info("Removing old assets directory: {} for {}".format(assets_dir, self))
                            # shutil.rmtree(assets_dir)

                            os.makedirs(assets_dir, exist_ok=True)

                            # Download and try to extract results up to 4 times
                            # (~5% of the times, on large downloads, the archive could be corrupted)
                            retry_num = 0
                            extracted = False
                            last_update = 0

                            def callback(progress):
                                nonlocal last_update

                                time_has_elapsed = time.time() - last_update >= 2

                                if time_has_elapsed or int(progress) == 100:
                                    Task.objects.filter(pk=self.id).update(
                                        running_progress=(
                                            self.TASK_PROGRESS_LAST_VALUE
                                            + (float(progress) / 100.0) * 0.1
                                        )
                                    )
                                    last_update = time.time()

                            while not extracted:
                                last_update = 0
                                logger.info("Downloading all.zip for {}".format(self))

                                # Download all assets
                                zip_path = self.processing_node.download_task_assets(
                                    self.uuid,
                                    assets_dir,
                                    progress_callback=callback,
                                    parallel_downloads=max(1, int(16 / (2**retry_num))),
                                )

                                # Rename to all.zip
                                all_zip_path = self.assets_path("all.zip")
                                os.rename(zip_path, all_zip_path)

                                logger.info("Extracting all.zip for {}".format(self))

                                try:
                                    self.extract_assets_and_complete()
                                    extracted = True
                                except zipfile.BadZipFile:
                                    if retry_num < 5:
                                        logger.warning(
                                            "{} seems corrupted. Retrying...".format(
                                                all_zip_path
                                            )
                                        )
                                        retry_num += 1
                                        os.remove(all_zip_path)
                                    else:
                                        raise NodeServerError(
                                            gettext("Invalid zip file")
                                        )
                        else:
                            # FAILED, CANCELED
                            self.save()

                            if self.status == status_codes.FAILED:
                                from app.plugins import signals as plugin_signals

                                plugin_signals.task_failed.send_robust(
                                    sender=self.__class__, task_id=self.id
                                )

                    else:
                        # Still waiting...
                        self.save()

        except (NodeServerError, NodeResponseError) as e:
            self.set_failure(str(e))
        except NodeConnectionError as e:
            logger.warning(
                "{} connection/timeout error: {}. We'll try reprocessing at the next tick.".format(
                    self, str(e)
                )
            )
        except TaskInterruptedException as e:
            # Task was interrupted during image resize / upload
            logger.warning("{} interrupted".format(self, str(e)))

    def extract_assets_and_complete(self):
        """
        Extracts assets/all.zip, populates task fields where required and assure COGs
        It will raise a zipfile.BadZipFile exception is the archive is corrupted.
        :return:
        """
        assets_dir = self.assets_path("")
        zip_path = self.assets_path("all.zip")

        # Extract from zip
        with zipfile.ZipFile(zip_path, "r") as zip_h:
            zip_h.extractall(assets_dir)

        logger.info("Extracted all.zip for {}".format(self))

        os.remove(zip_path)

        # Check if this looks like a backup file, in which case we need to move the files
        # a directory level higher
        is_backup = os.path.isfile(
            self.assets_path("data", "backup.json")
        ) and os.path.isdir(self.assets_path("assets"))
        if is_backup:
            logger.info("Restoring from backup")
            try:
                tmp_dir = os.path.join(
                    settings.FILE_UPLOAD_TEMP_DIR, f"{self.id}.backup"
                )

                shutil.move(assets_dir, tmp_dir)
                shutil.rmtree(self.task_path(""))
                shutil.move(tmp_dir, self.task_path(""))
            except shutil.Error as e:
                logger.warning("Cannot restore from backup: %s" % str(e))
                raise NodeServerError("Cannot restore from backup")

        # Populate *_extent fields
        extent_fields = [
            (
                os.path.realpath(
                    self.assets_path("odm_orthophoto", "odm_orthophoto.tif")
                ),
                "orthophoto_extent",
            ),
            (os.path.realpath(self.assets_path("odm_dem", "dsm.tif")), "dsm_extent"),
            (os.path.realpath(self.assets_path("odm_dem", "dtm.tif")), "dtm_extent"),
        ]

        for raster_path, field in extent_fields:
            if os.path.exists(raster_path):
                # Make sure this is a Cloud Optimized GeoTIFF
                # if not, it will be created
                try:
                    assure_cogeo(raster_path)
                except IOError as e:
                    logger.warning(
                        "Cannot create Cloud Optimized GeoTIFF for %s (%s). This will result in degraded visualization performance."
                        % (raster_path, str(e))
                    )

                # Read extent and SRID
                raster = GDALRaster(raster_path)
                extent = OGRGeometry.from_bbox(raster.extent)

                # Make sure PostGIS supports it
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT SRID FROM spatial_ref_sys WHERE SRID = %s",
                        [raster.srid],
                    )
                    if cursor.rowcount == 0:
                        raise NodeServerError(
                            gettext(
                                "Unsupported SRS %(code)s. Please make sure you picked a supported SRS."
                            )
                            % {"code": str(raster.srid)}
                        )

                # It will be implicitly transformed into the SRID of the model’s field
                # self.field = GEOSGeometry(...)
                setattr(self, field, GEOSGeometry(extent.wkt, srid=raster.srid))

                logger.info(
                    "Populated extent field with {} for {}".format(raster_path, self)
                )

        self.refresh_from_db()
        self.update_available_assets_field()
        self.update_epsg_field()
        self.update_orthophoto_bands_field()
        self.update_size()
        self.potree_scene = {}
        self.running_progress = 1.0
        self.status = status_codes.COMPLETED
        self.pending_action = pending_actions.UPLOAD_TO_S3
        self._remove_root_images()

        if is_backup:
            self.read_backup_file()
        else:
            self.console += gettext("Done!") + "\n"

        self.save()

        from app.plugins import signals as plugin_signals

        plugin_signals.task_completed.send_robust(
            sender=self.__class__, task_id=self.id
        )

    def get_tile_path(self, tile_type, z, x, y):
        return self.assets_path("{}_tiles".format(tile_type), z, x, "{}.png".format(y))

    def get_tile_base_url(self, tile_type):
        # plant is just a special case of orthophoto
        if tile_type == "plant":
            tile_type = "orthophoto"

        return "/api/projects/{}/tasks/{}/{}/".format(
            self.project.id, self.id, tile_type
        )

    def get_map_items(self):
        types = []
        if "orthophoto.tif" in self.available_assets:
            types.append("orthophoto")
            types.append("plant")
        if "dsm.tif" in self.available_assets:
            types.append("dsm")
        if "dtm.tif" in self.available_assets:
            types.append("dtm")

        camera_shots = ""
        if "shots.geojson" in self.available_assets:
            camera_shots = "/api/projects/{}/tasks/{}/download/shots.geojson".format(
                self.project.id, self.id
            )

        ground_control_points = ""
        if "ground_control_points.geojson" in self.available_assets:
            ground_control_points = "/api/projects/{}/tasks/{}/download/ground_control_points.geojson".format(
                self.project.id, self.id
            )

        return {
            "tiles": [{"url": self.get_tile_base_url(t), "type": t} for t in types],
            "meta": {
                "task": {
                    "id": str(self.id),
                    "name": self.name,
                    "project": self.project.id,
                    "public": self.public,
                    "camera_shots": camera_shots,
                    "ground_control_points": ground_control_points,
                    "epsg": self.epsg,
                    "orthophoto_bands": self.orthophoto_bands,
                }
            },
        }

    def get_model_display_params(self):
        """
        Subset of a task fields used in the 3D model display view
        """
        return {
            "id": str(self.id),
            "project": self.project.id,
            "available_assets": self.available_assets,
            "public": self.public,
            "epsg": self.epsg,
        }

    def generate_deferred_asset(self, archive, directory, stream=False):
        """
        :param archive: path of the destination .zip file (relative to /assets/ directory)
        :param directory: path of the source directory to compress (relative to /assets/ directory)
        :param stream: return a stream instead of a path to the file
        :return: full path of the generated archive
        """
        archive_path = self.assets_path(archive)
        directory_path = self.assets_path(directory)

        if not os.path.exists(directory_path):
            raise FileNotFoundError("{} does not exist".format(directory_path))

        if not os.path.exists(archive_path):
            shutil.make_archive(
                os.path.splitext(archive_path)[0], "zip", directory_path
            )

        return archive_path

    def update_available_assets_field(self, commit=False):
        """
        Updates the available_assets field with the actual types of assets available
        :param commit: when True also saves the model, otherwise the user should manually call save()
        """
        all_assets = list(self.ASSETS_MAP.keys())
        logger.info("All assets: {}".format(all_assets))

        # Obter os assets disponíveis atualmente
        current_available_assets = set(self.available_assets)
        logger.info(
            "Current available assets for {}: {}".format(self, current_available_assets)
        )

        # Verificar e adicionar novos assets disponíveis
        new_available_assets = [
            asset for asset in all_assets if self.is_asset_available_slow(asset)
        ]
        logger.info(
            "New available assets for {}: {}".format(self, new_available_assets)
        )

        # Atualizar a lista de available_assets com novos assets
        updated_available_assets = current_available_assets.union(new_available_assets)
        self.available_assets = list(updated_available_assets)

        logger.info(
            "Updated available assets for {}: {}".format(self, self.available_assets)
        )

        if commit:
            self.save()

    def update_epsg_field(self, commit=False):
        """
        Updates the epsg field with the correct value
        :param commit: when True also saves the model, otherwise the user should manually call save()
        """
        epsg = None
        for asset in ["orthophoto.tif", "dsm.tif", "dtm.tif"]:
            asset_path = self.assets_path(self.ASSETS_MAP[asset])
            if os.path.isfile(asset_path):
                try:
                    with rasterio.open(asset_path) as f:
                        if f.crs is not None:
                            epsg = f.crs.to_epsg()
                            break  # We assume all assets are in the same CRS
                except Exception as e:
                    logger.warning(e)

        # If point cloud is not georeferenced, dataset is not georeferenced
        # (2D assets might be using pseudo-georeferencing)
        point_cloud = self.assets_path(self.ASSETS_MAP["georeferenced_model.laz"])
        if epsg is not None and os.path.isfile(point_cloud):
            if not is_pointcloud_georeferenced(point_cloud):
                logger.info("{} is not georeferenced".format(self))
                epsg = None

        self.epsg = epsg
        if commit:
            self.save()

    def update_orthophoto_bands_field(self, commit=False):
        """
        Updates the orthophoto bands field with the correct value
        :param commit: when True also saves the model, otherwise the user should manually call save()
        """
        bands = []
        orthophoto_path = self.assets_path(self.ASSETS_MAP["orthophoto.tif"])

        if os.path.isfile(orthophoto_path):
            with rasterio.open(orthophoto_path) as f:
                names = [c.name for c in f.colorinterp]
                for i, n in enumerate(names):
                    bands.append({"name": n, "description": f.descriptions[i]})

        self.orthophoto_bands = bands
        if commit:
            self.save()

    def delete(self, using=None, keep_parents=False):
        task_id = self.id
        from app.plugins import signals as plugin_signals

        plugin_signals.task_removing.send_robust(sender=self.__class__, task_id=task_id)

        directory_to_delete = os.path.join(
            settings.MEDIA_ROOT, task_directory_path(self.id, self.project.id)
        )

        super(Task, self).delete(using, keep_parents)

        # Remove files related to this task
        try:
            shutil.rmtree(directory_to_delete)
        except FileNotFoundError as e:
            logger.warning(e)

        self.project.owner.profile.clear_used_quota_cache()

        plugin_signals.task_removed.send_robust(sender=self.__class__, task_id=task_id)

    def set_failure(self, error_message):
        logger.error("FAILURE FOR {}: {}".format(self, error_message))
        self.last_error = error_message
        self.status = status_codes.FAILED
        self.pending_action = None
        self.save()

    def find_all_files_matching(self, regex):
        directory = full_task_directory_path(self.id, self.project.id)
        return [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if re.match(regex, f, re.IGNORECASE)
        ]

    def check_if_canceled(self):
        # Check if task has been canceled/removed
        if Task.objects.only("pending_action").get(pk=self.id).pending_action in [
            pending_actions.CANCEL,
            pending_actions.REMOVE,
        ]:
            raise TaskInterruptedException()

    def resize_images(self):
        """
        Destructively resize this task's JPG images while retaining EXIF tags.
        Resulting images are always converted to JPG.
        TODO: add support for tiff files
        :return list containing paths of resized images and resize ratios
        """
        if self.resize_to < 0:
            logger.warning(
                "We were asked to resize images to {}, this might be an error.".format(
                    self.resize_to
                )
            )
            return []
        # Add a signal to notify that we are resizing images
        from app.plugins import signals as plugin_signals

        plugin_signals.task_resizing_images.send_robust(
            sender=self.__class__, task_id=self.id
        )

        images_path = self.find_all_files_matching(r".*\.(jpe?g|tiff?)$")
        total_images = len(images_path)
        resized_images_count = 0
        last_update = 0

        def callback(retval=None):
            nonlocal last_update
            nonlocal resized_images_count
            nonlocal total_images

            resized_images_count += 1
            if time.time() - last_update >= 2:
                # Update progress
                Task.objects.filter(pk=self.id).update(
                    resize_progress=(float(resized_images_count) / float(total_images))
                )
                self.check_if_canceled()
                last_update = time.time()

        resized_images = list(
            map(
                partial(resize_image, resize_to=self.resize_to, done=callback),
                images_path,
            )
        )

        Task.objects.filter(pk=self.id).update(resize_progress=1.0)

        return resized_images

    def resize_gcp(self, resized_images):
        """
        Destructively change this task's GCP file (if any)
        by resizing the location of GCP entries.
        :param resized_images: list of objects having "path" and "resize_ratio" keys
            for example [{'path': 'path/to/DJI_0018.jpg', 'resize_ratio': 0.25}, ...]
        :return: path to changed GCP file or None if no GCP file was found/changed
        """
        gcp_path = self.find_all_files_matching(r".*\.txt$")

        # Skip geo.txt, image_groups.txt, align.(las|laz|tif) files
        gcp_path = list(
            filter(
                lambda p: os.path.basename(p).lower()
                not in [
                    "geo.txt",
                    "image_groups.txt",
                    "align.las",
                    "align.laz",
                    "align.tif",
                ],
                gcp_path,
            )
        )
        if len(gcp_path) == 0:
            return None

        # Assume we only have a single GCP file per task
        gcp_path = gcp_path[0]

        image_ratios = {}
        for ri in resized_images:
            image_ratios[os.path.basename(ri["path"]).lower()] = ri["resize_ratio"]

        try:
            gcpFile = GCPFile(gcp_path)
            gcpFile.create_resized_copy(gcp_path, image_ratios)
            logger.info("Resized GCP file {}".format(gcp_path))
            return gcp_path
        except Exception as e:
            logger.warning("Could not resize GCP file {}: {}".format(gcp_path, str(e)))

    def create_task_directories(self):
        """
        Create directories for this task (if they don't exist already)
        """
        assets_dir = self.assets_path("")
        try:
            os.makedirs(assets_dir)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(assets_dir):
                pass
            else:
                raise

    def scan_s3_assets(self):
        s3_key = convert_task_path_to_s3(self.task_path())
        return [obj["Key"] for obj in list_s3_objects(s3_key)]

    def scan_images(self):
        return [e.name for e in self._entry_root_images()]

    def get_image_path(self, filename):
        p = self.task_path(filename)
        return path_traversal_check(p, self.task_path())

    def handle_images_upload(self, files: list[dict[str, str]]):
        uploaded = {}
        for file in files:
            if file is None:
                continue

            name = file["name"]

            tp = self.task_path()
            ensure_path_exists(tp)

            dst_path = self.get_image_path(name)
            shutil.copyfile(file["path"], dst_path)

            uploaded[name] = {
                "size": os.path.getsize(dst_path),
                "origin_path": file["path"],
                "destiny_path": dst_path,
            }

        return uploaded

    def update_size(self, commit=False, clear_quota=True):
        try:
            total_bytes = 0
            for dirpath, _, filenames in os.walk(self.task_path()):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    if not os.path.islink(fp):
                        total_bytes += os.path.getsize(fp)
            self.size = total_bytes / 1024 / 1024
            if commit:
                self.save()

            if clear_quota:
                self.project.owner.profile.clear_used_quota_cache()
        except Exception as e:
            logger.warn("Cannot update size for task {}: {}".format(self, str(e)))

    def _create_task_s3_download_dir(self):
        fotos_s3_dir = self.task_path()
        ensure_path_exists(fotos_s3_dir)
        return fotos_s3_dir

    def upload_and_cache_assets(self):
        files_uploadeds = self._upload_assets_to_s3()
        for file in files_uploadeds:
            worker_cache_files_tasks.download_and_add_to_cache.delay(file, False)

    def append_s3_assets(self, assets: list[str]):
        for asset in assets:
            self.append_s3_asset(asset)

    def append_s3_asset(self, asset: str):
        if not isinstance(self.s3_assets, list):
            self.s3_assets = []

        if asset not in self.s3_assets:
            self.s3_assets.append(asset)

    def _upload_assets_to_s3(self):
        s3_bucket = settings.S3_BUCKET
        files_to_upload = self._all_assets_needs_upload_to_s3()
        files_uploadeds = []
        self.uploading_s3_progress = 0.0
        self.save()

        if not s3_bucket:
            logger.error(
                "Could not upload any image to s3, because is missing some s3 configuration variable"
            )
            return

        class UploadProgressCallback(object):
            def __init__(self, task: Task, uploaded_images, total_size, total_images):
                self._size = total_size
                self._uploaded_bytes = 0.0
                self._task = task
                self._uploaded_images = uploaded_images
                self._total_images = total_images

            def __call__(self, bytes_transferred):
                self._uploaded_bytes += bytes_transferred
                progress = self._uploaded_bytes / self._size if self._size > 0 else 0
                self._task._update_upload_progress(
                    self._uploaded_images, self._total_images, progress
                )
                logger.info(
                    "Upload to S3 percent {}%".format(
                        self._task.uploading_s3_progress * 100
                    )
                )

        try:
            logger.info("Starting upload assets to S3...")
            s3_client = get_s3_client()

            if not s3_client:
                logger.error(
                    "Could not upload any image to s3, because is missing some s3 configuration variable"
                )
                return

            for file_to_upload in files_to_upload:
                if not os.path.exists(file_to_upload):
                    continue

                s3_key = remove_path_from_path(file_to_upload, settings.MEDIA_ROOT)
                checksum = calculate_sha256(file_to_upload)

                if not checksum:
                    continue

                if not self._need_upload_asset(checksum, s3_key, s3_client):
                    logger.info(
                        f"Asset '{file_to_upload}' already saved on s3, no need reupload."
                    )
                    self._update_upload_progress(
                        files_uploadeds, len(files_to_upload), 1
                    )
                    logger.info(
                        "Upload to S3 percent {}%".format(
                            self.uploading_s3_progress * 100
                        )
                    )
                else:
                    logger.info(
                        f"Asset '{file_to_upload}' not saved on s3, uploading..."
                    )
                    try:
                        file_size = os.path.getsize(file_to_upload)
                    except:
                        continue

                    s3_client.upload_file(
                        file_to_upload,
                        s3_bucket,
                        s3_key,
                        Callback=UploadProgressCallback(
                            self, files_uploadeds, file_size, len(files_to_upload)
                        ),
                        ExtraArgs={
                            "Metadata": {
                                "Checksumsha256": checksum,
                            }
                        },
                    )

                files_uploadeds.append(append_s3_bucket_prefix(file_to_upload))

            logger.info(f"Uploaded {len(files_uploadeds)} files to S3!")
        except Exception as e:
            raise NodeServerError(e)

        return files_uploadeds

    def _get_all_assets_files(self):
        task_path = self.task_path()

        return get_all_files_in_dir(task_path)

    def _remove_assets(self):
        task_path = self.task_path()

        if task_path[-1] == "/":
            task_path = task_path[0:-1]

        shutil.rmtree(task_path)

    def _remove_root_images(self):
        for e in self._entry_root_images():
            os.remove(e.path)

    def _entry_root_images(self):
        tp = self.task_path()

        if not os.path.exists(tp):
            return []

        try:
            return [e for e in os.scandir(tp) if e.is_file()]
        except:
            return []

    def _update_download_progress(self, downloaded_images, progress):
        self.downloading_s3_progress = self._calculate_progress_of_images(
            downloaded_images, len(self.s3_images), progress
        )
        self.save()

    def _update_upload_progress(self, uploaded_images, total_images, progress):
        self.uploading_s3_progress = self._calculate_progress_of_images(
            uploaded_images, total_images, progress
        )
        self.save()

    def _calculate_progress_of_images(self, succeded_images, total_images, progress):
        percent_per_image = 1.0 / total_images
        percent_success = len(succeded_images) * percent_per_image
        percent_current_progress = progress * percent_per_image
        return percent_success + percent_current_progress

    def _download_all_s3_images(self):
        task_path = self.task_path()
        logger.info('will download images with "{}"'.format(task_path))
        s3_images = list_s3_objects(task_path)
        s3_client = get_s3_client()

        if not s3_client:
            logger.error(
                "Could not download any image from s3, because is missing some s3 configuration variable"
            )
            return

        logger.info(
            'will download images: "{}"'.format(
                str([image["Key"] for image in s3_images])
            )
        )

        for image in s3_images:
            image_key = image["Key"]
            logger.info('start download image: "{}"'.format(image_key))

            download_s3_file(image_key, image_key, s3_client)
            logger.info('downloaded image: "{}"'.format(image_key))

    def _all_assets_needs_upload_to_s3(self):
        root_assets = [e.path for e in self._entry_root_images()]
        assets_dont_upload = root_assets + (
            self.s3_assets if isinstance(self.s3_assets, list) else []
        )
        return [
            f
            for f in self._get_all_assets_files()
            if not (f in assets_dont_upload) and os.path.exists(f)
        ]

    def _list_s3_root_images(self):
        s3_key = convert_task_path_to_s3(self.task_path())
        root_images = []

        for s3_obj in list_s3_objects(s3_key):
            obj_key = s3_obj["Key"]
            obj_filename = get_file_name(obj_key)
            obj_root_key = ensure_sep_at_end(s3_key) + obj_filename

            if obj_root_key == obj_key:
                root_images.append(obj_key)

        return root_images

    def _try_download_root_images_from_s3(self):
        s3_images = self._list_s3_root_images()

        if len(s3_images) == 0:
            return

        task_path = ensure_sep_at_end(self.task_path())
        ensure_path_exists(task_path)

        s3_client = get_s3_client()
        for key in s3_images:
            image_name = get_file_name(key)
            dst_path = task_path + image_name

            try:
                download_s3_file(key, dst_path, s3_client)
            except Exception as e:
                logger.error(
                    f"Error on download root files from s3. Original error: {str(e)}"
                )

    def _need_upload_asset(self, current_checksum: str, s3_key: str, s3_client):
        object_checksum = get_object_checksum(s3_key, s3_client=s3_client)

        return current_checksum != object_checksum
