import boto3
import logging
import rasterio
import re
import os
import worker.cache_files as worker_cache_files_tasks
from botocore.config import Config
from botocore.client import BaseClient
from webodm import settings
from contextlib import contextmanager
from rasterio.errors import RasterioIOError
from rasterio.session import AWSSession
from rio_tiler.io import COGReader
from rest_framework import exceptions
from app.utils.file_utils import remove_path_from_path, ensure_sep_at_end, remove_sep_from_start


logger = logging.getLogger('app.logger')

def get_s3_client():
    endpoint_url = settings.S3_DOWNLOAD_ENDPOINT
    access_key = settings.S3_DOWNLOAD_ACCESS_KEY
    secret_key = settings.S3_DOWNLOAD_SECRET_KEY
    timeout = settings.S3_TIMEOUT

    if not endpoint_url or not access_key or not secret_key:
        return None

    return boto3.client('s3',
                        endpoint_url=endpoint_url,
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                        config=Config(
                            signature_version='s3v4',
                            connect_timeout=timeout,
                            read_timeout=timeout))


def get_s3_object(key: str, bucket=settings.S3_BUCKET, s3_client=None):
    try:
        if not bucket:
            logger.error('Could not get any object from s3, because is missing some s3 configuration variable')
            return None

        valid_s3_client = _get_valid_s3_client(s3_client)
        
        if not valid_s3_client:
            return None

        s3_object = valid_s3_client.get_object(Bucket=bucket, Key=key)
        s3_object_exists = 'DeleteMarker' not in s3_object
        
        if s3_object_exists:
            return s3_object
    except Exception as e:
        logger.error(str(e))
    
    return None


def list_s3_objects(key_to_contains: str, s3_client=None, bucket=settings.S3_BUCKET):
    try:
        if not bucket:
            logger.error('Could not list any object from s3, because is missing some s3 configuration variable')
            return []

        valid_s3_client = _get_valid_s3_client(s3_client)

        if not valid_s3_client:
            return None

        response = valid_s3_client.list_objects_v2(Bucket=bucket, Prefix=key_to_contains)
        
        return response['Contents']
    except Exception as e:
        logger.error(str(e))
    
    return []


@contextmanager
def open_cog_reader(url: str):
    endpoint_url = sanitize_s3_endpoint(settings.S3_DOWNLOAD_ENDPOINT) if settings.S3_DOWNLOAD_ENDPOINT else None
    access_key = settings.S3_DOWNLOAD_ACCESS_KEY
    secret_key = settings.S3_DOWNLOAD_SECRET_KEY

    has_s3_config = endpoint_url and access_key and secret_key
    is_s3_url = has_s3_prefix(url)

    local_path = os.path.join(settings.MEDIA_ROOT, remove_s3_bucket_prefix(url))
    if os.path.isfile(local_path):
        with COGReader(local_path) as source:
            yield source

        worker_cache_files_tasks.refresh_file_in_cache.delay(url)

        return

    if not is_s3_url:
        raise exceptions.NotFound(url)

    if not has_s3_config:
        logger.error('Could not connect to s3, because is missing some s3 configuration variable')
        raise exceptions.NotFound(_("Unable to read the data from S3"))

    try:
        # Set AWS credentials
        boto3_session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        # Create a rasterio AWSSession with the boto3 session and your MinIO endpoint
        aws_session = AWSSession(
            boto3_session,
            endpoint_url=endpoint_url,
            region_name='us-east-1',  # Adjust if needed
            profile_name=None
        )

        with rasterio.Env(
            session=aws_session,
            AWS_VIRTUAL_HOSTING=False,  # Important for MinIO
            AWS_S3_ENDPOINT=endpoint_url,
            SSL=False,
        ):
            with COGReader(url) as source:
                yield source

        worker_cache_files_tasks.download_and_add_to_cache.delay(url)
    except RasterioIOError:
        raise exceptions.NotFound(_("Unable to read the data from S3"))
    except Exception as e:
        logger.error(str(e))
        raise e


def sanitize_s3_endpoint(s3_endpoint: str):
    without_last_slash = s3_endpoint[0:-1] if s3_endpoint[-1] == '/' else s3_endpoint
    return re.sub(r'(http|https)://', '', without_last_slash)


def download_s3_file(file_path, destiny_image_filename, s3_client=None, bucket=settings.S3_BUCKET, *args, **kwargs):
    if not bucket:
        logger.error('Could not download any file from s3, because is missing some s3 configuration variable')
        return

    valid_s3_client = _get_valid_s3_client(s3_client)

    if not valid_s3_client:
        return None

    key = remove_s3_bucket_prefix(file_path, bucket)
    logger.info('Downloading s3 file {} to {}'.format(key, destiny_image_filename))
    valid_s3_client.download_file(bucket, key, destiny_image_filename, *args, **kwargs)


def append_s3_bucket_prefix(path: str):
    bucket = settings.S3_BUCKET

    if not bucket:
        logger.error('Could append s3 prefix to access any s3 object, because is missing some s3 configuration variable')
        return path

    s3_path = remove_sep_from_start(
        remove_path_from_path(path, ensure_sep_at_end(settings.MEDIA_ROOT)))

    return f's3://{bucket}/{s3_path}'


def remove_s3_bucket_prefix(path: str, bucket=settings.S3_BUCKET):
    s3_prefix = 's3://'
    bucket_with_lash = ensure_sep_at_end(bucket)

    return path.replace(s3_prefix, '').replace(bucket_with_lash, '')


def has_s3_prefix(path: str):
    s3_prefix = 's3://'

    return path.startswith(s3_prefix)


def get_s3_object_metadata(key: str, bucket=settings.S3_BUCKET, s3_client=None):
    valid_s3_client = _get_valid_s3_client(s3_client)

    if not valid_s3_client:
        return None

    return valid_s3_client.head_object(Bucket=bucket, Key=key)


def convert_task_path_to_s3(task_path: str):
    return task_path.replace(ensure_sep_at_end(settings.MEDIA_ROOT), '')


def _get_valid_s3_client(unsafe_s3_client):
    logger.info(f'unsafe s3 client: {unsafe_s3_client}')
    if isinstance(unsafe_s3_client, BaseClient) and unsafe_s3_client.meta.service_model.service_name == 's3':
        return unsafe_s3_client

    s3_client = get_s3_client()
    logger.info(f'generated new client: {s3_client}')

    if not s3_client:
        logger.error('Could not connect to s3, because is missing some s3 configuration variable')
        return None

    return s3_client
