import boto3
import logging
import rasterio
import re
from botocore.config import Config
from webodm import settings
from contextlib import contextmanager
from rasterio.errors import RasterioIOError
from rasterio.session import AWSSession
from rio_tiler.io import COGReader
from rest_framework import exceptions


logger = logging.getLogger('app.logger')

def get_s3_client():
    endpoint_url = settings.S3_DOWNLOAD_ENDPOINT
    access_key = settings.S3_DOWNLOAD_ACCESS_KEY
    secret_key = settings.S3_DOWNLOAD_SECRET_KEY

    if not endpoint_url or not access_key or not secret_key:
        return None

    return boto3.client('s3',
                        endpoint_url=endpoint_url,
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                        config=Config(signature_version='s3v4'))

def get_s3_object(key, bucket=settings.S3_BUCKET):
    try:
        if not bucket:
            logger.error('Could not get any object from s3, because is missing some s3 configuration variable')
            return None

        s3_client = get_s3_client()
        
        if not s3_client:
            logger.error('Could not get any object from s3, because is missing some s3 configuration variable')
            return None

        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        s3_object_exists = 'DeleteMarker' not in s3_object
        
        if s3_object_exists:
            return s3_object
    except Exception as e:
        logger.error(str(e))
    
    return None

def list_s3_objects(key_to_contains: str, bucket=settings.S3_BUCKET):
    try:
        if not bucket:
            logger.error('Could not list any object from s3, because is missing some s3 configuration variable')
            return []

        s3_client = get_s3_client()

        if not s3_client:
            logger.error('Could not list any object from s3, because is missing some s3 configuration variable')
            return []

        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key_to_contains)
        
        return response['Contents']
    except Exception as e:
        logger.error(str(e))
    
    return []

@contextmanager
def open_cog_reader(url):
    endpoint_url = sanitize_s3_endpoint(settings.S3_DOWNLOAD_ENDPOINT)
    access_key = settings.S3_DOWNLOAD_ACCESS_KEY
    secret_key = settings.S3_DOWNLOAD_SECRET_KEY

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
            with COGReader(url) as src:
                yield src
    except RasterioIOError:
        raise exceptions.NotFound(_("Unable to read the data from S3"))
    except Exception as e:
        print(e)
        raise e

def sanitize_s3_endpoint(s3_endpoint: str):
    without_last_slash = s3_endpoint[0:-1] if s3_endpoint[-1] == '/' else s3_endpoint
    return re.sub(r'(http|https)://', '', without_last_slash)

def download_s3_file(file_path, destiny_image_filename, s3_client=None, bucket=settings.S3_BUCKET, *args, **kwargs):
    if not bucket:
        logger.error('Could not download any file from s3, because is missing some s3 configuration variable')
        return

    if not s3_client:
        s3_client = get_s3_client()
    
        if not s3_client:
            logger.error('Could not download any file from s3, because is missing some s3 configuration variable')
            return

    key = remove_s3_bucket_prefix(file_path)
    logger.info('Downloading s3 file {} to {}'.format(key, destiny_image_filename))
    s3_client.download_file(bucket, key, destiny_image_filename, *args, **kwargs)

def append_s3_bucket_prefix(path: str):
    bucket = settings.S3_BUCKET

    if not bucket:
        logger.error('Could append s3 prefix to access any s3 object, because is missing some s3 configuration variable')
        return path

    return f's3://{bucket}/{path}'

def remove_s3_bucket_prefix(path: str):
    s3_prefix = 's3://'
    bucket = settings.S3_BUCKET + '/' if settings.S3_BUCKET else ''

    return path.replace(s3_prefix, '').replace(bucket, '')
