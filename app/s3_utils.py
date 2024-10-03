import boto3
import logging
from botocore.config import Config
from webodm import settings


def get_s3_client():
    endpoint_url = settings.S3_DOWNLOAD_ENDPOINT
    access_key = settings.S3_DOWNLOAD_ACCESS_KEY
    secret_key = settings.S3_DOWNLOAD_SECRET_KEY

    return boto3.client('s3',
                        endpoint_url=endpoint_url,
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                        config=Config(signature_version='s3v4'))

def get_s3_object(key, bucket=settings.S3_BUCKET):
    try:
        s3_client = get_s3_client()
        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        s3_object_exists = 'DeleteMarker' not in s3_object
        
        if s3_object_exists:
            return s3_object
    except Exception as e:
        logger = logging.getLogger('app.logger')
        logger.error(str(e))
    
    return None