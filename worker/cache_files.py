import os

from celery.utils.log import get_task_logger

from .celery import app
from app.utils.file_utils import ensure_path_exists, get_file_name
from webodm import settings

logger = get_task_logger("app.logger")

@app.task()
def download_and_add_to_cache(file: str):
    from app.utils.s3_utils import remove_s3_bucket_prefix, download_s3_file

    try:
        s3_key = remove_s3_bucket_prefix(file)
        filename = get_file_name(s3_key)
        file_dir = os.path.join(settings.MEDIA_ROOT, s3_key.replace(filename, ''))

        logger.info(f'download with: {str([s3_key, filename, file_dir])}')
        ensure_path_exists(file_dir)

        filepath = os.path.join(file_dir, filename)

        download_s3_file(s3_key, filepath)
        logger.info(f'downloaded {filepath}')
    except Exception as e:
        logger.error(str(e))


@app.task()
def refresh_file_cache_keys():
    try:
        # logger.info('rodou aqui hein')
        print('foi')
    except:
        pass
