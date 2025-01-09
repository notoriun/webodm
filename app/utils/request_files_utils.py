import shutil
import tempfile

from webodm import settings
from django.core.files.uploadedfile import InMemoryUploadedFile, UploadedFile

def save_request_file(request_file: UploadedFile, destiny_path: str = None):
    if not destiny_path:
        destiny_path = tempfile.mktemp('', dir=settings.MEDIA_TMP)

    with open(destiny_path, 'wb+') as fd:
        if isinstance(request_file, InMemoryUploadedFile):
            for chunk in request_file.chunks():
                fd.write(chunk)
        else:
            with open(request_file.temporary_file_path(), 'rb') as f:
                shutil.copyfileobj(f, fd)

    return { 'path': destiny_path, 'name': request_file.name } 
