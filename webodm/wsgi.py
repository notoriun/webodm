"""
WSGI config for webodm project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.10/howto/deployment/wsgi/
"""

import os
from django.core.wsgi import get_wsgi_application
from observability.otel_setup import setup_otel

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webodm.settings")

setup_otel()

application = get_wsgi_application()

from webodm.wsgi_booted import booted
