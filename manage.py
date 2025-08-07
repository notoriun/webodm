#!/usr/bin/env python
import os
import sys
from observability.otel_setup import setup_otel

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webodm.settings")

    try:
        setup_otel()
    except Exception as e:
        print(f"Failed to setup OpenTelemetry. Original error: {e}")

    try:
        from django.core.management import execute_from_command_line
    except ImportError:
        # The above import may fail for some other reason. Ensure that the
        # issue is really that Django is missing to avoid masking other
        # exceptions on Python 2.
        try:
            import django
        except ImportError:
            raise ImportError(
                "Couldn't import Django. Are you sure it's installed and "
                "available on your PYTHONPATH environment variable? Did you "
                "forget to activate a virtual environment?"
            )
        raise
    from django.conf import settings

    if os.environ.get("RUN_MAIN") and settings.DEBUG:
        try:
            import debugpy

            debugpy.listen(("0.0.0.0", 5678))
        except:
            pass

    execute_from_command_line(sys.argv)
