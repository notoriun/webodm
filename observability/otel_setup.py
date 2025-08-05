import logging

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.django import DjangoInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.celery import CeleryInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry._logs import set_logger_provider as public_set_logger_provider
from django.conf import settings


def setup_otel():
    if not _need_add_otel():
        return

    logger = logging.getLogger("app.logger")
    logger.info("Starting Open Telemetry observers...")

    _setup_trace_provider("webapp")
    _add_django_base_instrumentors()
    _setup_logs_provider("webapp")


def setup_otel_celery():
    if not _need_add_otel():
        return

    logger = logging.getLogger("app.logger")
    logger.info("Starting Open Telemetry celery observers...")

    _setup_trace_provider("worker")
    CeleryInstrumentor().instrument()
    _add_django_base_instrumentors()
    _setup_logs_provider("worker")


def _setup_logs_provider(service_name: str):
    resource = Resource.create(
        attributes={
            "service.name": service_name,
        }
    )

    provider = LoggerProvider(resource=resource)
    processor = BatchLogRecordProcessor(
        OTLPLogExporter(endpoint=f"{settings.OTEL_ENDPOINT}/v1/logs")
    )
    provider.add_log_record_processor(processor)

    public_set_logger_provider(provider)

    handler = SafeLoggingHandler(level=logging.INFO, logger_provider=provider)
    logger = logging.getLogger("app.logger")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)


def _add_django_base_instrumentors():
    DjangoInstrumentor().instrument()
    RequestsInstrumentor().instrument()
    Psycopg2Instrumentor().instrument()
    RedisInstrumentor().instrument()


def _setup_trace_provider(service_name: str):
    resource = Resource.create(
        attributes={
            "service.name": service_name,
        }
    )

    trace.set_tracer_provider(TracerProvider(resource=resource))
    tracer_provider = trace.get_tracer_provider()

    otlp_exporter = OTLPSpanExporter(
        # endpoint="http://otel-collector:4318/v1/traces", insecure=True
        endpoint=f"{settings.OTEL_ENDPOINT}/v1/traces"
    )
    span_processor = BatchSpanProcessor(otlp_exporter)
    tracer_provider.add_span_processor(span_processor)


def _need_add_otel():
    return settings.OTEL_ENABLED and not settings.MIGRATING and not settings.FLUSHING


class SafeLoggingHandler(LoggingHandler):
    def emit(self, record):
        safe_record = self._sanitize_record(record)
        try:
            super().emit(safe_record)
        except Exception as e:
            print(f"[OpenTelemetry Logging Error] {e}")
            print("Registro problemático (sanitizado):", safe_record.__dict__)

    def _sanitize_record(self, record):
        from logging import LogRecord

        sanitized = LogRecord(
            name=record.name,
            level=record.levelno,
            pathname=record.pathname,
            lineno=record.lineno,
            msg=record.msg,
            args=(),
            exc_info=None,
        )
        for attr in [
            "levelname",
            "filename",
            "funcName",
            "module",
            "process",
            "thread",
        ]:
            if hasattr(record, attr):
                setattr(sanitized, attr, getattr(record, attr))
        return sanitized
