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
from django.conf import settings


def setup_otel():
    if not settings.OTEL_ENABLED:
        return

    resource = Resource.create(
        attributes={
            "service.name": "webodm",
        }
    )

    trace.set_tracer_provider(TracerProvider(resource=resource))
    tracer_provider = trace.get_tracer_provider()

    otlp_exporter = OTLPSpanExporter(
        # endpoint="http://otel-collector:4318/v1/traces", insecure=True
        endpoint="http://collector:4318/v1/traces"
    )
    span_processor = BatchSpanProcessor(otlp_exporter)
    tracer_provider.add_span_processor(span_processor)

    DjangoInstrumentor().instrument()
    RequestsInstrumentor().instrument()
    Psycopg2Instrumentor().instrument()
    RedisInstrumentor().instrument()


def setup_otel_celery():
    CeleryInstrumentor().instrument()
