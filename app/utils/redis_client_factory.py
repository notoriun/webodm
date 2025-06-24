import redis
from webodm import settings


def get_redis_client():
    if not settings.BROKER_CELERY_USE_SSL:
        return redis.Redis().from_url(settings.CELERY_BROKER_URL)

    return redis.StrictRedis.from_url(
        settings.CELERY_BROKER_URL,
        ssl_cert_reqs=settings.BROKER_CELERY_REQS,
        ssl_ca_certs=settings.BROKER_CELERY_CERT,
    )
