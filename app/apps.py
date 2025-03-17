from django.apps import AppConfig


class MainConfig(AppConfig):
    name = "app"
    verbose_name = "Application"

    def ready(self):
        result = super().ready()

        self._execute_populate_cache_task()

        return result

    def _execute_populate_cache_task(self):
        from worker.cache_files import seek_and_populate_redis_cache

        seek_and_populate_redis_cache.delay()
