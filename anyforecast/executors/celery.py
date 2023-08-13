import logging

from celery import Celery
from celery.result import AsyncResult as CeleryAsyncResult

from anyforecast.settings import conf

from . import base

log = logging.getLogger(__name__)

celery_settings = conf.get_celery_settings()
celery_app_name = getattr(celery_settings, "celery", "celery-executor")

celery_app = Celery(celery_app_name, config_source=celery_settings)


@celery_app.task(name="run_celery")
def run_task(task, *args, **kwargs):
    """Runs given task."""
    task(*args, **kwargs)


class CeleryFuture(base.Future):
    """Wrapper for Celery async result."""

    def __init__(self, celery_async_result: CeleryAsyncResult):
        self.celery_async_result = celery_async_result

    def get_state(self) -> str:
        return self.celery_async_result.state


class CeleryExecutor(base.Executor):
    def start(self):
        log.debug("Starting Local Executor.")

    def execute(self, task, *args, **kwargs):
        celery_async_result = run_task.delay(task, *args, **kwargs)
        return CeleryFuture(celery_async_result)

    def get_state(self) -> str:
        return self.async_.state

    def shutdown(self):
        pass
