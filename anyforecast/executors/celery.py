from __future__ import annotations

from celery import Celery
from celery.result import AsyncResult as CeleryAsyncResult

from anyforecast.settings import conf

from . import base

celery_settings = conf.get_celery_settings()
celery_app_name = getattr(celery_settings, "celery", "celery-executor")

celery_app = Celery(celery_app_name, config_source=celery_settings)


@celery_app.task(name="run_celery")
def run_task(runner):
    """Runs given task."""
    runner.run()


class CeleryFuture(base.Future):
    """Wrapper for Celery async result."""

    def __init__(self, celery_async_result: CeleryAsyncResult):
        self.celery_async_result = celery_async_result

    def get_id(self) -> str:
        return self.celery_async_result.id

    def get_state(self) -> str:
        return self.celery_async_result.state

    @classmethod
    def from_id(cls, id: str) -> CeleryFuture:
        return cls(CeleryAsyncResult(id=id))


class CeleryExecutor(base.ExecutorBackend):
    def execute(self, runner: base.Runner, **opts):
        celery_async_result = run_task.apply_async(runner, **opts)
        return CeleryFuture(celery_async_result)
