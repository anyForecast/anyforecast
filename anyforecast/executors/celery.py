from __future__ import annotations

from celery import Celery
from celery.result import AsyncResult as CeleryAsyncResult

from anyforecast.settings import conf

from . import base

celery_settings = conf.get_celery_settings()
celery_app_name = getattr(celery_settings, "celery", "celery-executor")

celery_app = Celery(celery_app_name, config_source=celery_settings)


@celery_app.task(name="run_celery")
def execute_task(executor: base.Executor):
    """Runs given task."""
    return executor.execute()


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


@base.BackendExecutorFactory.register("celery")
class CeleryExecutor(base.BackendExecutor):
    def __init__(self):
        super().__init__(future_cls=CeleryFuture)

    def execute(self, executor: base.Executor, **opts):
        celery_async_result = execute_task.apply_async(executor, **opts)
        return self.future_cls(celery_async_result)
