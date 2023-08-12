import logging

from celery import Celery

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


class CeleryExecutor(base.Executor):
    def start(self):
        log.debug("Starting Local Executor.")

    def check_async(self):
        if not hasattr(self, "_async"):
            raise ValueError()

    def execute(self, task, *args, **kwargs):
        self.async_ = run_task.delay(task, *args, **kwargs)

    def get_state(self) -> str:
        return self.async_.state

    def shutdown(self):
        pass
