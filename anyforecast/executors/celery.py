import logging

from celery import Celery

from anyforecast.settings import conf
from . import base

log = logging.getLogger(__name__)

celery_settings = conf.get_celery_settings()
celery_app_name = getattr(celery_settings, "celery", "celery-executor")

app = Celery(celery_app_name, config_source=celery_settings)


@app.task(name="run_celery")
def run_task(task, *args, **kwargs):
    """Runs given task.
    """
    celery_task_id = app.current_task.request.id
    log.info(f"[{celery_task_id}] Executing task in Celery: {task.name}")
    task.run(*args, **kwargs)


class CeleryExecutor(base.Executor):

    def start(self):
        log.debug("Starting Local Executor.")

    def submit(self, task, *args, **kwargs):
        return run_task.delay(task, *args, **kwargs)

    def shutdown(self):
        pass
