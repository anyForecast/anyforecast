import logging

from celery import Celery

from anyforecast.settings import conf
from . import base

log = logging.getLogger(__name__)

celery_settings = conf.get_celery_settings()
celery_app_name = celery_settings.get("celery", "celery-executor")

app = Celery(celery_app_name, config_source=celery_settings)


@app.task(name="run_celery")
def run_task(task: Task):
    """Runs given task.
    """
    celery_task_id = app.current_task.request.id
    log.info(f"[{celery_task_id}] Executing task in Celery: {task.name}")
    task.run()


class CeleryExecutor(base.Executor):

    def start(self):
        log.debug("Starting Celery Executor.")

    def submit(self, task):
        run_task.apply_async(args=[task])

    def shutdown(self):
        pass
