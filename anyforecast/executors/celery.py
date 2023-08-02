import logging

from . import _base
from tasks.celeryapp import app
from tasks.tasks import Task

log = logging.getLogger(__name__)


@app.task(name="run_celery")
def run_task(task: Task):
    """Runs given task.
    """
    celery_task_id = app.current_task.request.id
    log.info(f"[{celery_task_id}] Executing task in Celery: {task.name}")
    task.run()


class CeleryExecutor(_base.BaseExecutor):

    def submit(self, task: Task):
        kwargs = task.get_kwargs('celery')
        run_task.apply_async(args=[task], **kwargs)

    def shutdown(self):
        pass
