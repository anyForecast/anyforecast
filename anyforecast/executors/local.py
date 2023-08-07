import logging

from anyforecast.app.task import Task
from . import base

log = logging.getLogger(__name__)


class LocalExecutor(base.Executor):

    def start(self):
        log.debug("Starting Local Executor.")

    def submit(self, task: Task, *args, **kwargs):
        return task.run(*args, **kwargs)

    def shutdown(self):
        pass
