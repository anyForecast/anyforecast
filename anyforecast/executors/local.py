import logging
import queue

from anyforecast.app.task import Task

from . import base

log = logging.getLogger(__name__)


class LocalExecutor(base.Executor):
    def start(self):
        log.debug("Starting Local Executor.")

    def execute(self, task: Task, *args, **kwargs):
        return task(*args, **kwargs)

    def shutdown(self):
        pass
