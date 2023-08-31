import logging
import queue

from anyforecast.app.task import Task

from . import executorbackend

log = logging.getLogger(__name__)


class LocalExecutor(executorbackend.Executor):
    def start(self):
        log.debug("Starting Local Executor.")

    def execute(self, task: Task, *args, **kwargs):
        print(f'Task: {task}')
        return task(*args, **kwargs)

    def shutdown(self):
        pass
