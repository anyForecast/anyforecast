from . import _base
from tasks.tasks import Task


class LocalExecutor(_base.BaseExecutor):

    def submit(self, task: Task):
        task.run()

    def shutdown(self):
        pass
