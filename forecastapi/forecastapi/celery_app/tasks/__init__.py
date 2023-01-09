from ._chainer import TaskChainer
from ._dataloading import LoadDatasetTask
from ._minio import GetParquetPartitions, GetLastKnownDate
from ..celery import app
from ..exceptions import UnknownTaskError


class TaskRegistry:

    def __init__(self):
        self._registry = {}

    def register(self, task, name):
        obj = task().make_celery_task(app)
        self._registry[name] = obj

    def get_task(self, name):
        try:
            return self._registry[name]
        except KeyError:
            raise UnknownTaskError(name=name)

    def get_registry(self):
        return self._registry


TASKS = [
    LoadDatasetTask,
    GetParquetPartitions,
    GetLastKnownDate
]


def make_task_registry():
    registry = TaskRegistry()
    for task in TASKS:
        registry.register(task, task.__name__)

    return registry


task_registry = make_task_registry()

__all__ = [
    'task_registry',
    'TaskChainer'
]
