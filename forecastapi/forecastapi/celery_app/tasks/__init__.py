from ._dataloading import LoadDatasetTask
from ._minio import GetParquetPartitions, GetLastKnownDate
from ._predict import GroupPredictionTask, ResponseFunctionEstimationTask
from ._train import TrainTask
from ..celery import app
from ._chainer import TaskChainer


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
            raise

    def get_registry(self):
        return self._registry


TASKS = [
    LoadDatasetTask,
    TrainTask,
    GroupPredictionTask,
    ResponseFunctionEstimationTask,
    GetParquetPartitions,
    GetLastKnownDate
]


def register_tasks():
    registry = TaskRegistry()
    for task in TASKS:
        registry.register(task, task.__name__)

    return registry


task_registry = register_tasks()

__all__ = [
    'task_registry',
    'TaskChainer'
]
