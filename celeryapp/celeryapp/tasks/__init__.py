from . import dataloading
from . import inference
from . import train
from .exceptions import UnknownTaskError

TASKS = [
    dataloading.LoadPandas,
    train.SkorchForecastingTrainer,
    inference.BaseInference
]


class TaskRegistry:

    def __init__(self):
        self._registry = {}

    def register(self, task):
        task = task()
        celery_task = task.create_celery_task()
        self._registry[celery_task.name] = celery_task

    def get_task(self, name):
        try:
            return self._registry[name]
        except KeyError:
            raise UnknownTaskError(name=name)

    def get_registry(self):
        return self._registry


def create_task_registry():
    registry = TaskRegistry()
    for task in TASKS:
        registry.register(task)

    return registry


task_registry = create_task_registry()

__all__ = [
    'task_registry'
]
