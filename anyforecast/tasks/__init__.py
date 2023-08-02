from .tasks import AnyForecastTask



__all__ = [
    'registry'
]


class TaskRegistry:
    def __init__(self):
        self._registry = {}


    def add_task(self, name, task: AnyForecastTask):
        self._registry[name] = task



def create_registry() -> TaskRegistry:
    registry = TaskRegistry()
    for task in TASK:
        celery_task = task.create_celery_task()
        registry.add_task(celery_task)

    return registry


registry = create_registry()


