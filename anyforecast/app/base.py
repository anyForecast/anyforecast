from typing import Union, Callable

from anyforecast.executors import Executor
from anyforecast.task import Task
from anyforecast.web import webapp


class TasksRegistry:
    def __init__(self):
        self._registry = {}

    def register_task(self, task):
        self._registry[task.name] = task


class AnyForecast:
    """AnyForecast application.

    Parameters
    ----------
    executor : str or anyforecast.executors.Executor
        The executor class object or the name of the executor class to use.
    """

    def __init__(self, executor: Union[str, Executor]):
        self._executor = executor
        self._webapp = webapp
        self._tasks_registry = TasksRegistry()

    def start(self):
        #self._start_webapp()
        self._start_executor()

    def _start_webapp(self):
        self._webapp.start()

    def _start_executor(self):
        self._executor.start()

    def set_executor(self, executor: Executor):
        self._executor = executor

    def task(self, fun: Callable):
        """Decorator to create a class out of any callable.
        """

        def create_task_object():
            task = Task.from_callable(fun)
            self.register_task(task)
            return task

        return create_task_object

    def register_task(self, task):
        self._tasks_registry.register_task(task)
