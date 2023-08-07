import importlib
from typing import Callable, Tuple, Dict

from anyforecast.exceptions import UnknownTaskError
from anyforecast.web import webapp
from anyforecast.executors import get_executor
from .task import Task


class AnyForecast:
    """AnyForecast application.
    """

    def __init__(self):
        self._webapp = webapp
        self._tasks = {}

    def start(self):
        self._import_tasks()
        # self._start_webapp()

    def _start_webapp(self):
        self._webapp.start()

    def _import_tasks(self) -> None:
        importlib.import_module("anyforecast.tasks")

    def send_task(
            self,
            name: str,
            args: Tuple = (),
            kwargs: Dict = None,
            executor: str = None
    ):
        """Sends task by name.

        Parameters
        ----------
        name: str
            Name of task to call (e.g., `"tasks.add"`).

        args: list
            The keyword arguments to pass on to the task.

        kwargs : dict
            Dict of task kwargs.

        executor : str, default=None
            Task executor.
        """
        if kwargs is None:
            kwargs = {}

        task = self.get_task(name)
        executor = get_executor(task.executor or executor)
        return executor.submit(task, *args, **kwargs)

    def get_task(self, name: str) -> Task:
        """Returns task from name.
        """
        if name not in self._tasks:
            raise UnknownTaskError(name=name)
        return self._tasks[name]

    def task(self, *args, **kwargs):
        """Decorator to create a task class out of any callable.

        See :ref:`Task options<task-options>` for a list of the
        arguments that can be passed to this decorator.
        """

        def task_decorator(fun: Callable):
            """Actual decorator.
            """
            def create_task_object():
                task = Task.from_callable(fun, **kwargs)
                self._tasks[task.name] = task
                return task

            return create_task_object()

        # There is only 1 positional argument allowed, and it must be a
        # python callable (from which the Task object will be created).
        if args:
            if len(args) == 1:
                fun = args[0]
                if callable(fun):
                    return task_decorator(fun)
                raise TypeError(
                    "First positional argument to @task() must be a callable")

            raise TypeError(
                f'@task() takes exactly 1 positional argument '
                f'({len(args)} given).')

        return task_decorator
