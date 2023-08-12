import importlib
from typing import Callable, Dict, Optional, Tuple

from kombu.utils.uuid import uuid

from anyforecast.exceptions import UnknownTaskError
from anyforecast.executors import get_executor
from anyforecast.web import webapp

from .task import Task
from .taskrunner import TaskRunner


class AnyForecast:
    """AnyForecast application."""

    def __init__(self):
        self._webapp = webapp
        self._tasks = {}

    def _import_tasks(self) -> None:
        importlib.import_module("anyforecast.tasks")

    def create_runner(
        self,
        name: str,
        args: Tuple = (),
        kwargs: Optional[Dict] = None,
        task_id: Optional[str] = None,
    ) -> TaskRunner:
        """Sends task by name.

        Parameters
        ----------
        name: str
            Name of task to run (e.g., `"tasks.add"`).

        args: list
            The positional arguments to pass on to the task.

        kwargs : dict
            The keyword arguments to pass on to the task.

        executor : str, default=None
            Task executor.
        """
        if kwargs is None:
            kwargs = {}

        task_id = task_id or uuid()
        task = self.get_task(name)
        return TaskRunner(task_id, task, args, kwargs)

    def get_task(self, name: str) -> Task:
        """Returns task from name."""
        if name not in self._tasks:
            raise UnknownTaskError(name=name)
        return self._tasks[name]

    def task(self, *args, **kwargs):
        """Decorator to create a task class out of any callable.

        See :ref:`Task options<task-options>` for a list of the
        arguments that can be passed to this decorator.
        """

        def task_decorator(fun: Callable):
            """Actual decorator."""

            def create_task_object():
                task = Task.from_callable(fun, **kwargs)
                self._tasks[task.name] = task
                return task

            return create_task_object()

        # Only 1 positional argument is allowed and it must be a
        # python callable (from which the Task object will be created).
        if args:
            if len(args) == 1:
                fun = args[0]
                if callable(fun):
                    return task_decorator(fun)
                raise TypeError(
                    "First positional argument to @task() must be a callable"
                )

            raise TypeError(
                f"@task() takes exactly 1 positional argument "
                f"({len(args)} given)."
            )

        return task_decorator
