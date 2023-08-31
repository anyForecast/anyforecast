from __future__ import annotations

import importlib
from typing import Callable, Dict, Tuple

from anyforecast.exceptions import UnknownTaskError
from anyforecast.executors import ExecutorBackend, LocalExecutor
from anyforecast.web import webapp

from .executor import Executor
from .task import Task, TaskAsyncResult, TaskDescription


class AnyForecast:
    """AnyForecast application."""

    def __init__(self):
        self._webapp = webapp
        self._executor = Executor()
        self._tasks = {}

    def start(self) -> None:
        self._import_tasks()

    def _import_tasks(self) -> None:
        importlib.import_module("anyforecast.tasks")

    def execute_task(
        self,
        name,
        args: Tuple = (),
        kwargs: Dict = None,
        exec_backend: ExecutorBackend = LocalExecutor(),
        **opts,
    ) -> TaskAsyncResult:
        if kwargs is None:
            kwargs = {}

        task = self.get_task(name)
        task_descr = TaskDescription(task, args, kwargs)
        return self._executor.launch_task(exec_backend, task_descr)

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
