from __future__ import annotations

import importlib
from typing import Callable, Dict, Tuple

from kombu.utils.uuid import uuid

from anyforecast.executors import ExecutorBackend, LocalExecutor
from anyforecast.web import webapp

from .executor import Executor
from .objects_registry import create_registry
from .task import Task, TaskAsyncResult, TaskContainer
from .dbsession import validate_database


class AnyForecast:
    """AnyForecast application."""

    def __init__(self):
        self._webapp = webapp
        self._executor = Executor()
        self._tasks_registry = create_registry()

    def start(self) -> None:
        validate_database()
        self._import_tasks()

    def _import_tasks(self) -> None:
        importlib.import_module("anyforecast.tasks")

    def execute_task(
        self,
        name,
        args: Tuple = (),
        kwargs: Dict = None,
        exec_backend: ExecutorBackend = LocalExecutor(),
        task_id: str = None,
        **opts,
    ) -> TaskAsyncResult:
        """Executes tasks on the specified executor backend.

        Patameters
        ----------
        name : str
            Name of the task name to execute.

        args : tuple, default=()
            Task positional arguments.

        kwargs : dict, default=None
            Task key-word arguments

        exec_backend : ExecutorBackend, default=LocalExecutor()
            Executor backend.

        task_id : str, default=None
            Task identifier.

        **opts : optional args
            Optional arguments to executor backend.
        """
        if kwargs is None:
            kwargs = {}

        task = self.get_task(name)
        task_id = task_id or uuid()
        task_container = TaskContainer(task, args, kwargs, task_id)
        return self._executor.launch_task(exec_backend, task_container, **opts)

    def get_task(self, name: str) -> Task:
        """Returns task by name.

        Parameters
        ----------
        name : str
            Name of the task.
        """
        return self._tasks_registry.get(name)

    def task(self, *args, **kwargs):
        """Decorator to create a task class out of any callable.

        See :ref:`Task options<task-options>` for a list of the
        arguments that can be passed to this decorator.
        """

        def task_decorator(fun: Callable):
            """Actual decorator."""

            def create_task_object():
                task = Task.from_callable(fun, **kwargs)
                self._tasks_registry.put(task.name, task)
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
