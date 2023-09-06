from __future__ import annotations

from typing import Dict, Tuple

from kombu.utils.uuid import uuid

from anyforecast.executors import ExecutorBackend, LocalExecutor
from anyforecast.models.dbsession import validate_database
from anyforecast.tasks import Task, tasks_registry
from anyforecast.web import webapp

from .execution import Executor
from .task import TaskAsyncResult, TaskContainer


class AnyForecast:
    """AnyForecast application."""

    def __init__(self):
        self._webapp = webapp
        self._executor = Executor()

    def start(self) -> None:
        validate_database()

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
        return tasks_registry.get(name)
