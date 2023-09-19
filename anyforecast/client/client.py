from __future__ import annotations

from typing import Dict, Tuple

from kombu.utils.uuid import uuid

from anyforecast.exceptions import RunningTasksDoesNotExist
from anyforecast.tasks import Task, task_registry

from .execution import ClientExecutorBridge, TaskContainer
from .promise import TaskPromise


class RunningTasks(dict):
    """Map of running task promises."""

    def __missing__(self, key):
        raise RunningTasksDoesNotExist(task_id=key)

    def add(self, promise: TaskPromise):
        self[promise.task_id] = promise

    def remove(self, task_id: str):
        self.pop(task_id)

    def get(self, task_id: str):
        return self[task_id]


class AnyForecastClient:
    """AnyForecast client application."""

    def __init__(self):
        self._running = RunningTasks()

    def save_promise(self, promise: TaskPromise) -> None:
        """Saves promise."""
        self._running.add(promise)

    def get_promise(self, task_id: str) -> TaskPromise:
        return self._running.get(task_id)

    def execute_task(
        self,
        name,
        args: Tuple = (),
        kwargs: Dict = None,
        backend: str = "local",
        task_id: str = None,
        **opts,
    ) -> TaskPromise:
        """Executes tasks on the specified executor backend.

        Patameters
        ----------
        name : str
            Name of the task name to execute.

        args : tuple, default=()
            Task positional arguments.

        kwargs : dict, default=None
            Task key-word arguments

        backend : str or ExecutorBackend, default="local"
            Backend executor.

        task_id : str, default=None
            Task identifier.

        **opts : optional args
            Optional arguments to executor backend.
        """
        if kwargs is None:
            kwargs = {}

        client_executor_bridge = ClientExecutorBridge()
        task = self.get_task(name)
        task_id = task_id or uuid()
        task_container = TaskContainer(task, args, kwargs, task_id)
        promise = client_executor_bridge.submit_task(
            backend, task_container, **opts
        )
        self.save_promise(promise)
        return promise

    def get_available_tasks(self) -> list:
        """Returns tasks registry."""
        return list(task_registry)

    def get_task(self, name: str) -> Task:
        """Returns single task by name.

        Parameters
        ----------
        name : str
            Name of the task.
        """
        return task_registry[name]
