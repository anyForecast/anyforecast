from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Tuple

from anyforecast.executors import Future
from anyforecast.tasks import Task


class TaskStatus(Enum):
    """Task status.

    Attributes
    ----------
    READY. Task has not been run.
    RUNNING. Task is currently in progress.
    COMPLETED. Task has completed
    FAILED. An error occurred with the task.
    """

    READY = 0
    RUNNING = 1
    COMPLETED = 2
    FAILED = 3


class TaskPromise:
    """Query task state.

    Parameters
    ----------
    task_id : str
        The task's UUID.
    """

    def __init__(self, task_id: str, future: Future):
        self.task_id = task_id
        self.future = future

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.task_id}>"

    @property
    def state(self):
        return self.future.get_state()


@dataclass
class TaskContainer:
    """Holds the Task instance and its parameters.

    Parameters
    ----------
    task : Task
        Task instance.

    args : tuple
            Task positional arguments.

    kwargs : dict
        Task key-word arguments

    task_id : str
        The task's UUID.
    """

    task: Task
    args: Tuple
    kwargs: Dict
    task_id: str

    def run(self) -> Any:
        return self.task(*self.args, **self.kwargs)
