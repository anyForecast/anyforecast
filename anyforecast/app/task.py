from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Tuple

from anyforecast.executors import Future


def gen_task_name(name, module_name):
    return ".".join([module_name.split(".")[-1], name])


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


class TaskAsyncResult:
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


class Task:
    """Task base class.

    Notes
    -----
    When called tasks apply the :meth:`run` method.  This method must
    be defined by all tasks (that is unless the :meth:`__call__` method
    is overridden).
    """

    #: Name of the task.
    name = None

    def run(self, *args, **kwargs):
        """The body of the task executed by workers."""
        raise NotImplementedError("Tasks must define the `run` method.")

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def on_success(self, retval, task_id, args, kwargs):
        """Success handler.

        Run by the worker if the task executes successfully.
        """
        pass

    def on_failure(self, exc, task_id, args, kwargs):
        """Error handler.

        This is run by the worker when the task fails.
        """
        pass

    @classmethod
    def from_callable(cls, fun: Callable, **kwargs) -> Task:
        if "name" not in kwargs:
            kwargs["name"] = gen_task_name(fun.__name__, fun.__module__)

        base = cls
        kwargs = {
            "run": staticmethod(fun),
            "__doc__": fun.__doc__,
            "__module__": fun.__module__,
            **kwargs,
        }

        task = type(fun.__name__, (base,), kwargs)

        return task()
