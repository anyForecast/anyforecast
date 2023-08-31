from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Tuple

from anyforecast.executors import Future


def gen_task_name(name, module_name):
    return ".".join([module_name.split(".")[-1], name])


@dataclass
class TaskDescription:
    task: Task
    args: Tuple
    kwargs: Dict
    task_id: str


class Task:
    """AnyForecast Task"""

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


class TaskAsyncResult:
    def __init__(self, task_id: str, future: Future):
        self.task_id = task_id
        self.future = future

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.task_id}>"

    @property
    def state(self):
        return self.future.get_state()


class TaskRunner:
    def __init__(self, task_descr: TaskDescription):
        self.task_descr = task_descr

    def run(self):
        try:
            # Run the actual task.
            retval = self.task_descr.task(*self.args, **self.kwargs)
        except Exception as exc:
            pass

        return retval
