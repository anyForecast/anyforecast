from __future__ import annotations

from typing import Callable
from .registry import unpickle_task


def gen_task_name(name, module_name):
    return ".".join([module_name, name])


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

    def __reduce__(self):
        return (unpickle_task, (self.name,), None)

    def on_success(self, retval, task_id, args, kwargs) -> None:
        """Success handler.

        Run by the worker if the task executes successfully.
        """
        pass

    def on_failure(self, exc, task_id, args, kwargs) -> None:
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
            "__annotations__": fun.__annotations__,
            **kwargs,
        }

        task = type(fun.__name__, (base,), kwargs)

        return task()
