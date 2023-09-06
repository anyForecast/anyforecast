from __future__ import annotations

from typing import Callable, Dict


def gen_task_name(name, module_name):
    return ".".join([module_name.split(".")[-1], name])


class TasksRegistry:
    """Tasks registry."""

    def __init__(self):
        self._registry: Dict[str, Task] = {}

    def get(self, name: str) -> Task:
        """Retrieves objective function by its name.
        Parameters
        ----------
        name : str
            Objective function name.
        """
        if name not in self._registry:
            raise ValueError(f"Object '{name}' does not exist.")
        return self._registry[name]

    def get_registry(self) -> Dict[str, Task]:
        """Returns registered objects."""
        return self._registry

    def put(self, name, obj: Task) -> None:
        """Registers new object.

        Parameters
        ----------
        name : str
            Object identifier name. Use this name to retrieve the object through
            method :meth:`get`.

        obj : Any
            Object to store.
        """
        self._registry[name] = obj


tasks_registry = TasksRegistry()


def task(*args, **kwargs):
    """Decorator to create a task class out of any callable.

    See :ref:`Task options<task-options>` for a list of the
    arguments that can be passed to this decorator.
    """

    def task_decorator(fun: Callable):
        """Actual decorator."""

        def create_task_object():
            task = Task.from_callable(fun, **kwargs)
            tasks_registry.put(task.name, task)
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
