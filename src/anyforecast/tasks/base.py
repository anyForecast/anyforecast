from __future__ import annotations

from anyforecast.callbacks import Callback
from anyforecast.tasks import registry


def gen_task_name(name, module_name) -> str:
    return ".".join([module_name, name])


def unpickle_task(name) -> Task:
    return TasksFactory._registry[name]


class TasksFactory:
    _registry = registry.TasksRegistry()

    @classmethod
    def register(cls, name: str | None = None) -> callable:
        """Use is as decorator to register tasks in the internal registry.

        Parameters
        ----------
        name : str, default = None
            The name of the task.

        Returns
        -------
        decorator : callable
            Actual decorator that registers :class:`Task` instances to the
            internal registry under the given name.
        """

        def decorator(fun: callable) -> callable:
            """Registers :class:`Task` instances to the internal registry."""
            task = Task.from_callable(fun, name)
            cls._registry.register(task.name, task)
            return task

        return decorator

    @classmethod
    def get(cls, name: str) -> Task:
        """Tasks factory.

        Parameters
        ----------
        name : str
            The name of the task to create.

        Returns
        -------
        task : Task
            An instance of the task that is created.
        """
        return cls._registry[name]


class Task:
    """Task base class.

    Notes
    -----
    When called, tasks apply the :meth:`run` method.  This method must
    be defined by all tasks (that is unless the :meth:`__call__` method
    is overridden).
    """

    #: Name of the task.
    name: str = None

    #: Run behavior. Do nothing by default,
    callbacks: list[Callback] = []

    def run(self, *args, **kwargs):
        """The body of the task executed by workers."""
        raise NotImplementedError("Tasks must define the `run` method.")

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def __reduce__(self):
        return (unpickle_task, (self.name,), None)

    def notify(self, method_name: str, **kwargs) -> None:
        for cb in self.callbacks:
            getattr(cb, method_name)(**kwargs)

    def set_callbacks(self, callbacks: list[Callback]) -> Task:
        self.callbacks = callbacks

    @classmethod
    def from_callable(
        cls, fun: callable, name: str | None = None, **kwargs
    ) -> Task:
        name = name or gen_task_name(fun.__name__, fun.__module__)
        base = cls
        kwargs = {
            "run": staticmethod(fun),
            "name": name,
            "__doc__": fun.__doc__,
            "__module__": fun.__module__,
            "__annotations__": fun.__annotations__,
            **kwargs,
        }

        task = type(fun.__name__, (base,), kwargs)

        return task()
