from __future__ import annotations

from anyforecast.exceptions import TaskNotRegistered


def gen_task_name(name, module_name):
    return ".".join([module_name, name])


def unpickle_task(name) -> Task:
    return TasksFactory.registry[name]


class TasksRegistry(dict):
    """Map of registered tasks."""

    def __missing__(self, key):
        raise TaskNotRegistered(name=key)

    def register(self, name, task):
        """Register a task in the task registry.

        The task will be automatically instantiated if not already an
        instance. Name must be configured prior to registration.
        """
        self[name] = task

    def unregister(self, name: str):
        """Unregisters task by name.

        Parameters
        ----------
        name : str or Task
            name of the task to unregister, or a
            :class:`anyforecast.tasks.Task` with a valid `name` attribute.

        Raises
        ------
        anyforecast.exceptions.TaskNotRegistered if the task is not registered.
        """
        try:
            self.pop(getattr(name, "name", name))
        except KeyError:
            raise TaskNotRegistered(name=name)


class TasksFactory:
    registry = TasksRegistry()

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
            """Registers :class:`Task` instances to the internal registry under
            the given name.
            """
            task = Task.from_callable(fun, name)
            cls.registry.register(task.name, task)
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
        return cls.registry[name]


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
    def from_callable(cls, fun: callable, name: str | None = None, **kwargs) -> Task:
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
