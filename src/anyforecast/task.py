from __future__ import annotations

from anyforecast import callback, registry


def gen_task_name(name, module_name) -> str:
    return ".".join([module_name, name])


def unpickle_task(name) -> Task:
    return factory.create(name)


def create_factory() -> TasksFactory:

    from anyforecast.tasks import mlflow
    from anyforecast.testing import test_tasks

    factory = TasksFactory()
    factory.include_registry(test_tasks.registry)
    factory.include_registry(mlflow.registry)

    return factory


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
    callbacks: list[callback.Callback] = ()

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

    def set_callbacks(self, callbacks: list[callback.Callback]) -> Task:
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


class TasksFactory:
    """Tasks registry.

    Example
    -------
    registry = TasksRegistry()

    @registry()
        def sample_task():
            ...
    """

    registry: registry.Registry = registry.Registry()

    def include_registry(self, registry: registry.Registry) -> None:
        self.registry.update(registry)

    def list(self) -> list[str]:
        return list(self.registry)

    def get_registry(self) -> registry.Registry:
        return self.registry

    def create(self, name: str) -> Task:
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
        fun = self.registry[name]
        return Task.from_callable(fun, name)


#: Tasks factory
factory = create_factory()
