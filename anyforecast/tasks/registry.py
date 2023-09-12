import inspect

from anyforecast.exceptions import InvalidTaskError, TaskNotRegistered


class TaskRegistry(dict):
    """Map of registered tasks."""

    def __missing__(self, key):
        raise TaskNotRegistered(name=key)

    def register(self, task):
        """Register a task in the task registry.

        The task will be automatically instantiated if not already an
        instance. Name must be configured prior to registration.
        """
        if task.name is None:
            raise InvalidTaskError(name=type(task).__name__)

        task = inspect.isclass(task) and task() or task
        self[task.name] = task

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


task_registry = TaskRegistry()


def unpickle_task(name):
    return task_registry[name]
