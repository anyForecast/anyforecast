from anyforecast.exceptions import TaskNotRegistered


class TasksRegistry(dict):
    """Map of registered tasks."""

    def __missing__(self, key):
        raise TaskNotRegistered(name=key)

    def register(self, name, task):
        """Register a task in the task registry.
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
