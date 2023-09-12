from .registry import task_registry
from .task import Task


def task(*args, **kwargs):
    """Decorator to create a task class out of any callable.

    See :ref:`Task options<task-options>` for a list of the
    arguments that can be passed to this decorator.
    """

    def task_decorator(fun: callable):
        """Actual decorator."""

        def create_task_object():
            task = Task.from_callable(fun, **kwargs)
            task_registry[task.name] = task
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
                "First positional argument to @task() must be a callable."
            )

        raise TypeError(
            f"@task() takes exactly 1 positional argument "
            f"({len(args)} given)."
        )

    return task_decorator
