from typing import Callable, Optional


def gen_task_name(name, module_name):
    return '.'.join([module_name.split('.')[-1], name])


class Task:
    """AnyForecast Task

    Parameters
    ----------
    name : str
        Name of the task

    executor : str, default=None
        Default task executor.
    """

    def __init__(self, name, executor: Optional[str] = None):
        self.name = name
        self.executor = executor

    def run(self, *args, **kwargs):
        """The body of the task executed by workers."""
        raise NotImplementedError('Tasks must define the run method.')

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    @classmethod
    def from_callable(cls, fun: Callable, **kwargs):
        if "name" not in kwargs:
            kwargs["name"] = gen_task_name(fun.__name__, fun.__module__)

        base = cls
        methods = {
            'run': staticmethod(fun),
            '__doc__': fun.__doc__,
            '__module__': fun.__module__
        }

        task = type(fun.__name__, (base,), methods)(**kwargs)

        return task
