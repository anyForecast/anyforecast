from typing import Callable


def gen_task_name(name, module_name):
    return ".".join([module_name.split(".")[-1], name])


class Task:
    """AnyForecast Task

    Parameters
    ----------
    name : str
        Name of the task
    """

    name = None

    def run(self, *args, **kwargs):
        """The body of the task executed by workers."""
        raise NotImplementedError("Tasks must define the _run method.")

    def __call__(self, *args, **kwargs):
        try:
            retval = self(*args, **kwargs)
        except Exception as exc:
            self.on_failure(exc, args, kwargs)

        self.on_success(retval, args, kwargs)

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
    def from_callable(cls, fun: Callable, **kwargs):
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

        return task
