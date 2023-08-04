from typing import Callable


def gen_task_name(name, module_name):
    return '.'.join(p for p in (module_name, name) if p)


class Task:

    def run(self, *args, **kwargs):
        """The body of the task executed by workers."""
        raise NotImplementedError('Tasks must define the run method.')

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    @classmethod
    def from_callable(cls, fun: Callable):
        name = gen_task_name(fun.__name__, fun.__module__)
        base = cls
        kwargs = {
            'name': name,
            'run': fun,
            '__doc__': fun.__doc__,
            '__module__': fun.__module__
        }

        task = type(fun.__name__, (base,), kwargs)()

        return task
