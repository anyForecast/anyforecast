from concurrent.futures import Future as PythonFuture
from concurrent.futures import ProcessPoolExecutor

from . import base

PYTHON_EXECUTOR = ProcessPoolExecutor()


class LocalFuture(base.BackendFuture):
    def __init__(self, python_future: PythonFuture) -> None:
        self.python_future = python_future

    def get_id(self) -> str:
        pass

    def get_state(self) -> str:
        pass


def execute_task(executor: base.Executor, **opts):
    return PYTHON_EXECUTOR.submit(executor.execute, **opts)


@base.BackendExecutorFactory.register("local")
class LocalExecutor(base.BackendExecutor):
    """Local executor.

    The local executor uses the built-in :class:`ProcessPoolExecutor` located
    in the ``concurrent`` python package.
    """

    def __init__(self):
        super().__init__(future_cls=LocalFuture)

    def execute(self, executor: base.Executor, **opts) -> LocalFuture:
        future = execute_task(executor)
        return LocalFuture(future)

    def get_future_cls(self):
        return LocalFuture
