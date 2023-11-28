from concurrent.futures import Future as PythonFuture
from concurrent.futures import ProcessPoolExecutor

from . import base


class LocalFuture(base.BackendFuture):
    def __init__(self, python_future: PythonFuture) -> None:
        self.python_future = python_future

    def get_id(self) -> str:
        pass

    def get_state(self) -> str:
        pass


class LocalBackend(base.BackendExecutor):
    """Local executor.

    The local executor uses the built-in :class:`ProcessPoolExecutor` located
    in the ``concurrent`` python package.
    """

    def __init__(self, max_workers: int | None = None):
        self._executor = ProcessPoolExecutor(max_workers=max_workers)

    def run(self, runner: base.BackendRunner) -> LocalFuture:
        future = self._executor.submit(runner.run)
        return LocalFuture(future)

    def get_future_cls(self):
        return LocalFuture
