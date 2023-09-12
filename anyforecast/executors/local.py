from concurrent.futures import Future as PythonFuture
from concurrent.futures import ProcessPoolExecutor

from . import base


class LocalFuture(base.Future):
    def __init__(self, python_future: PythonFuture) -> None:
        self.python_future = python_future

    def get_id(self) -> str:
        pass

    def get_state(self) -> str:
        pass


class LocalExecutor(base.ExecutorBackend):
    def __init__(self):
        self._python_executor = ProcessPoolExecutor()

    def execute(self, runner: base.Runner, **opts) -> LocalFuture:
        future = self._python_executor.submit(runner.run, **opts)
        return LocalFuture(future)
