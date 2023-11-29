from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any

from . import base


def _execute(
    fn,
    args: tuple = (),
    kwargs: dict | None = None,
    max_workers: int | None = None,
) -> Future:
    if kwargs is None:
        kwargs = {}

    executor = ThreadPoolExecutor(max_workers=max_workers)
    return executor.submit(fn, *args, **kwargs)


class LocalFuture(base.BackendFuture):
    def __init__(self, python_future: Future) -> None:
        self.python_future = python_future

    def result(self) -> Any:
        return self.python_future.result()

    def wait(self):
        return wait([self.python_future])

    def done(self) -> bool:
        return self.python_future.done()


class LocalBackend(base.BackendExecutor):
    """Local executor.

    The local executor uses the built-in :class:`ProcessPoolExecutor` located
    in the ``concurrent`` python package.
    """

    def __init__(self, max_workers: int | None = None):
        self.max_workers = max_workers

    def run(self, runner: base.BackendRunner) -> LocalFuture:
        python_future = _execute(fn=runner.run, max_workers=self.max_workers)
        return LocalFuture(python_future)

    def get_future_cls(self):
        return LocalFuture
