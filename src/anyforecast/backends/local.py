from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from anyforecast import backend


def _run(
    fn,
    args: tuple = (),
    kwargs: dict | None = None,
    max_workers: int | None = None,
) -> Future:
    if kwargs is None:
        kwargs = {}

    executor = ThreadPoolExecutor(max_workers=max_workers)
    return executor.submit(fn, *args, **kwargs)


class LocalPromise(backend.Promise):
    def __init__(self, python_future: Future) -> None:
        self.python_future = python_future

    def result(self) -> Any:
        return self.python_future.result()

    def done(self) -> bool:
        return self.python_future.done()


class LocalBackend(backend.BackendExecutor):
    """Local executor.

    The local executor uses the built-in :class:`ThreadPoolExecutor` located
    in the ``concurrent`` python package.
    """

    def __init__(self, max_workers: int | None = None):
        self.max_workers = max_workers

    def run(self, runner: backend.Runner) -> LocalPromise:
        python_future = _run(fn=runner.run, max_workers=self.max_workers)
        return LocalPromise(python_future)
