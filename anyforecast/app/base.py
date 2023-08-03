from typing import Union

from anyforecast.executors import Executor
from anyforecast.web import webapp


class AnyForecast:
    """AnyForecast application.

    Parameters
    ----------
    executor : str or anyforecast.executors.Executor
        The executor class object or the name of the executor class to use.
    """

    def __init__(self, executor: Union[str, Executor]):
        self._executor = executor
        self._webapp = webapp

    def start(self):
        self._start_webapp()
        self._start_executor()

    def _start_webapp(self):
        self._webapp.start()

    def _start_executor(self):
        self._executor.start()

    def set_executor(self, executor: Executor):
        self._executor = executor

    def task(self, *args, **kwargs):
        """Decorator to create a class out of any callable.
        """
        pass
