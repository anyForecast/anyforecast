import abc
from typing import Any, Protocol


class Runner(Protocol):
    """Runner interface.

    Any object implementing the :class:`Runner` interface can be passed
    to the backend executors.
    """

    def run(self) -> Any: ...


class Promise(abc.ABC):
    """Base class to inherit for concrete future/async results.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    @abc.abstractmethod
    def result(self) -> Any:
        """Returns the result of the call that the future represents."""
        pass

    @abc.abstractmethod
    def done(self) -> bool:
        """Returns True if the future was cancelled or finished executing."""
        pass


class BackendExecutor(abc.ABC):
    """Base class to inherit for concrete backend executors.

    Backend executors recieve any runner (object with :meth:`run`)
    and run it on their own workers.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    def start(self) -> None:
        """Backend executors might need things to get started."""

    @abc.abstractmethod
    def run(self, runner: Runner) -> Promise:
        """Runs task.

        Parameters
        ----------
        executor : Executor
            Object with :meth:`execute`.
        """


def is_backend(backend: BackendExecutor):
    if not isinstance(backend, BackendExecutor):
        raise ValueError(
            "Passed `backend_exec` is not an instance of `BackendExecutor`. "
            f"Instead got {type(backend).__name__}."
        )
