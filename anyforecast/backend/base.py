from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Protocol, Type


def check_backend_exec(backend_exec: BackendExecutor):
    if not isinstance(backend_exec, BackendExecutor):
        raise ValueError(
            "Passed `backend_exec` is not an instance of `BackendExecutor`. "
            f"Instead got {type(backend_exec).__name__}."
        )


class BackendRunner(Protocol):
    """Runner interface.

    Any object implementing the :class:`BackendRuner` interface can be passed
    to the backend executors.
    """

    def run(self) -> Any:
        ...


class BackendFuture(ABC):
    """Base class to inherit for concrete future/async results.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    @abstractmethod
    def result(self) -> Any:
        """Return the result of the call that the future represents."""
        pass

    @abstractmethod
    def wait(self) -> None:
        """Waits for task to complete."""
        pass

    @abstractmethod
    def done(self) -> bool:
        """Return True if the future was cancelled or finished executing."""
        pass


class BackendExecutor(ABC):
    """Base class to inherit for concrete backend executors.

    Backend executors recieve any runner (object with :meth:`run`)
    and run it on their own workers.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    @abstractmethod
    def run(self, runner: BackendRunner) -> BackendFuture:
        """Runs task.

        Parameters
        ----------
        executor : Executor
            Object with :meth:`execute`.
        """
