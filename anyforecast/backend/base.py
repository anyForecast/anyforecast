from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Protocol, Type


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
    def get_state(self) -> str:
        pass

    @abstractmethod
    def get_id(self) -> str:
        pass

    @classmethod
    def from_id(cls, id: str):
        raise NotImplementedError()


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

    def get_future_cls(self) -> Type[BackendFuture]:
        """Returns executor backend's associated future class."""
        return self._future_cls
