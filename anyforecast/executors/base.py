from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol, Type


class Executor(Protocol):
    """Executor interface."""

    def execute(self):
        ...


class Future(ABC):
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


class ExecutorBackend(ABC):
    """Base class to inherit for concrete executors.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    def __init__(self, future_cls: Type[Future]):
        self._future_cls = future_cls

    @abstractmethod
    def execute(self, executor: Executor, **opts) -> Future:
        """Executes task."""
        pass

    def get_future_cls(self) -> Type[Future]:
        """Returns executor backend's associated future class."""
        return self._future_cls
