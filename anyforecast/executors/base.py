from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Protocol

log = logging.getLogger(__name__)


class Runner(Protocol):
    """Runner interace."""

    def run(self):
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
    def from_id(cls, id: str) -> Future:
        raise NotImplementedError()


class ExecutorBackend(ABC):

    """Base class to inherit for concrete executors.

    . note::
        This class should not be used directly. Use derived classes instead.
    """

    @abstractmethod
    def execute(self, runner: Runner, **opts) -> Future:
        """Executes the task."""
        pass
