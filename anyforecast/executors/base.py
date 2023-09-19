from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol, Type

from anyforecast.exceptions import ExecutorBackendDoesNotExist


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


class BackendExecutor(ABC):
    """Base class to inherit for concrete backend executors.

    Backend executors recieve any executable (object with :meth:`execute`)
    and execute it on their own workers.

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


class BackendExecutorFactory:
    registry = {}

    @classmethod
    def register(cls, name: str) -> callable:
        """Use is as decorator to register backend executors to the internal
        registry.

        Parameters
        ----------
        name : str
            The name of the executor.

        Returns
        -------
        wrapper : callable
            Actual decorator that registers the BackendExecutor class to the
            internal registry under the given name.
        """

        def wrapper(backend_exec: BackendExecutor) -> callable:
            """Registers backend executor class to the internal registry.

            This is the actual decorator.
            """
            cls.registry[name] = backend_exec
            return backend_exec

        return wrapper

    @classmethod
    def create(cls, name: str, **kwargs) -> BackendExecutor:
        """Backend executors factory.

        This method retrieves the BackendExecutor class from the registry and
        creates an instance of it, while passing in the parameters given in
        ``kwargs``.

        Parameters
        ----------
        name : str
            The name of the executor to create.

        Returns
        -------
        backend_executor : BackendExecutor
            An instance of the backend executor that is created.
        """
        if name not in cls.registry:
            raise ExecutorBackendDoesNotExist(
                name=name, available=tuple(cls.registry)
            )

        return cls.registry[name](**kwargs)
