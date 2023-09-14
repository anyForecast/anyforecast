from anyforecast.exceptions import ExecutorBackendDoesNotExist

from .base import ExecutorBackend, Future
from .celery import CeleryExecutor
from .local import LocalExecutor
from .ray import RayExecutor


def create_executors_mapping() -> dict[str, ExecutorBackend]:
    executors = (
        LocalExecutor,
        CeleryExecutor,
        RayExecutor,
    )

    mapping = {}
    for exec in executors:
        mapping[exec.__name__] = exec
    return mapping


EXECUTORS_BACKEND_MAPPING = create_executors_mapping()


def get_executor_backend(name: str) -> ExecutorBackend:
    """Returns executor instance.

    Parameters
    ----------
    name : str
        Executor name.
    """
    if isinstance(name, ExecutorBackend):
        return name

    if name not in EXECUTORS_BACKEND_MAPPING:
        raise ExecutorBackendDoesNotExist(
            name=name, available=list(EXECUTORS_BACKEND_MAPPING)
        )

    return EXECUTORS_BACKEND_MAPPING[name]()


__all__ = [
    "get_executor_backend",
    "Future",
    "ExecutorBackend",
    "CeleryExecutor",
    "RayExecutor",
    "LocalExecutor",
]
