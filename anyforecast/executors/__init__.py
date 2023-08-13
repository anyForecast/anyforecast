from typing import Dict

from .base import Executor, Future
from .celery import CeleryExecutor
from .local import LocalExecutor
from .ray import RayExecutor

_EXECUTORS = {
    "local": LocalExecutor,
    "celery": CeleryExecutor,
    "ray": RayExecutor,
}


def get_executors() -> Dict[str, Executor]:
    return _EXECUTORS


def get_executor(name: str):
    """Executors factory.

    Parameters
    ----------
    name : str
        Executor name.
    """
    if isinstance(name, Executor):
        return name

    return _EXECUTORS[name]()


__all__ = ["get_executors", "get_executor", "Executor", "Future"]
