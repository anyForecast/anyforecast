import importlib

from anyforecast.tasks.base import Task, TasksFactory


def discover_tasks() -> None:
    MODULES = ["examples", "mlflow"]

    for mod in MODULES:
        importlib.import_module(name=f"anyforecast.tasks.{mod}")


discover_tasks()


__all__ = ["Task", "TasksFactory"]
