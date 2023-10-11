from typing import Any, Literal

import mlflow

from .task import TasksFactory


@TasksFactory.register()
def run_mlflow_project(
    uri: str | None = None,
    entry_point: str = "main",
    parameters: dict[str, Any] | None = None,
    experiment_name: str | None = None,
    experiment_id: str | None = None,
    storage_dir: str | None = None,
    run_name: str | None = None,
    env_manager: Literal["local", "virtualenv", "conda"] | None = None,
) -> mlflow.projects.SubmittedRun:
    return mlflow.projects.run(
        uri=uri,
        entry_point=entry_point,
        parameters=parameters,
        experiment_name=experiment_name,
        experiment_id=experiment_id,
        storage_dir=storage_dir,
        run_name=run_name,
        env_manager=env_manager,
    )
