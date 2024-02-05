import os
from typing import Any, Literal

import mlflow

from anyforecast.tasks import TasksFactory


class EnvVarsSetter:
    def __init__(self, vars: dict[str, Any]) -> None:
        self.vars = vars

    def set(self) -> None:
        for k, v in self.vars.items():
            os.environ[k] = v


@TasksFactory.register()
def run_mlflow(
    uri: str | None = None,
    entry_point: str = "main",
    parameters: dict[str, Any] | None = None,
    experiment_name: str | None = None,
    experiment_id: str | None = None,
    storage_dir: str | None = None,
    run_name: str | None = None,
    env_manager: Literal["local", "virtualenv", "conda"] | None = None,
    env_vars: dict[str, Any] | None = None,
) -> mlflow.projects.SubmittedRun:
    if env_vars is not None:
        EnvVarsSetter(env_vars).set()

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
