import os
from typing import Any, Literal

import mlflow

from anyforecast.registry import Registry

registry = Registry()


def set_environmet(
    enviroment: dict[str, Any], upper_case: bool = False
) -> None:
    """Sets enviroment variables."""
    for k, v in enviroment.items():
        if upper_case:
            k = k.upper()
        os.environ[k] = v


@registry()
def run_mlflow(
    uri: str | None = None,
    entry_point: str = "main",
    version: str | None = None,
    parameters: dict[str, Any] | None = None,
    experiment_name: str | None = None,
    experiment_id: str | None = None,
    storage_dir: str | None = None,
    run_name: str | None = None,
    env_manager: Literal["local", "virtualenv", "conda"] | None = None,
    environment: dict[str, Any] | None = None,
) -> mlflow.projects.SubmittedRun:
    if environment is not None:
        set_environmet(environment, upper_case=True)

    return mlflow.projects.run(
        uri=uri,
        entry_point=entry_point,
        version=version,
        parameters=parameters,
        experiment_name=experiment_name,
        experiment_id=experiment_id,
        storage_dir=storage_dir,
        run_name=run_name,
        env_manager=env_manager,
    )
