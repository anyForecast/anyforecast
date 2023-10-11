import os
from functools import cached_property
from typing import Any, Literal, Self

from anyforecast.client import TaskPromise, TasksClient
from anyforecast.definitions import PROJECTS_PATH


class MLFlowPredictor:
    def predict(self):
        pass


class MLFlowDeployer:
    def deploy():
        pass


class MLFlowEstimator:
    """Handle end-to-end training and deployment of MLFlow projects.

    Parameters
    ----------
    """

    def __init__(
        self,
        project: str,
        entry_point: str = "main",
        parameters: dict[str, Any] | None = None,
        experiment_name: str | None = None,
        experiment_id: str | None = None,
        storage_dir: str | None = None,
        run_name: str | None = None,
        env_manager: Literal["local", "virtualenv", "conda"] | None = None,
    ):
        self.project = project
        self.entry_point = entry_point
        self.parameters = parameters
        self.experiment_name = experiment_name
        self.experiment_id = experiment_id
        self.storage_dir = storage_dir
        self.run_name = run_name
        self.env_manager = env_manager

    @cached_property
    def client(self) -> TasksClient:
        return TasksClient()

    @property
    def project_uri(self) -> str:
        return os.path.join(PROJECTS_PATH, self.project)

    @property
    def task_name(str) -> str:
        return "anyforecast.tasks.mlflow.run_mlflow_project"

    def check_is_fitted(self):
        if not hasattr("submitted_run_"):
            raise ValueError()

    def get_kwargs(self, inputs: dict[str, str]) -> dict[str, Any]:
        parameters = {} if self.parameters is None else self.parameters.copy()
        parameters.update(inputs)

        return {
            "uri": self.project_uri,
            "entry_point": self.entry_point,
            "parameters": parameters,
            "experiment_name": self.experiment_name,
            "experiment_id": self.experiment_id,
            "storage_dir": self.storage_dir,
            "run_name": self.run_name,
            "env_manager": self.env_manager,
        }

    def fit_async(
        self, inputs: dict[str, str], backend_exec: str = "local"
    ) -> TaskPromise:
        return self.client.execute_async(
            self.task_name,
            kwargs=self.get_kwargs(inputs),
            backend_exec=backend_exec,
        )

    def fit(self, inputs: dict[str, str]) -> Self:
        kwargs = self.get_kwargs(inputs)
        self.run_ = self.client.execute(self.task_name, kwargs=kwargs)
        return self

    def deploy(self):
        self.check_is_fitted()
