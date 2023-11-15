import os
from abc import ABC, abstractmethod
from typing import Any, Literal

from mlflow.projects.submitted_run import SubmittedRun

from anyforecast.deployments import Deployer, get_deployer
from anyforecast.execution import RegisteredTasksExecutor, TaskPromise


class MLFlowEstimator(ABC):
    """Handle end-to-end training and deployment of MLFlow projects.

    Parameters
    ----------
    project : str
        Project name to run.

    entry_point : str, default="main"
        Entry point to run within the project.

    experiment_name : str, default=None
        Name of experiment under which to launch the run.

    experiment_id : str, default=None
        ID of experiment under which to launch the run.

    run_name : str, default=None
        The name to give the MLflow Run associated with the project execution.
        If None, the MLflow Run name is left unset.

    env_manager : str, default=None
        Specify an environment manager to create a new environment for the run
        and install project dependencies within that environment. If
        unspecified, MLflow automatically determines the environment manager to
        use by inspecting files in the project directory.
    """

    #: Name of the task to be executed.
    task_name: str = "anyforecast.tasks.mlflow.run_mlflow_project"

    def __init__(
        self,
        project_uri: str,
        entry_point: str = "main",
        experiment_name: str | None = None,
        experiment_id: str | None = None,
        run_name: str | None = None,
        env_manager: Literal["local", "virtualenv", "conda"] | None = None,
    ):
        self.project_uri = project_uri
        self.entry_point = entry_point
        self.experiment_name = experiment_name
        self.experiment_id = experiment_id
        self.run_name = run_name
        self.env_manager = env_manager

        self.executor = RegisteredTasksExecutor()

    @abstractmethod
    def get_parameters(self) -> dict:
        pass

    def get_kwargs(self) -> dict[str, Any]:
        return {
            "uri": self.project_uri,
            "entry_point": self.entry_point,
            "parameters": self.get_parameters(),
            "experiment_name": self.experiment_name,
            "experiment_id": self.experiment_id,
            "run_name": self.run_name,
            "env_manager": self.env_manager,
        }

    def fit_async(self, backend_exec: str = "local") -> TaskPromise:
        """Fits estimator asynchronously.

        Parameters
        ----------
        inputs : dict, str -> str
            Location where training data is saved.

        backend_exec : str
            Backend executor.
        """
        return self.executor.execute_async(
            name=self.task_name,
            kwargs=self.get_kwargs(),
            backend_exec=backend_exec,
        )

    def fit(self):
        """Fits estimator.

        Parameters
        ----------
        inputs : dict, str -> str
            Location where training data is saved.

        Returns
        -------
        self : object
        """
        self.run_: SubmittedRun = self.executor.execute(
            name=self.task_name, kwargs=self.get_kwargs()
        )
        return self

    def deploy(self, mode: str = "local", config: dict = None):
        if config is None:
            config = {}

        run_id = self.run_.run_id
        model_uri = f"runs:/{run_id}/model"
        deployer: Deployer = get_deployer(mode, **config)
        return deployer.deploy(run_id, model_uri)
