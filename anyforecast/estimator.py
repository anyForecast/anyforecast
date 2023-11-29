from abc import ABC, abstractmethod
from typing import Any, Literal

from anyforecast.backend import BackendExecutor, LocalBackend
from anyforecast.callbacks import Callback
from anyforecast.deployments import Deployer, get_deployer
from anyforecast.execution import TaskPromise, TasksExecutor


class MLFlowEstimator(ABC):
    """Handle training and deployment of MLFlow projects.

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

    callbacks : list of Callback instances, default=()
        Which callbacks to enable.


    Attributes
    ----------
    promise_ : TaskPromise
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
        callbacks: list[Callback] = (),
        backend_exec: BackendExecutor = LocalBackend(),
    ):
        self.project_uri = project_uri
        self.entry_point = entry_point
        self.experiment_name = experiment_name
        self.experiment_id = experiment_id
        self.run_name = run_name
        self.env_manager = env_manager
        self.callbacks = callbacks
        self._executor = TasksExecutor(backend_exec=backend_exec)

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

    def set_backend_exec(self, backend_exec: BackendExecutor) -> None:
        """Sets backend executor."""
        self._executor.set_backend_exec(backend_exec)

    def fit(self):
        """Fits estimator on the configured backend executor.

        Returns
        -------
        self : object
            This estimator.
        """
        self.promise_: TaskPromise = self._executor.execute(
            name=self.task_name,
            kwargs=self.get_kwargs(),
            callbacks=self.callbacks,
        )

        return self

    def deploy(self, mode: str = "local", config: dict = None):
        if config is None:
            config = {}

        run_id = self.run_.run_id
        model_uri = f"runs:/{run_id}/model"
        deployer: Deployer = get_deployer(mode, **config)
        return deployer.deploy(run_id, model_uri)
