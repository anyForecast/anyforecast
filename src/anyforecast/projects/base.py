import abc
import os
from typing import Any, Literal, TypedDict

from anyforecast import backend, callbacks, definitions, execution


def get_script_uri(name: str):
    """Returns script complete uri.

    Parameters
    ----------
    name : str
        Script name.
    """
    endpoint = f"#anyforecast_scripts/{name}"
    return os.path.join(definitions.SCRIPTS_REPO, endpoint)


class RunProjectSignature(TypedDict):
    uri: str | None
    entry_point: str
    parameters: dict[str, Any] | None
    experiment_name: str | None
    experiment_id: str | None
    storage_dir: str | None
    run_name: str | None
    env_manager: Literal["local", "virtualenv", "conda"] | None
    environment: dict[str, Any] | None


class MLflowProject(abc.ABC):
    """Handles training and deployment of MLflow projects.

    Parameters
    ----------
    uri : str
        MLflow project URI.

    Attributes
    ----------
    promise_ : TaskPromise
    """

    #: Name of the task to be executed.
    task_name: str = "anyforecast.tasks.mlflow.run_mlflow"

    def __init__(self, uri: str) -> None:
        self.uri = uri

    @abc.abstractmethod
    def get_parameters(self) -> dict[str, Any]:
        """Returns MLflow project parameters."""
        pass

    def run(
        self,
        input_channels: dict[str, str],
        callbacks: list[callbacks.Callback] = (),
        backend: backend.BackendExecutor = backend.LocalBackend(),
        entry_point: str = "main",
        experiment_name: str | None = None,
        experiment_id: str | None = None,
        storage_dir: str | None = None,
        run_name: str | None = None,
        env_manager: Literal["local", "virtualenv", "conda"] | None = None,
        environment: dict | None = None,
    ):
        """Runs project on the configured backend executor.

        Parameters
        ----------
        input_channels : dict, str -> str
            Mapping from input channel name to its filepath.
            Allowed input channels names are "train", "val" and "test".

        callbacks : list of callbacks.Callback, default=()
            Which callbacks to enable.

        backend : backend.BackendExecutor, default=LocalExecutor()
            Backend executor. Default local.

        entry_point : str, default="main"
            MLflow project entrypoint.

        experiment_name : str, default=None
            MLflow experiment name.

        experiment_id : str, default=None
            MLflow experiment id.

        storage_dir : str, default=None
            pass


        enviroment : dict
            Enviroment variables to set for the run.

        Returns
        -------
        self : object
            This estimator.
        """
        self.check_input_channels(input_channels)

        environment = environment or {}
        environment.update(input_channels)

        kwargs = RunProjectSignature(
            uri=self.uri,
            entry_point=entry_point,
            parameters=self.get_parameters(),
            environment=environment,
            experiment_name=experiment_name,
            experiment_id=experiment_id,
            storage_dir=storage_dir,
            env_manager=env_manager,
            run_name=run_name,
        )

        self.promise_ = execution.TasksExecutor(backend).execute(
            name=self.task_name,
            kwargs=kwargs,
            callbacks=callbacks,
        )

        return self

    def check_input_channels(self, input_channels: dict[str, str]) -> None:
        """Checks inpunt channel names/keys.

        Parameters
        ----------
        input_channels : dict, str -> str
            Input channels allowed keys are "train", "val", "test"
        """
        allowed_channels = ["train", "val", "test"]

        for channel in input_channels:
            if channel not in allowed_channels:
                raise ValueError(
                    f"Input channel name: {channel} is not allowed. "
                    f"Allowed names: {allowed_channels}."
                )
