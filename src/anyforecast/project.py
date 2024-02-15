import abc
from typing import Any, Literal, TypedDict

from anyforecast import (
    backends,
    callbacks,
    deployer,
    execution,
    predictor,
    settings,
)

scripts_settings = settings.conf.get_scripts_settings()


def create_script_uri(name: str) -> str:
    """Returns script complete uri.

    Parameters
    ----------
    name : str
        Script name.
    """
    return scripts_settings.create_uri(name)


class RunProjectSignature(TypedDict):
    uri: str | None
    entry_point: str
    version: str | None
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

    def is_run(self) -> bool:
        """Returns True if project has been run.

        Returns
        -------
        is_run : bool
        """
        return hasattr(self, "promise_")

    def check_is_run(self) -> None:
        """Checks project has been run.

        Raises
        ------
        ValueError if project has not been run.
        """
        if not self.is_run():
            raise ValueError(
                "This instance is not run yet. Call 'run' with "
                "appropriate arguments before using it."
            )

    def run(
        self,
        uri: str,
        parameters: dict[str, Any],
        input_channels: dict[str, str],
        callbacks: list[callbacks.Callback] = (),
        backend: backends.BackendExecutor = backends.LocalBackend(),
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
            MLflow storage dir.

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
            uri=uri,
            entry_point=entry_point,
            parameters=parameters,
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

    def deploy(self, deployer: deployer.Deployer) -> predictor.Predictor:
        self.check_is_run()


        self.promise_.result()

        self.promise_.result()
        return deployer.deploy()
        pass

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
