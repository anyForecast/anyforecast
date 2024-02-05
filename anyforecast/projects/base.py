import abc
import dataclasses
from typing import Any, TypedDict

from anyforecast.backend import BackendExecutor, LocalBackend
from anyforecast.callbacks import Callback
from anyforecast.deployments import Deployer, get_deployer
from anyforecast.execution import TaskPromise, TasksExecutor


@dataclasses.dataclass
class TrackingParams:
    """MLflow tracking parameters.

    Parameters
    ----------
    run_name : str, default=None
        The name to give the MLflow Run associated with the project execution.
        If None, a unique name will be generated for the run.

    tags : dict, default=None
        An optional dictionary of string keys and values to set as tags on the
        run.

    description : str, default=None
        An optional string that populates the description box of the run.
    """

    run_name: str | None = None
    tags: dict[str, str] | None = None
    description: str | None = None


class TaskKwargs(TypedDict):
    """Task kwargs. Used for type hints."""

    uri: str
    entry_point: str
    parameters: dict[str, str]
    run_name: str
    env_vars: dict[str, str]


class MLFlowProject(abc.ABC):
    """Handles training and deployment of MLflow projects.

    Parameters
    ----------


    entry_point : str, default="main"
        Entry point to run within the project.

    callbacks : list of Callback instances, default=()
        Which callbacks to enable.


    Attributes
    ----------
    promise_ : TaskPromise
    """

    #: Name of the task to be executed.
    task_name: str = "anyforecast.tasks.mlflow.run_mlflow"

    def __init__(
        self,
        uri: str,
        model_params: dict,
        tracking_params: TrackingParams | None = None,
        entry_point: str = "main",
        callbacks: list[Callback] = (),
        backend_exec: BackendExecutor = LocalBackend(),
        env_vars: dict[str, Any] | None = None,
    ):
        self.uri = uri
        self.model_params = model_params
        self.tracking_params = TrackingParams() or tracking_params
        self.entry_point = entry_point
        self.callbacks = callbacks
        self.env_vars = env_vars or {}

        self._executor = TasksExecutor(backend_exec=backend_exec)

    def run(self, input_channels: dict[str, str]):
        """Runs project on the configured backend executor.

        Parameters
        ----------
        input_channels : dict, str -> str
            Mapping from input channel name to its filepath.
            Allowed input channels names are "train", "val" and "test".

        Returns
        -------
        self : object
            This estimator.
        """
        self.check_input_channels(input_channels)

        kwargs = self.get_task_kwargs()
        kwargs["env_vars"].update(input_channels)

        self.promise_: TaskPromise = self._executor.execute(
            name=self.task_name,
            kwargs=kwargs,
            callbacks=self.callbacks,
        )

    def check_input_channels(self, input_channels: dict[str, str]):
        """Checks inpunt channel names/keys.

        Parameters
        ----------
        input_channels : dict, str -> str
            Input channels allowed keys are "train", "val", "test"
        """
        allowed_input_channels = ["train", "val", "test"]

        for channel in input_channels:
            if channel not in allowed_input_channels:
                raise ValueError(
                    f"Input channel name: {channel} is not allowed. "
                    f"Allowed names: {allowed_input_channels}."
                )

    def get_task_kwargs(self) -> TaskKwargs:
        return TaskKwargs(
            uri=self.uri,
            entry_point=self.entry_point,
            parameters=self.get_parameters(),
            run_name=self.tracking_params.run_name,
            env_vars=self.env_vars,
        )

    def get_parameters(self) -> dict:
        if dataclasses.is_dataclass(self.model_params):
            return dataclasses.asdict(self.model_params)

        return self.model_params

    def set_backend(self, backend: BackendExecutor) -> None:
        """Sets current backend executor.

        Parameters
        ----------
        backend: BackendExecutor
            Backend executor.
        """
        self._executor.set_backend_exec(backend)

    def deploy(self, mode: str = "local", config: dict = None):
        if config is None:
            config = {}

        run_id = self.run_.run_id
        model_uri = f"runs:/{run_id}/model"
        deployer: Deployer = get_deployer(mode, **config)
        return deployer.deploy(run_id, model_uri)
