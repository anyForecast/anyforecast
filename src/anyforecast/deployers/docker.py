from typing import Literal

import docker
import mlflow

from anyforecast import deployer


def build_mlflow_image(
    model_uri: str | None = None,
    name: str = "mlflow-pyfunc",
    env_manager: Literal["local", "virtualenv", "conda"] = "local",
    mlflow_home: str | None = None,
    install_mlflow: bool = False,
    enable_mlserver: bool = False,
) -> None:
    """Builds a Docker image whose default entrypoint serves an MLflow model.

    Parameters
    ----------
    """
    mlflow.models.build_docker(
        model_uri=model_uri,
        name=name,
        env_manager=env_manager,
        mlflow_home=mlflow_home,
        install_mlflow=install_mlflow,
        enable_mlserver=enable_mlserver,
    )


def run_container(
    image_uri: str,
    command: str | list[str] | None = None,
    environment: dict[str, str] | None = None,
    entrypoint: str | list[str] | None = None,
    volumes: list[str] | dict[str, str] | None = None,
    ports: dict | None = None,
    name: str | None = None,
    detach: bool = False,
) -> None | docker.models.containers.Container:
    """Runs Docker container.

    Parameters
    ----------
    image_uri : str
        The image to run.

    command : str, default=None
        The command to run in the container.

    environment : dict, default=None
        Environment variables to set inside the container, as a dictionary.

    entrypoint : str or list of str, default=None
        The entrypoint for the container

    volumes : list of str or dict, default=None
        A dictionary to configure volumes mounted inside the container.
        Or a list of strings which each one of its elements specifies a mount
        volume.

    name : str, default=None
        Name of the container.

    detach : bool, default=False
        Run container in the background and return a Container object.

    Returns
    -------
    If detach is True, a Container object is returned instead else None.
    """
    docker_client = docker.from_env()
    return docker_client.containers.run(
        image=image_uri,
        command=command,
        entrypoint=entrypoint,
        environment=environment,
        volumes=volumes,
        ports=ports,
        name=name,
        detach=detach,
    )


class DockerDeployer(deployer.Deployer):
    """Deploys model in a Docker container.

    Parameters
    ----------
    container_name : str, default=None
        The name for the running container.

    port : int, default=8080
        Host port to bind to the running container.

    environment : dict, default=None
        Environment variables to set inside the container.

    env_manager : str, default="local", {"local", "virtualenv", "conda"}
        If specified, create an environment for MLmodel using the specified
        environment manager.
    """

    def __init__(
        self,
        container_name: str | None = None,
        port: int = 8080,
        environment: dict[str, str] | None = None,
        env_manager: Literal["local", "virtualenv", "conda"] = "local",
        mlflow_home: str | None = None,
        install_mlflow: bool = False,
        enable_mlserver: bool = False,
    ):
        self.container_name = container_name
        self.port = port
        self.environment = environment
        self.env_manager = env_manager
        self.mlflow_home = mlflow_home
        self.install_mlflow = install_mlflow
        self.enable_mlserver = enable_mlserver

    def deploy(self, model_uri: str):

        build_mlflow_image(
            model_uri=model_uri,
            env_manager=self.env_manager,
            mlflow_home=self.mlflow_home,
            install_mlflow=self.install_mlflow,
            enable_mlserver=self.enable_mlserver,
        )

        run_container(image_uri)

    def run_image(self, image_uri: str):
        container_name = image_uri or self.container_name
        ports = {"8080/tcp": self.port}
        runner = DockerRunner(
            image_uri=image_uri,
            environment=self.environment,
            ports=ports,
            name=container_name,
            detach=True,
        )
        runner.run()
