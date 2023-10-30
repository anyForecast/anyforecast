from typing import Literal

import mlflow

from .base import Deployer
from .docker_utils import DockerRunner


class DockerDeployer(Deployer):
    """Deploys model in a Docker container.

    Parameters
    ----------
    container_name : str, default=None
        The name for the running container.

    port : int, default=8080
        Host's port to bind to the running container.

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
    ):
        self.container_name = container_name
        self.port = port
        self.environment = environment
        self.env_manager = env_manager

    def deploy(self, name: str, model_uri: str):
        self.build_image(name, model_uri)
        self.run_image(name)

    def build_image(self, name: str, model_uri: str):
        return mlflow.models.build_docker(
            model_uri=model_uri,
            name=name,
            env_manager=self.env_manager,
        )

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
