from typing import Literal

import mlflow

from .docker_utils import DockerRunner


class DockerDeployer:
    def create_deployment(
        self,
        name: str,
        model_uri: str,
        container_name: str | None = None,
        env_manager: Literal["local", "virtualenv", "conda"] = "virtualenv",
        port: int = 8080,
        environment: dict[str, str] = None,
    ) -> None:
        """Deploys model locally in a Docker container.

        Builds a Docker image whose default entrypoint serves an MLflow model
        at the specified ``port``. The container serves the model referenced by
        ``model-uri``.

        Parameters
        ----------
        name: str
            Unique name to use for deployment. Also used as model image when
            built.

        model_uri: str
            URI of model to deploy

        container_name, str, default=None
            If None, value in ``name`` will be used.

        env_manager : str, {"local", "virtualenv", "conda"},
        default="virtualenv"
            Create an environment for MLmodel using the specified environment
            manager.

        port : int, default=8080
            Port in the host machine to map.

        environment: dict, default=None
            Environment variables to set inside the container.
        """
        container_name = container_name or name
        flavor_backend = self.build_image(name, model_uri, env_manager)
        self.run_image(
            image_uri=name,
            container_name=container_name,
            environment=environment,
            port=port,
        )

    def run_image(
        self,
        image_uri: str,
        container_name: str,
        environment: dict[str, str] | None,
        port: int,
    ) -> None:
        runner = DockerRunner(
            image_uri,
            name=container_name,
            environment=environment,
            ports={"8080/tcp": port},
        )

        runner.run()

    def build_image(
        self,
        name: str,
        model_uri: str,
        env_manager: str = "virtualenv",
        mlflow_home: str | None = None,
        install_mlflow: bool = False,
        enable_mlserver: bool = False,
    ):
        """Builds MLFlow Docker image.

        For more details see:
        https://mlflow.org/docs/latest/cli.html#mlflow-models
        """
        return mlflow.models.build_docker(
            model_uri=model_uri,
            name=name,
            env_manager=env_manager,
            mlflow_home=mlflow_home,
            install_mlflow=install_mlflow,
            enable_mlserver=enable_mlserver,
        )
