import multiprocessing
from typing import Literal

from mlflow.models.flavor_backend_registry import get_flavor_backend

from .base import Deployer


def run_mlflow_server(
    model_uri, env_manager, workers, port, host, timeout, enable_mlserver
):
    flavor_backend = get_flavor_backend(
        model_uri,
        env_manager=env_manager,
        workers=workers,
    )
    flavor_backend.serve(
        model_uri=model_uri,
        port=port,
        host=host,
        timeout=timeout,
        enable_mlserver=enable_mlserver,
    )


class LocalDeployer(Deployer):
    def __init__(
        self,
        port: int = 8080,
        host: str = "127.0.0.1",
        timeout: int = 60,
        workers: int = 1,
        env_manager: Literal["local", "virtualenv", "conda"] = "local",
        enable_mlserver: bool = False,
    ):
        self.port = port
        self.host = host
        self.timeout = timeout
        self.workers = workers
        self.env_manager = env_manager
        self.enable_mlserver = enable_mlserver

    def deploy(self, name: str, model_uri: str):
        kwargs = {
            "model_uri": model_uri,
            "env_manager": self.env_manager,
            "workers": self.workers,
            "port": self.port,
            "host": self.host,
            "timeout": self.timeout,
            "enable_mlserver": self.enable_mlserver,
        }

        process = multiprocessing.Process(
            target=run_mlflow_server, name=name, kwargs=kwargs
        )

        return process.start()
