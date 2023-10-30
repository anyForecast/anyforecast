from .base import Deployer
from .docker import DockerDeployer
from .local import LocalDeployer


def get_deployer(name: str, **kwargs):
    deployers = {"docker": DockerDeployer, "local": LocalDeployer}
    return deployers[name](**kwargs)


__all__ = ["get_deployer", "Deployer"]
