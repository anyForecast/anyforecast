from .docker import DockerDeployer


def get_deployer(name: str, *args, **kwargs):
    deployers = {"docker": DockerDeployer}

    return deployers[name](*args, **kwargs)


__all__ = ["get_deployer"]
