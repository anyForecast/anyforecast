from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import docker


@dataclass
class PythonCommand:
    """Represents a python command.

    Parameters
    ----------
    script : name
        Python script to invocate.

    args : dict
        Python scripts arguments.

    name : str, default="python"
        Python command name.
    """

    script: str
    args: dict | None = None
    name: str = "python"

    def list(self) -> list[str]:
        args = self.args or {}
        args_list = [f"--{k}={v}" for k, v in args.items()]
        return [self.name, self.script] + args_list


class DockerRunner:
    """Runs Docker containers.

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
    """

    def __init__(
        self,
        image_uri: str,
        command: str | list[str] | None = None,
        environment: dict[str, str] | None = None,
        entrypoint: str | list[str] | None = None,
        volumes: list[str] | dict[str, str] | None = None,
        ports: dict | None = None,
        name: str | None = None,
        detach: bool = False,
    ) -> None:
        self.image_uri = image_uri
        self.command = command
        self.environment = environment
        self.entrypoint = entrypoint
        self.volumes = volumes
        self.ports = ports
        self.name = name
        self.detach = detach

        #: Actual Docker Python client.
        self.docker_client = docker.from_env()

    def run(self) -> None | docker.models.containers.Container:
        """Runs container.

        Returns
        -------
        If detach is True, a Container object is returned instead else None.
        """
        return self.docker_client.containers.run(
            image=self.image_uri,
            command=self.command,
            entrypoint=self.entrypoint,
            environment=self.environment,
            volumes=self.volumes,
            ports=self.ports,
            name=self.name,
            detach=self.detach,
        )


class DockerScriptRunner:
    """Runs Python scripts inside Docker containers.

    Parameters
    ----------
    script: str
        Path to the python script

    image_uri : str
        Docker image to run.

    environment : dict
        Environment variables to set inside the container.

    args : dict
        Script args. These are passed to the script as arguments.
    """

    #: Script volume to be mounted in the container.
    SCRIPT_VOLUME = "{script}:/tmp/{script}"

    def __init__(
        self,
        script: str,
        image_uri: str,
        environment: dict[str, str] | None = None,
        args: dict[str, Any] | None = None,
    ) -> None:
        self.script = script
        self.image_uri = image_uri
        self.environment = environment
        self.args = args

    @property
    def script_volume(self) -> str:
        return self.SCRIPT_VOLUME.format(script=self.script)

    def run(self, **kwargs) -> None:
        """Runs script inside Docker container.

        Parameters
        ----------
        kwargs : keyword args
            Additonal arguments to pass to :meth:`docker.client.containers.run`.
        """
        docker_runner = self.create_docker_runner()
        docker_runner.run(**kwargs)

    def create_docker_runner(self) -> DockerRunner:
        """Creates Docker runner.

        Returns
        -------
        runner : DockerRunner
        """
        volumes = [self.script_volume]
        command = self.create_command().list()
        return DockerRunner(
            image_uri=self.image_uri,
            command=command,
            environment=self.environment,
            volumes=volumes,
        )

    def create_command(self) -> PythonCommand:
        # Script path inside container.
        script = self.script_volume.split(":")[-1]
        return PythonCommand(script=script, args=self.args)
