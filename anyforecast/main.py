import logging

import click

from anyforecast import AnyForecast, version
from anyforecast.executors import get_executor

log = logging.getLogger(__name__)


@click.group()
@click.version_option(version=version.VERSION)
def cli():
    pass


app = AnyForecast()


@cli.command()
@click.option("--executor", "-e", default="local", help="Executor to use.")
def start(executor):
    """Start AnyForecast app."""
    executor = get_executor(executor)
    app.set_executor(executor)
    app.start()


if __name__ == "__main__":
    cli()
