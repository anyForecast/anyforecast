import click

import anyforecast.web.cli
from anyforecast import version


@click.group()
@click.version_option(version=version.VERSION)
def cli():
    pass


cli.add_command(anyforecast.web.cli.commands)
