import click

from .app import webapp


@click.group("web", help="Start and manage anyforecast web application.")
def commands():
    """Starts web server."""


@commands.command("start", help="Start web application.")
@click.option(
    "--host",
    "-h",
    type=str,
    envvar="WEB_HOST",
    default="127.0.0.1",
    help="The network address to listen on (default: 127.0.0.1). "
    "Use 0.0.0.0 to bind to all addresses if you want to access the tracking "
    "server from other machines.",
)
@click.option(
    "--port",
    "-p",
    type=int,
    envvar="WEB_PORT",
    default=80,
    help="The port to listen on (default: 80).",
)
@click.option(
    "--reload",
    "-r",
    type=bool,
    default=False,
)
def start(host, port, reload):
    webapp.run_server(host, port, reload)
