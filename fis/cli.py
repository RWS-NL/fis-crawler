"""Main FIS CLI entry point combining all subcommands."""

import logging

import click

from fis.graph.cli import cli as graph_cli
from fis.lock.cli import cli as lock_cli
from fis.bridge.cli import cli as bridge_cli
from fis.dropins.cli import dropins_cli
from fis.publish.cli import publish_cli

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug logging.")
def cli(debug):
    """FIS data processing pipeline."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    pass


# Add subcommand groups
cli.add_command(graph_cli, name="graph")
cli.add_command(lock_cli, name="lock")
cli.add_command(bridge_cli, name="bridge")
cli.add_command(dropins_cli, name="dropins")
cli.add_command(publish_cli, name="publish")


if __name__ == "__main__":
    cli()
