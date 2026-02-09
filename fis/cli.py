"""Main FIS CLI entry point combining all subcommands."""

import logging

import click

from fis.graph.cli import cli as graph_cli
from fis.lock.cli import cli as lock_cli

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@click.group()
def cli():
    """FIS data processing pipeline."""
    pass


# Add subcommand groups
cli.add_command(graph_cli, name="graph")
cli.add_command(lock_cli, name="lock")


if __name__ == "__main__":
    cli()
