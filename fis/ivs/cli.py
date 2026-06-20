import pathlib
import click
from .process import process_ivs_data


@click.group()
def cli():
    """IVS Shipping Data tools."""
    pass


@cli.command()
@click.option(
    "--downloads-dir",
    type=click.Path(
        exists=True, file_okay=False, dir_okay=True, path_type=pathlib.Path
    ),
    default="/scratch-shared/fbaart/data/ivs/downloads",
    help="Path to directory containing weekmonitor ZIP files.",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path),
    default="/scratch-shared/fbaart/data/ivs/partitioned",
    help="Path to output directory for partitioned Parquet files.",
)
def process(downloads_dir: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Process downloaded IVS weekmonitor ZIP files into partitioned Parquet files."""
    click.echo("Starting IVS data processing...")
    click.echo(f"Downloads directory: {downloads_dir}")
    click.echo(f"Output directory:    {output_dir}")
    process_ivs_data(downloads_dir, output_dir)
    click.echo("IVS data processing finished successfully.")
