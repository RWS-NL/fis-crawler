import pathlib
import click
from .process import process_ivs_data
from .assign import assign_traffic


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
    default="output/ivs-downloads",
    help="Path to directory containing weekmonitor ZIP files.",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path),
    default="output/ivs-partitioned",
    help="Path to output directory for partitioned Parquet files.",
)
def process(downloads_dir: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Process downloaded IVS weekmonitor ZIP files into partitioned Parquet files."""
    click.echo("Starting IVS data processing...")
    click.echo(f"Downloads directory: {downloads_dir}")
    click.echo(f"Output directory:    {output_dir}")
    process_ivs_data(downloads_dir, output_dir)
    click.echo("IVS data processing finished successfully.")


@cli.command()
@click.option(
    "--graph-path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=pathlib.Path),
    default="output/dropins-fis-detailed/graph.pickle",
    help="Path to the detailed dropins graph pickle file.",
)
@click.option(
    "--base-graph",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=pathlib.Path),
    default="output/merged-graph/graph.pickle",
    help="Path to the base merged graph pickle file.",
)
@click.option(
    "--ivs-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=pathlib.Path),
    default="/scratch-shared/fbaart/data/ivs/partitioned",
    help="Path to partitioned IVS parquet files.",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, dir_okay=True, path_type=pathlib.Path),
    default="output/ivs-assignment",
    help="Path to output directory for traffic assignment datasets.",
)
@click.option(
    "--year",
    type=int,
    default=2024,
    help="Year of the IVS voyages to route.",
)
@click.option(
    "--month",
    type=int,
    default=1,
    help="Month of the IVS voyages to route.",
)
def assign(
    graph_path: pathlib.Path,
    base_graph: pathlib.Path,
    ivs_dir: pathlib.Path,
    output_dir: pathlib.Path,
    year: int,
    month: int
) -> None:
    """Assign IVS voyages to the detailed waterway network with optimal routing."""
    # Resolve correct default for ivs_dir if fbaart scratch is used
    if not ivs_dir.exists() and pathlib.Path("/scratch-shared/fbaart/data/ivs/partitioned").exists():
        ivs_dir = pathlib.Path("/scratch-shared/fbaart/data/ivs/partitioned")
        
    click.echo("Starting IVS traffic assignment routing...")
    click.echo(f"Detailed Graph: {graph_path}")
    click.echo(f"Base Graph:     {base_graph}")
    click.echo(f"IVS Directory:  {ivs_dir}")
    click.echo(f"Output Dir:     {output_dir}")
    click.echo(f"Period:         {year}-{month:02d}")
    
    assign_traffic(graph_path, base_graph, ivs_dir, output_dir, year, month)
    click.echo("IVS traffic assignment finished successfully.")

