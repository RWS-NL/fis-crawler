import click
import pathlib
import logging

from fis.dropins.core import build_integrated_dropins_graph

logger = logging.getLogger(__name__)


@click.group(name="dropins")
def dropins_cli():
    """Tools for integrated drop-ins in FIS."""
    pass


@dropins_cli.command()
@click.option(
    "--export-dir",
    type=click.Path(exists=True, path_type=pathlib.Path),
    default="output/fis-export",
)
@click.option(
    "--disk-dir",
    type=click.Path(exists=True, path_type=pathlib.Path),
    default="output/disk-export",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=pathlib.Path),
    default="output/dropins-schematization",
)
@click.option(
    "--bbox",
    type=str,
    help="Bounding box to filter by coordinates in 'minx,miny,maxx,maxy' (EPSG:4326). Useful for fast iteration.",
    default=None,
)
@click.option(
    "--mode",
    type=click.Choice(["detailed", "coarse"]),
    default="detailed",
    help="Level of detail for the schematization. 'detailed' (default) preserves lock-associated subgraphs; 'coarse' (simplified) represents all bridges and locks as single edges.",
)
@click.option(
    "--include-berths/--no-berths",
    default=False,
    help="Include berths as nodes and access edges in the graph. Caution: This adds many extra edges.",
)
def schematize(export_dir, disk_dir, output_dir, bbox, mode, include_berths):
    """Integrate locks and bridges into fairways globally."""
    bbox_tuple = None
    if bbox:
        try:
            bbox_tuple = tuple(map(float, bbox.split(",")))
            if len(bbox_tuple) != 4:
                raise ValueError
        except Exception:
            raise click.BadParameter("BBox must be 'minx,miny,maxx,maxy'")

    build_integrated_dropins_graph(
        export_dir,
        disk_dir,
        output_dir,
        bbox=bbox_tuple,
        mode=mode,
        include_berths=include_berths,
    )
