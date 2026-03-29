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
    type=click.Path(path_type=pathlib.Path),
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
    type=click.Choice(["detailed", "simplified"]),
    default="detailed",
    help="Level of detail for the schematization. 'detailed' (default) preserves all structure-associated subgraphs; 'simplified' represents all bridges and locks as single edges.",
)
@click.option(
    "--include-berths/--no-berths",
    default=False,
    help="Include berths as nodes and access edges in the graph. Caution: This adds many extra edges.",
)
@click.option(
    "--source",
    type=click.Choice(["fis", "euris"]),
    default="fis",
    help="Data source to use for schematization. 'fis' (default) uses Dutch FIS/DISK data; 'euris' uses European data.",
)
def schematize(export_dir, disk_dir, output_dir, bbox, mode, include_berths, source):
    """Integrate locks and bridges into fairways globally."""
    bbox_tuple = None
    if bbox:
        try:
            bbox_tuple = tuple(map(float, bbox.split(",")))
            if len(bbox_tuple) != 4:
                raise ValueError
        except Exception:
            raise click.BadParameter("BBox must be 'minx,miny,maxx,maxy'")

    from fis.dropins.io import load_dropins_with_spatial_matching
    from fis.dropins.euris_io import load_dropins_with_explicit_linking

    if source == "euris":
        (
            lock_complexes,
            bridge_complexes,
            terminals,
            berths,
            sections,
            openings,
        ) = load_dropins_with_explicit_linking(export_dir, bbox=bbox_tuple)
    else:
        if disk_dir is None or not disk_dir.exists():
            raise click.BadParameter(
                f"Disk directory '{disk_dir}' does not exist. It is required for source='fis'.",
                param_hint="--disk-dir",
            )
        (
            lock_complexes,
            bridge_complexes,
            terminals,
            berths,
            sections,
            openings,
        ) = load_dropins_with_spatial_matching(export_dir, disk_dir, bbox=bbox_tuple)

    build_integrated_dropins_graph(
        lock_complexes,
        bridge_complexes,
        terminals,
        berths,
        sections,
        openings,
        output_dir,
        mode=mode,
        include_berths=include_berths,
    )
