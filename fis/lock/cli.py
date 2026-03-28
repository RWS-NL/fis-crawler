"""Lock schematization CLI commands."""

import json
import logging
import pathlib
import pickle

import click
import geopandas as gpd

from fis import utils
from fis.lock.core import group_complexes, load_data
from fis.lock.graph import (
    build_berths_gdf,
    build_chambers_gdf,
    build_edges_gdf,
    build_locks_gdf,
    build_nodes_gdf,
    build_subchambers_gdf,
)
from fis.utils import load_schema

logger = logging.getLogger(__name__)


class NumpyEncoder(json.JSONEncoder):
    """Custom encoder for numpy types."""

    def default(self, obj):
        import numpy as np

        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        return super().default(obj)


@click.group()
def cli():
    """Lock schematization commands."""
    pass


@cli.command()
@click.option(
    "--export-dir",
    type=click.Path(exists=True, path_type=pathlib.Path),
    default="output/fis-export",
    help="Directory containing input parquet files.",
)
@click.option(
    "--disk-dir",
    type=click.Path(path_type=pathlib.Path),
    default="output/disk-export",
    help="Directory containing exported DISK parquet files.",
)
@click.option(
    "--fis-graph",
    type=click.Path(path_type=pathlib.Path),
    default="output/fis-graph/graph.pickle",
    help="Path to fis-graph pickle file for topology matching.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=pathlib.Path),
    default="output/lock-schematization",
    help="Output directory.",
)
def schematize(
    export_dir: pathlib.Path,
    disk_dir: pathlib.Path,
    fis_graph: pathlib.Path,
    output_dir: pathlib.Path,
) -> None:
    """Process lock complexes into detailed graph features."""
    data = load_data(export_dir, disk_dir)

    # Load Network Graph for fairway connectivity
    network_graph = None
    if fis_graph and fis_graph.exists():
        with open(fis_graph, "rb") as f:
            network_graph = pickle.load(f)
        logger.info(
            "Loaded network graph with %d nodes", network_graph.number_of_nodes()
        )

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Group complexes
    result = group_complexes(data, network_graph)

    # Summary JSON (per-lock metadata)
    output_json = output_dir / "summary.json"
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, cls=NumpyEncoder)
    logger.info("Saved summary to %s", output_json)

    if not result:
        return

    def save_gdf(gdf: gpd.GeoDataFrame, name: str) -> None:
        """Save a GeoDataFrame as both GeoJSON and GeoParquet."""
        if gdf is None or gdf.empty:
            logger.warning(
                "Output DataFrame for '%s' is empty or None. Skipping export.", name
            )
            return

        # Ensure ID columns are strings to avoid Arrow conversion errors (mixed types)
        schema = load_schema()
        id_cols = schema["identifiers"]["columns"]

        gdf = gdf.copy()
        for col in id_cols:
            if col in gdf.columns:
                gdf[col] = gdf[col].apply(utils.stringify_id)

        gdf.to_file(output_dir / f"{name}.geojson", driver="GeoJSON")
        gdf.to_parquet(output_dir / f"{name}.geoparquet")
        logger.info("Saved %d %s features to %s", len(gdf), name, output_dir)

    save_gdf(build_locks_gdf(result), "lock")
    save_gdf(build_chambers_gdf(result), "chamber")
    save_gdf(build_subchambers_gdf(result), "subchamber")
    save_gdf(build_nodes_gdf(result), "nodes")
    save_gdf(build_edges_gdf(result), "edges")
    save_gdf(build_berths_gdf(result), "berths")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cli()
