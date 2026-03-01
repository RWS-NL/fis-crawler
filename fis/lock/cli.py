"""Lock schematization CLI commands."""

import json
import logging
import pathlib
import sys

import click
import geopandas as gpd

from fis.ris_index import load_ris_index
from fis.lock.core import load_data, group_complexes

from fis.lock.graph import (
    build_nodes_gdf,
    build_edges_gdf,
    build_berths_gdf,
    build_locks_gdf,
    build_chambers_gdf,
    build_subchambers_gdf,
)

logger = logging.getLogger(__name__)


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
    export_dir: pathlib.Path, fis_graph: pathlib.Path, output_dir: pathlib.Path
) -> None:
    """Process lock complexes into detailed graph features."""
    try:
        locks, chambers, subchambers, isrs, fairways, berths, sections = load_data(
            export_dir
        )
    except FileNotFoundError:
        logger.exception("Failed to load data from %s", export_dir)
        sys.exit(1)

    # Load RIS Index
    ris_df = None
    ris_path = export_dir / "RisIndexNL.xlsx"
    if ris_path.exists():
        try:
            ris_df = load_ris_index(ris_path)
            logger.info("Loaded %d RIS Index entries", len(ris_df))
        except Exception as e:
            logger.warning("Could not load RIS Index: %s", e)

    # Load Network Graph for fairway connectivity
    import pickle

    network_graph = None
    if fis_graph and fis_graph.exists():
        try:
            with open(fis_graph, "rb") as f:
                network_graph = pickle.load(f)
            logger.info(
                "Loaded network graph with %d nodes", network_graph.number_of_nodes()
            )
        except Exception as e:
            logger.warning("Could not load fis-graph: %s", e)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Group complexes
    result = group_complexes(
        locks,
        chambers,
        subchambers,
        isrs,
        ris_df,
        fairways,
        berths,
        sections,
        network_graph,
    )

    # Summary JSON (per-lock metadata)
    output_json = output_dir / "summary.json"
    with open(output_json, "w") as f:
        json.dump(result, f, indent=2)
    logger.info("Saved summary to %s", output_json)

    if not result:
        return

    def save_gdf(gdf: gpd.GeoDataFrame, name: str) -> None:
        """Save a GeoDataFrame as both GeoJSON and GeoParquet."""
        if gdf.empty:
            logger.warning("No features for %s, skipping.", name)
            return
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
