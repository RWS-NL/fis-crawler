"""Lock schematization CLI commands."""

import json
import logging
import pathlib
import sys

import click
import geopandas as gpd

from fis.ris_index import load_ris_index
from fis.lock.core import load_data, group_complexes
from fis.lock.graph import build_graph_features

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
    "--output-dir",
    type=click.Path(path_type=pathlib.Path),
    default="output/lock-schematization",
    help="Output directory.",
)
def schematize(export_dir: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Process lock complexes into detailed graph features."""
    try:
        locks, chambers, isrs, fairways, berths, sections = load_data(export_dir)
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

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Group complexes
    result = group_complexes(locks, chambers, isrs, ris_df, fairways, berths, sections)

    # JSON output
    output_json = output_dir / "lock_schematization.json"
    with open(output_json, "w") as f:
        json.dump(result, f, indent=2)
    logger.info("Saved JSON to %s", output_json)

    if not result:
        return

    # Generate GeoDataFrame
    features = build_graph_features(result)
    gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")

    # Enforce integer types
    for col in ["fairway_id", "lock_id", "chamber_id", "berth_id"]:
        if col in gdf.columns:
            gdf[col] = gdf[col].astype("Int64")

    # Save outputs
    gdf.to_file(output_dir / "lock_schematization.geojson", driver="GeoJSON")
    gdf.to_parquet(output_dir / "lock_schematization.geoparquet")
    logger.info("Saved GeoJSON and GeoParquet to %s", output_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cli()
