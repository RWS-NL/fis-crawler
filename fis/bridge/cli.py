"""Bridge schematization CLI commands."""

import json
import logging
import pathlib
from typing import List, Dict

import click
import geopandas as gpd

from fis.bridge.core import group_bridge_complexes
from fis.lock.core import load_data
from fis import utils

logger = logging.getLogger(__name__)

CRS = "EPSG:4326"


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


@click.group(name="bridge")
def cli():
    """Bridge graph commands."""
    pass


def build_bridge_features(complexes: List[Dict]) -> List[Dict]:
    """Convert bridge complexes into GeoJSON features."""
    features = []
    for c in complexes:
        # 1. Main Bridge Node/Polygon
        # Ensure it has an ID
        bid = c.get("id")
        if not bid:
            continue

        features.append(
            {
                "type": "Feature",
                "geometry": c.get("geometry"),
                "properties": {
                    "id": bid,
                    "feature_type": "bridge",
                    "name": c.get("name"),
                    "related_building_complex_name": c.get(
                        "related_building_complex_name"
                    ),
                },
            }
        )

        # 2. Openings
        for op in c.get("openings", []):
            op_id = op.get("id")
            if op_id:
                features.append(
                    {
                        "type": "Feature",
                        "geometry": op.get("geometry"),
                        "properties": op
                        | {
                            "feature_type": "bridge_opening",
                            "bridge_id": bid,
                        },
                    }
                )

    return features


def _geom_from_feature(f):
    from shapely import wkt

    geom = f.get("geometry")
    if isinstance(geom, str):
        return wkt.loads(geom)
    elif isinstance(geom, dict):
        from shapely.geometry import shape

        return shape(geom)
    return geom


def build_bridges_gdf(complexes) -> gpd.GeoDataFrame:
    features = build_bridge_features(complexes)
    rows = [
        f["properties"] | {"geometry": _geom_from_feature(f)}
        for f in features
        if f["properties"].get("feature_type") == "bridge"
    ]
    if not rows:
        return gpd.GeoDataFrame(columns=["id", "name", "geometry"], crs=CRS)
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)


def build_openings_gdf(complexes) -> gpd.GeoDataFrame:
    features = build_bridge_features(complexes)
    rows = [
        f["properties"] | {"geometry": _geom_from_feature(f)}
        for f in features
        if f["properties"].get("feature_type") == "bridge_opening"
    ]
    if not rows:
        return gpd.GeoDataFrame(
            columns=["id", "bridge_id", "dim_width", "dim_height", "geometry"], crs=CRS
        )
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)


@cli.command()
@click.option(
    "--export-dir",
    type=click.Path(path_type=pathlib.Path),
    default="output/fis-export",
    help="Directory containing FIS parquet exports.",
)
@click.option(
    "--disk-dir",
    type=click.Path(path_type=pathlib.Path),
    default="output/disk-export",
    help="Directory containing DISK parquet exports.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=pathlib.Path),
    default="output/bridge-schematization",
    help="Output directory.",
)
def schematize(
    export_dir: pathlib.Path,
    disk_dir: pathlib.Path,
    output_dir: pathlib.Path,
) -> None:
    """Process bridge complexes from FIS and DISK into structured outputs."""
    data = load_data(export_dir, disk_dir)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Group complexes
    result = group_bridge_complexes(data)

    # Summary JSON (per-bridge metadata)
    output_json = output_dir / "summary.json"
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, cls=NumpyEncoder)
    logger.info("Saved summary to %s", output_json)

    def save_gdf(gdf: gpd.GeoDataFrame, name: str) -> None:
        if gdf is None or gdf.empty:
            logger.warning("No features for %s, skipping.", name)
            return

        # Ensure ID columns are strings to avoid Arrow conversion errors (mixed types)
        from fis.utils import load_schema

        schema = load_schema()
        id_cols = schema.get("identifiers", {}).get("columns", [])
        gdf = gdf.copy()
        for col in id_cols:
            if col in gdf.columns:
                gdf[col] = gdf[col].apply(utils.stringify_id)

        gdf.to_file(output_dir / f"{name}.geojson", driver="GeoJSON")
        gdf.to_parquet(output_dir / f"{name}.geoparquet")
        logger.info("Saved %d %s features to %s", len(gdf), name, output_dir)

    save_gdf(build_bridges_gdf(result), "bridge")
    save_gdf(build_openings_gdf(result), "opening")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cli()
