import json
import logging
import pathlib
import click
import geopandas as gpd

from fis.lock.core import load_data
from fis.bridge.core import group_bridge_complexes

logger = logging.getLogger(__name__)

CRS = "EPSG:4326"


def build_bridge_features(complexes):
    """
    Flatten hierarchical bridge complex objects into a list of GeoJSON features (Nodes and Edges).
    For now, this returns a list of dictionaries with 'geometry' and 'properties'.
    We will fully integrate the FairwaySplicer and routing graph later.
    """
    features = []

    for c in complexes:
        geom_wkt = c.get("geometry")
        if geom_wkt:
            features.append(
                {
                    "type": "Feature",
                    "geometry": geom_wkt,  # WKT text
                    "properties": {
                        "id": c.get("id"),
                        "name": c.get("name"),
                        "feature_type": "bridge",
                        "bridge_id": c.get("id"),
                    },
                }
            )

        for opening in c.get("openings", []):
            op_geom = (
                opening.get("Geometry_isrs")
                or opening.get("geometry")
                or opening.get("Geometry")
                or geom_wkt
            )
            if op_geom:
                op_id = opening.get("id")
                features.append(
                    {
                        "type": "Feature",
                        "geometry": op_geom,
                        "properties": {
                            "id": op_id,
                            "feature_type": "bridge_opening",
                            "bridge_id": c.get("id"),
                            "opening_id": op_id,
                            "dim_width": opening.get("dim_width"),
                            "dim_height": opening.get("dim_height"),
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


@click.group(name="bridge")
def cli():
    """Bridge graph commands."""
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

    output_dir.mkdir(parents=True, exist_ok=True)

    # Group complexes
    result = group_bridge_complexes(data)

    # Summary JSON (per-bridge metadata)
    output_json = output_dir / "summary.json"
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    logger.info("Saved summary to %s", output_json)

    def save_gdf(gdf: gpd.GeoDataFrame, name: str) -> None:
        if gdf is None or gdf.empty:
            raise ValueError(
                f"Output DataFrame for '{name}' is empty or None. Expected data to export."
            )

        # Ensure ID columns are strings to avoid Arrow conversion errors (mixed types)
        from fis.utils import load_schema, stringify_id

        schema = load_schema()
        id_cols = schema.get("identifiers", {}).get("columns", [])
        gdf = gdf.copy()
        for col in id_cols:
            if col in gdf.columns:
                gdf[col] = gdf[col].apply(stringify_id)

        gdf.to_file(output_dir / f"{name}.geojson", driver="GeoJSON")
        gdf.to_parquet(output_dir / f"{name}.geoparquet")
        logger.info("Saved %d %s features to %s", len(gdf), name, output_dir)

    save_gdf(build_bridges_gdf(result), "bridge")
    save_gdf(build_openings_gdf(result), "opening")
