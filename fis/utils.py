import logging
import pathlib
import tomllib
from typing import Dict, Any

import pandas as pd
import numpy as np
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point
from shapely.ops import nearest_points, substring
from pyproj import Geod
from fis import settings

logger = logging.getLogger(__name__)


def load_schema(
    config_path: pathlib.Path = pathlib.Path("config/schema.toml"),
) -> Dict[str, Any]:
    """Load schema configuration from TOML file."""
    if not config_path.exists():
        # Fallback for when running from subdirectories
        root_path = pathlib.Path(__file__).parent.parent / "config" / "schema.toml"
        if root_path.exists():
            config_path = root_path

    with open(config_path, "rb") as f:
        return tomllib.load(f)


def camel_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case."""
    import re

    # Skip geometry columns
    if name.lower() == "geometry":
        return "geometry"

    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-0])([A-Z])", r"\1_\2", s1).lower()


def to_python(obj):
    """Recursively convert numpy/pandas types to plain Python for JSON serialization."""
    if isinstance(obj, np.ndarray):
        return [to_python(v) for v in obj.tolist()]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, dict):
        return {k: to_python(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_python(v) for v in obj]
    return obj


def sanitize_attrs(row_obj):
    """Clean row values into pure Python JSON-serializable types, skipping geometry and nested objects."""
    from shapely.geometry.base import BaseGeometry

    attrs = {}
    for k, v in row_obj.items():
        if k == "geometry":
            continue
        if isinstance(v, (list, dict, np.ndarray)):
            continue
        if pd.isna(v):
            attrs[k] = None
        elif isinstance(v, BaseGeometry):
            attrs[k] = v.wkt
        elif hasattr(v, "isoformat"):
            attrs[k] = v.isoformat()
        else:
            attrs[k] = to_python(v)
    geom = row_obj.get("geometry")
    if geom is not None:
        attrs["geometry"] = geom.wkt if hasattr(geom, "wkt") else str(geom)
    return attrs


def stringify_id(val):
    """
    Standardize ID values as clean strings.
    Handles float-like strings ("123.0" -> "123") and numeric types.
    Returns None for NaN values.
    """
    if pd.isna(val) or val is None:
        return None

    # If it's already a string, check if it's a "float-string" like "123.0"
    if isinstance(val, str):
        if val.lower() == "nan":
            return None
        try:
            # Attempt conversion to see if it's numeric
            f_val = float(val)
            if f_val.is_integer():
                return str(int(f_val))
            return str(f_val)
        except ValueError:
            return val

    # Handle numeric types (float, int, np.integer, etc.)
    try:
        f_val = float(val)
        if not np.isfinite(f_val):
            return None
        if f_val.is_integer():
            return str(int(f_val))
        return str(val)
    except (ValueError, TypeError):
        return str(val)


def normalize_attributes(
    df: pd.DataFrame, schema_section: str, schema: Dict[str, Any] = None
) -> pd.DataFrame:
    """
    Normalize DataFrame columns based on schema mappings.
    Any columns not in the explicit schema are converted from CamelCase to snake_case.

    Args:
        df: Input DataFrame or GeoDataFrame.
        schema_section: Key in [attributes] (e.g. 'locks', 'chambers').
        schema: Optional pre-loaded schema dict.

    Returns:
        DataFrame with renamed columns.
    """
    if df is None or df.empty:
        return df

    if schema is None:
        schema = load_schema()

    mappings = schema.get("attributes", {}).get(schema_section, {})

    # 1. Start with automatic snake_case renaming for ALL columns
    rename_map = {col: camel_to_snake(col) for col in df.columns}

    # 2. Apply explicit overrides from schema.toml (highest priority)
    for k, v in mappings.items():
        if k in df.columns:
            rename_map[k] = v

    if rename_map:
        logger.info("Normalizing columns for %s", schema_section)
        # Avoid duplicate columns by dropping existing columns that will be overwritten by a rename
        new_df = df.copy()
        for old_col, new_col in rename_map.items():
            if old_col != new_col and new_col in new_df.columns:
                new_df = new_df.drop(columns=[new_col])
        # Perform rename
        new_df = new_df.rename(columns=rename_map)

        # 3. Standardize common ID columns as STRINGS
        # Load list from identifiers section in schema.toml
        id_cols = schema.get("identifiers", {}).get("columns", [])

        for col in id_cols:
            if col in new_df.columns:
                new_df[col] = new_df[col].apply(stringify_id)

        return new_df

    return df


def process_fairway_geometry(fw_row, lock_row, buffer_dist=0, openings_data=None):
    """
    Calculate fairway segments and distance using metric projection (EPSG:28992).
    If openings_data is provided, expand buffer_dist to encompass the farthest opening.
    """
    fairway_data = {}

    # Extract geometries safely
    from shapely import wkt

    fw_geom = fw_row.geometry if hasattr(fw_row, "geometry") else None
    if isinstance(fw_geom, str):
        fw_geom = wkt.loads(fw_geom)

    lock_geom = lock_row.geometry if hasattr(lock_row, "geometry") else None
    if isinstance(lock_geom, str):
        lock_geom = wkt.loads(lock_geom)

    if not fw_geom or not lock_geom:
        return fairway_data

    # Accurate Spatial Projection (EPSG:28992) for metric calculations
    gs_lock = gpd.GeoSeries([lock_geom], crs="EPSG:4326").to_crs("EPSG:28992")
    gs_fw = gpd.GeoSeries([fw_geom], crs="EPSG:4326").to_crs("EPSG:28992")

    lock_point_rd = gs_lock.iloc[0]
    fw_line_rd = gs_fw.iloc[0]

    if lock_point_rd.geom_type != "Point":
        lock_point_rd = lock_point_rd.centroid

    # Project lock point to line (in meters)
    projected_dist = fw_line_rd.project(lock_point_rd)
    fairway_data["lock_to_fairway_distance"] = fw_line_rd.distance(lock_point_rd)

    # Dynamic buffer expansion based on associated bridge openings
    max_offset = buffer_dist
    if openings_data:
        for op in openings_data:
            op_geom_wkt = op.get("geometry")
            if not op_geom_wkt:
                continue
            op_geom = wkt.loads(op_geom_wkt)
            gs_op = gpd.GeoSeries([op_geom], crs="EPSG:4326").to_crs("EPSG:28992")
            op_point_rd = gs_op.iloc[0]
            if op_point_rd.geom_type != "Point":
                op_point_rd = op_point_rd.centroid

            op_proj_dist = fw_line_rd.project(op_point_rd)
            dist_from_lock = abs(op_proj_dist - projected_dist)

            # Check available space on the fairway section to move the node back
            if op_proj_dist >= projected_dist:
                space_left = fw_line_rd.length - op_proj_dist
            else:
                space_left = op_proj_dist

            target_margin = 100
            actual_margin = min(target_margin, space_left)
            max_offset = max(max_offset, dist_from_lock + actual_margin)

    total_len = fw_line_rd.length
    # Split with offset (gap for the structure complex)
    dist_before = max(0, projected_dist - max_offset)
    dist_after = min(total_len, projected_dist + max_offset)

    # Convert back to WGS84 by interpolating on the original WGS84 line
    # interpolating relative distances on WGS84 line is robust
    geom_before = substring(fw_geom, 0, (dist_before / total_len) * fw_geom.length)
    geom_after = substring(
        fw_geom, (dist_after / total_len) * fw_geom.length, fw_geom.length
    )

    if geom_before:
        fairway_data["geometry_before_wkt"] = geom_before.wkt
    if geom_after:
        fairway_data["geometry_after_wkt"] = geom_after.wkt

    return fairway_data


def find_nearby_berths(
    lock_row,
    berths_gdf,
    fairway_geom_before,
    fairway_geom_after,
    max_dist_m=None,
    allowed_categories=None,
    allowed_fairways=None,
    disallowed_sections=None,
    sections_gdf=None,
):
    """
    Find berths associated with the lock's fairway and determine if they are before or after.
    Enforces a strict distance check (default from settings) and category filtering.
    """
    if max_dist_m is None:
        max_dist_m = settings.BERTH_MATCH_MAX_DIST_M

    if allowed_categories is None:
        allowed_categories = ["WAITING_AREA"]

    nearby = []
    if berths_gdf is None:
        return nearby

    candidates = berths_gdf.copy()

    # Filter by category (if present)
    if "category" in candidates.columns and allowed_categories:
        candidates = candidates[
            candidates["category"].isna()
            | candidates["category"].isin(allowed_categories)
        ]

    # Filter by allowed FairwayIDs (normalized)
    if allowed_fairways and "fairway_id" in candidates.columns:
        candidates = candidates[candidates["fairway_id"].isin(allowed_fairways)]

    if candidates.empty:
        return nearby

    lock_geom = lock_row.geometry if hasattr(lock_row, "geometry") else None
    geod = Geod(ellps="WGS84")

    # Handle disallowed sections (inside lock)
    disallowed_mask = None
    if disallowed_sections and sections_gdf is not None:
        from shapely.ops import unary_union

        invalid_mask = sections_gdf["id"].isin(disallowed_sections)
        disallowed_geoms = sections_gdf[invalid_mask].geometry.tolist()
        if disallowed_geoms:
            disallowed_mask = unary_union(disallowed_geoms).buffer(
                settings.BERTH_INTERNAL_SECTION_BUFFER_DEG
            )

    # Pre-parse fairway geometries
    from shapely.geometry import LineString

    g_before = (
        wkt.loads(fairway_geom_before)
        if isinstance(fairway_geom_before, str)
        else fairway_geom_before
        if isinstance(fairway_geom_before, LineString)
        else None
    )
    g_after = (
        wkt.loads(fairway_geom_after)
        if isinstance(fairway_geom_after, str)
        else fairway_geom_after
        if isinstance(fairway_geom_after, LineString)
        else None
    )

    for _, berth in candidates.iterrows():
        is_nearby = False
        dist_m = None

        if disallowed_mask and berth.geometry:
            if disallowed_mask.intersects(berth.geometry):
                continue

        # Calculate spatial distance
        if lock_geom and berth.geometry:
            lg = lock_geom if isinstance(lock_geom, Point) else lock_geom.centroid
            bg = (
                berth.geometry
                if isinstance(berth.geometry, Point)
                else berth.geometry.centroid
            )

            if lg and bg:
                _, _, dist_m = geod.inv(lg.x, lg.y, bg.x, bg.y)
                if dist_m <= max_dist_m:
                    is_nearby = True

        if not is_nearby:
            continue

        # Determine relation (before/after)
        relation = "unknown"
        if g_before and g_after and berth.geometry:
            if g_before.distance(berth.geometry) < g_after.distance(berth.geometry):
                relation = "before"
            else:
                relation = "after"

        # Propagate ALL scalar attributes
        b_obj = {}
        for k, v in berth.to_dict().items():
            if k == "geometry":
                continue

            # Use a robust way to check for NA on potentially complex types
            try:
                if isinstance(v, (list, dict, pd.Series, pd.Index, np.ndarray)):
                    # Keep complex types as-is (might be problematic for downstream but avoids error)
                    b_obj[k] = v
                elif pd.notna(v):
                    b_obj[k] = v
            except (ValueError, TypeError):
                # If truth value is still ambiguous, just include it to be safe
                b_obj[k] = v

        b_obj.update(
            {
                "geometry": berth.geometry.wkt
                if hasattr(berth, "geometry") and berth.geometry
                else None,
                "relation": relation,
                "dist_m": round(dist_m, 1) if dist_m is not None else None,
            }
        )
        nearby.append(b_obj)

    return nearby


def find_chamber_doors(chamber_geom, split_point, merge_point):
    """
    Find the entrance and exit points (doors) of a chamber.
    Calculated as the nearest points on the chamber boundary to the split and merge points.
    """
    if not chamber_geom or not split_point or not merge_point:
        return None, None

    # Ensure we work with the boundary for polygons
    target_geom = (
        chamber_geom.boundary
        if chamber_geom.geom_type in ["Polygon", "MultiPolygon"]
        else chamber_geom
    )

    # Door 1: Nearest to split_point (upstream/start)
    door_start = nearest_points(target_geom, split_point)[0]

    # Door 2: Nearest to merge_point (downstream/end)
    door_end = nearest_points(target_geom, merge_point)[0]

    return door_start, door_end
