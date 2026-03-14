import logging
from typing import List, Dict, Any

import geopandas as gpd
import pandas as pd
from shapely import wkt
from tqdm import tqdm

logger = logging.getLogger(__name__)


def sanitize_attrs(obj):
    """Sanitize pandas/geopandas objects to serializable dicts."""
    if isinstance(obj, pd.Series):
        obj = obj.to_dict()
    res = {}
    for k, v in obj.items():
        if pd.isna(v) or k == "geometry" or k == "Geometry":
            continue
        res[k] = v
    return res


def group_bridge_complexes(data: Dict[str, Any]) -> List[Dict]:
    """
    Groups FIS Bridges with their Openings, Sections, and Operating Times.
    Also spatially matches them to DISK physical bridge structures.
    """
    bridges_df = data.get("bridges")
    openings_df = data.get("openings")
    sections_gdf = data.get("sections")
    disk_bridges = data.get("disk_bridges")
    operatingtimes = data.get("operatingtimes")
    complexes = []

    # Map operating times
    op_times_map = {}
    if operatingtimes is not None and not operatingtimes.empty:
        for _, row in operatingtimes.iterrows():
            if pd.notna(row.get("Id")):
                op_id = int(row["Id"])
                op_times_map[op_id] = {
                    "NormalSchedules": row.get("NormalSchedules", []),
                    "HolidaySchedules": row.get("HolidaySchedules", []),
                    "ExceptionSchedules": row.get("ExceptionSchedules", []),
                }

    # Pre-project DISK bridges for spatial intersection matching
    disk_bridges_rd = None
    if (
        disk_bridges is not None
        and not disk_bridges.empty
        and "geometry" in disk_bridges.columns
    ):
        disk_bridges_rd = disk_bridges.to_crs("EPSG:28992")

    # Ensure bridges is a GeoDataFrame
    if "geometry" in bridges_df.columns and bridges_df["geometry"].dtype == "object":
        bridges_df = bridges_df.copy()
        bridges_df["geometry"] = bridges_df["geometry"].apply(
            lambda x: wkt.loads(x) if isinstance(x, str) else x
        )
    bridges_gdf = gpd.GeoDataFrame(bridges_df, geometry="geometry")

    for _, bridge in tqdm(
        bridges_gdf.iterrows(),
        total=len(bridges_gdf),
        desc="Processing bridges",
        mininterval=2.0,
    ):
        bridge_id = bridge["Id"]
        bridge_data = sanitize_attrs(bridge)
        bridge_data["id"] = bridge_id
        bridge_data["feature_type"] = "bridge"
        bridge_data["geometry"] = bridge.geometry.wkt if bridge.geometry else None

        # Related openings
        bridge_openings = []
        if openings_df is not None and not openings_df.empty:
            openings_match = openings_df[openings_df["ParentId"] == bridge_id]
            for _, op in openings_match.iterrows():
                op_data = sanitize_attrs(op)
                op_data["id"] = int(op["Id"])

                # Restore opening geometry (stripped by sanitize_attrs)
                geom_val = op.get("geometry", op.get("Geometry"))
                if pd.notna(geom_val):
                    if hasattr(geom_val, "wkt"):
                        op_data["geometry"] = geom_val.wkt
                    elif isinstance(geom_val, str):
                        op_data["geometry"] = geom_val

                # Operating times for this opening
                if pd.notna(op.get("OperatingTimesId")):
                    op_time_id = int(op["OperatingTimesId"])
                    if op_time_id in op_times_map:
                        op_data["operating_times"] = op_times_map[op_time_id]

                bridge_openings.append(op_data)
        bridge_data["openings"] = bridge_openings

        # Spatially match sections (using 10m buffer approach similar to locks)
        intersecting_sections = []
        if sections_gdf is not None and bridge.geometry:
            bridge_geom_rd = (
                gpd.GeoSeries([bridge.geometry], crs="EPSG:4326")
                .to_crs("EPSG:28992")
                .iloc[0]
            )
            bridge_buf = bridge_geom_rd.buffer(10)
            sections_rd = sections_gdf.to_crs("EPSG:28992")

            mask = sections_rd.intersects(bridge_buf)
            for _, sec in sections_gdf[mask].iterrows():
                s_data = sanitize_attrs(sec)
                s_data["id"] = int(sec["Id"])
                s_data["geometry"] = sec.geometry.wkt if sec.geometry else None
                intersecting_sections.append(s_data)
        bridge_data["sections"] = intersecting_sections

        # Spatially match DISK structures (using 200m buffer)
        matched_disk = []
        if disk_bridges_rd is not None and bridge.geometry:
            bridge_geom_rd = (
                gpd.GeoSeries([bridge.geometry], crs="EPSG:4326")
                .to_crs("EPSG:28992")
                .iloc[0]
            )
            bridge_buf = bridge_geom_rd.buffer(200)

            mask = disk_bridges_rd.intersects(bridge_buf)
            for _, db in disk_bridges[mask].iterrows():
                matched_disk.append(sanitize_attrs(db))
        bridge_data["disk_bridges"] = matched_disk

        complexes.append(bridge_data)

    return complexes
