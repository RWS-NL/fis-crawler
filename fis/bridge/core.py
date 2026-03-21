import logging
from typing import List, Dict, Any

import geopandas as gpd
import pandas as pd
from shapely import wkt
from tqdm import tqdm
from fis import settings

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
            if pd.notna(row.get("id")):
                op_id = int(row["id"])
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
        disk_bridges_rd = disk_bridges.to_crs(settings.PROJECTED_CRS)

    # Pre-project sections for spatial matching
    sections_rd = None
    if sections_gdf is not None and not sections_gdf.empty:
        sections_rd = sections_gdf.to_crs(settings.PROJECTED_CRS)

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
        bridge_id = bridge["id"]
        bridge_data = sanitize_attrs(bridge)
        bridge_data["id"] = bridge_id
        bridge_data["feature_type"] = "bridge"
        bridge_data["geometry"] = bridge.geometry.wkt if bridge.geometry else None

        # Related openings
        bridge_openings = []
        if openings_df is not None and not openings_df.empty:
            openings_match = openings_df[openings_df["parent_id"] == bridge_id]
            for _, op in openings_match.iterrows():
                op_data = sanitize_attrs(op)
                op_data["id"] = int(op["id"])

                # Restore opening geometry (stripped by sanitize_attrs)
                geom_val = op.get("geometry", op.get("Geometry"))
                if pd.notna(geom_val):
                    if hasattr(geom_val, "wkt"):
                        op_data["geometry"] = geom_val.wkt
                    elif isinstance(geom_val, str):
                        op_data["geometry"] = geom_val

                # Operating times for this opening
                if pd.notna(op.get("operating_times_id")):
                    op_time_id = int(op["operating_times_id"])
                    if op_time_id in op_times_map:
                        op_data["operating_times"] = op_times_map[op_time_id]

                bridge_openings.append(op_data)
        bridge_data["openings"] = bridge_openings

        # Match sections
        intersecting_sections = []
        matched_section_ids = set()

        # 1. Attribute-based matching (section_id, fairway_id)
        if sections_gdf is not None:
            # Match directly by section_id
            fsid = bridge.get("section_id")
            if pd.notna(fsid):
                matches = sections_gdf[sections_gdf["id"] == int(fsid)]
                for _, sec in matches.iterrows():
                    sid = int(sec["id"])
                    if sid not in matched_section_ids:
                        s_data = sanitize_attrs(sec)
                        s_data["id"] = sid
                        s_data["relation"] = "direct"
                        intersecting_sections.append(s_data)
                        matched_section_ids.add(sid)

            # Match by fairway_id (as context)
            fid = bridge.get("fairway_id")
            if pd.notna(fid):
                matches = sections_gdf[sections_gdf["fairway_id"] == int(fid)]
                for _, sec in matches.iterrows():
                    sid = int(sec["id"])
                    # We don't automatically add all fairway sections,
                    # but we keep this for context if needed.
                    pass

        # 2. Spatial match (using settings buffer for robustness)
        if sections_rd is not None and bridge.geometry:
            bridge_geom_rd = (
                gpd.GeoSeries([bridge.geometry], crs="EPSG:4326")
                .to_crs(settings.PROJECTED_CRS)
                .iloc[0]
            )
            bridge_buf = bridge_geom_rd.buffer(settings.BRIDGE_SECTION_MATCH_BUFFER_M)

            mask = sections_rd.intersects(bridge_buf)
            for _, sec in sections_gdf[mask].iterrows():
                sid = int(sec["id"])
                if sid not in matched_section_ids:
                    s_data = sanitize_attrs(sec)
                    s_data["id"] = sid
                    s_data["relation"] = "overlap"
                    intersecting_sections.append(s_data)
                    matched_section_ids.add(sid)

        bridge_data["sections"] = intersecting_sections

        # Spatially match DISK structures (using settings buffer)
        matched_disk = []
        if disk_bridges_rd is not None and bridge.geometry:
            bridge_geom_rd = (
                gpd.GeoSeries([bridge.geometry], crs="EPSG:4326")
                .to_crs(settings.PROJECTED_CRS)
                .iloc[0]
            )
            bridge_buf = bridge_geom_rd.buffer(settings.DISK_MATCH_BUFFER_BRIDGE_M)

            mask = disk_bridges_rd.intersects(bridge_buf)
            for _, db in disk_bridges[mask].iterrows():
                matched_disk.append(sanitize_attrs(db))
        bridge_data["disk_bridges"] = matched_disk

        complexes.append(bridge_data)

    return complexes
