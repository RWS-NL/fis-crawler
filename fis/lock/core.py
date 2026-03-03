import pathlib
import logging
import pandas as pd
import geopandas as gpd
from shapely import wkt
from tqdm import tqdm

from shapely.geometry import Point, LineString

logger = logging.getLogger(__name__)


def load_data(export_dir: pathlib.Path, disk_dir: pathlib.Path):
    """Load necessary parquet files."""

    def read_geo_or_parquet(dir_path, stem):
        gpq = dir_path / f"{stem}.geoparquet"
        pq = dir_path / f"{stem}.parquet"
        if gpq.exists():
            return gpd.read_parquet(gpq)
        if pq.exists():
            df = pd.read_parquet(pq)
            if "Geometry" in df.columns and df["Geometry"].dtype == "object":
                df["geometry"] = df["Geometry"].apply(
                    lambda x: wkt.loads(x) if x else None
                )
                return gpd.GeoDataFrame(df, geometry="geometry")
            return df
        return None

    locks = read_geo_or_parquet(export_dir, "lock")
    chambers = read_geo_or_parquet(export_dir, "chamber")
    subchambers = read_geo_or_parquet(export_dir, "subchamber")
    isrs = read_geo_or_parquet(export_dir, "isrs")
    fairways = read_geo_or_parquet(export_dir, "fairway")
    berths = read_geo_or_parquet(export_dir, "berth")
    sections = read_geo_or_parquet(export_dir, "section")

    if locks is None or chambers is None:
        raise FileNotFoundError("Missing essential lock/chamber data.")

    disk_locks = read_geo_or_parquet(disk_dir, "schutsluis")
    brug_vast = read_geo_or_parquet(disk_dir, "brug_vast")
    brug_beweegbaar = read_geo_or_parquet(disk_dir, "brug_beweegbaar")

    # Combine bridges
    bridges = []
    if brug_vast is not None:
        bridges.append(brug_vast)
    if brug_beweegbaar is not None:
        bridges.append(brug_beweegbaar)
    disk_bridges = None
    if bridges:
        disk_bridges = pd.concat(bridges, ignore_index=True)
        if isinstance(bridges[0], gpd.GeoDataFrame):
            disk_bridges = gpd.GeoDataFrame(
                disk_bridges, geometry="geometry", crs=bridges[0].crs
            )

    if disk_locks is None or disk_bridges is None:
        raise FileNotFoundError("Missing essential DISK data (schutsluis or bridges).")

    operatingtimes = read_geo_or_parquet(export_dir, "operatingtimes")

    return (
        locks,
        chambers,
        subchambers,
        isrs,
        fairways,
        berths,
        sections,
        disk_locks,
        disk_bridges,
        operatingtimes,
    )


from shapely.geometry.base import BaseGeometry
import numpy as np


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


def match_disk_objects(lock, lock_chambers, disk_locks_rd, disk_bridges_rd):
    """Spatially match DISK locks and bridges to a given FIS lock complex."""
    matched_disk_locks = []
    matched_disk_bridges = []

    complex_geoms_rd = []
    lock_geom_rd = None
    if hasattr(lock, "geometry") and lock.geometry:
        lock_geom_rd = (
            gpd.GeoSeries([lock.geometry], crs="EPSG:4326").to_crs("EPSG:28992").iloc[0]
        )
        complex_geoms_rd.append(lock_geom_rd)

    chamber_geoms_rd = []
    if "geometry" in lock_chambers.columns:
        for _, c_row in lock_chambers.iterrows():
            if pd.notna(c_row["geometry"]):
                c_geom = (
                    wkt.loads(c_row["geometry"])
                    if isinstance(c_row["geometry"], str)
                    else c_row["geometry"]
                )
                c_geom_rd = (
                    gpd.GeoSeries([c_geom], crs="EPSG:4326")
                    .to_crs("EPSG:28992")
                    .iloc[0]
                )
                chamber_geoms_rd.append(c_geom_rd)
                complex_geoms_rd.append(c_geom_rd)

    if complex_geoms_rd:
        from shapely.ops import unary_union

        # For bridges, we use the complex buffered bounds
        complex_union_rd = unary_union(complex_geoms_rd)
        complex_buffered_rd = complex_union_rd.buffer(500)

        # Match DISK Bridges
        if disk_bridges_rd is not None:
            bridge_mask = disk_bridges_rd.intersects(complex_buffered_rd)
            for _, b in disk_bridges_rd[bridge_mask].iterrows():
                matched_disk_bridges.append(sanitize_attrs(b))

        # Match DISK Locks
        if disk_locks_rd is not None:
            # 1. Try strict chamber intersection first
            chamber_union_rd = (
                unary_union(chamber_geoms_rd) if chamber_geoms_rd else None
            )
            if chamber_union_rd:
                lock_mask_strict = disk_locks_rd.intersects(chamber_union_rd)
                for _, l in disk_locks_rd[lock_mask_strict].iterrows():
                    matched_disk_locks.append(sanitize_attrs(l))

            # 2. If NO locks matched via chambers, fallback to 500m lock complex buffer
            if not matched_disk_locks and lock_geom_rd:
                lock_buffer_rd = lock_geom_rd.buffer(500)
                lock_mask_loose = disk_locks_rd.intersects(lock_buffer_rd)
                for _, l in disk_locks_rd[lock_mask_loose].iterrows():
                    matched_disk_locks.append(sanitize_attrs(l))

    return matched_disk_locks, matched_disk_bridges


def find_fairway_junctions(sections_gdf, fairway_id):
    """
    Identify start and end junctions for a given fairway based on sections.
    """
    start_junction = None
    end_junction = None

    if sections_gdf is None:
        return start_junction, end_junction

    fw_sections = sections_gdf[sections_gdf["FairwayId"] == int(fairway_id)]
    if fw_sections.empty:
        return start_junction, end_junction

    fw_sections = fw_sections.sort_values("RouteKmBegin")

    if pd.notna(fw_sections.iloc[0]["StartJunctionId"]):
        start_junction = int(fw_sections.iloc[0]["StartJunctionId"])

    if pd.notna(fw_sections.iloc[-1]["EndJunctionId"]):
        end_junction = int(fw_sections.iloc[-1]["EndJunctionId"])

    return start_junction, end_junction


def _resolve_isrs_code(lock, isrs):
    if pd.notna(lock.get("IsrsId")) and isrs is not None:
        isrs_row = isrs[isrs["Id"] == lock["IsrsId"]]
        if not isrs_row.empty:
            return isrs_row.iloc[0]["Code"]
    return None


def _resolve_ris_info(lock_isrs_code, ris_df):
    ris_info = {}
    if lock_isrs_code and ris_df is not None:
        match = ris_df[ris_df["isrs_code"] == lock_isrs_code]
        if not match.empty:
            ris_info = {
                "ris_name": match.iloc[0]["name"],
                "ris_function": match.iloc[0]["function"],
            }
    return ris_info


def _resolve_fairway_data(lock, lock_chambers, fairways, sections_gdf):
    fairway_data = {}
    chamber_routes = {}
    if fairways is not None and pd.notna(lock.get("FairwayId")):
        fw_row = fairways[fairways["Id"] == lock["FairwayId"]]
        if not fw_row.empty:
            fw_obj = fw_row.iloc[0]
            fairway_data = {
                "fairway_name": fw_obj["Name"],
                "fairway_id": int(fw_obj["Id"]),
            }
            max_length = 0
            if "Length" in lock_chambers.columns:
                max_length = lock_chambers["Length"].max()
            if pd.isna(max_length):
                max_length = 0

            buffer_dist = (max_length / 2) + 50
            geom_data = process_fairway_geometry(fw_obj, lock, buffer_dist=buffer_dist)
            fairway_data.update(geom_data)

            if sections_gdf is not None:
                s_junc, e_junc = find_fairway_junctions(sections_gdf, int(fw_obj["Id"]))
                fairway_data["start_junction_id"] = s_junc
                fairway_data["end_junction_id"] = e_junc

    if "geometry_before_wkt" in fairway_data and "geometry_after_wkt" in fairway_data:
        bwkt = fairway_data["geometry_before_wkt"]
        awkt = fairway_data["geometry_after_wkt"]
        if bwkt and awkt:
            g_before = wkt.loads(bwkt)
            g_after = wkt.loads(awkt)
            split_point = Point(g_before.coords[-1])
            merge_point = Point(g_after.coords[0])
            chamber_routes["split_point"] = split_point
            chamber_routes["merge_point"] = merge_point

    return fairway_data, chamber_routes


def _find_connected_sections(
    lock, lock_chambers, sections_gdf, fairway_data, network_graph
):
    sections_data = []
    internal_sections = set()
    connected_fairways = set()

    if fairway_data.get("fairway_id"):
        connected_fairways.add(fairway_data["fairway_id"])

    if network_graph:
        for j_id in [
            fairway_data.get("start_junction_id"),
            fairway_data.get("end_junction_id"),
        ]:
            if j_id and network_graph.has_node(j_id):
                for nbr in network_graph.neighbors(j_id):
                    edge_data = network_graph.get_edge_data(j_id, nbr)
                    if edge_data and "FairwayId" in edge_data:
                        connected_fairways.add(int(edge_data["FairwayId"]))

    if sections_gdf is not None:
        complex_geoms = (
            [lock.geometry] if hasattr(lock, "geometry") and lock.geometry else []
        )
        if "geometry" in lock_chambers.columns:
            for _, c_row in lock_chambers.iterrows():
                if pd.notna(c_row["geometry"]):
                    c_geom = (
                        wkt.loads(c_row["geometry"])
                        if isinstance(c_row["geometry"], str)
                        else c_row["geometry"]
                    )
                    complex_geoms.append(c_geom)

        if complex_geoms:
            from shapely.ops import unary_union

            complex_union = unary_union([g for g in complex_geoms if g])
            if complex_union:
                buffered_union = complex_union.buffer(0.0001)
                intersecting = sections_gdf[sections_gdf.intersects(buffered_union)]

                for _, s_row in intersecting.iterrows():
                    sid = int(s_row["Id"])
                    fid = (
                        int(s_row["FairwayId"])
                        if pd.notna(s_row.get("FairwayId"))
                        else None
                    )

                    internal_sections.add(sid)
                    if fid:
                        connected_fairways.add(fid)

                    sections_data.append(
                        {
                            "id": sid,
                            "name": s_row["Name"],
                            "fairway_id": fid,
                            "length": float(s_row["Length"])
                            if pd.notna(s_row.get("Length"))
                            else None,
                            "geometry": s_row.geometry.wkt
                            if hasattr(s_row, "geometry") and s_row.geometry
                            else None,
                            "relation": "overlap",
                        }
                    )
    return sections_data, internal_sections, connected_fairways


def _build_chamber_objects(lock_chambers, chamber_routes, subchambers, op_times_map):
    chambers_list = []
    for _, chamber in lock_chambers.iterrows():
        route_wkt = None
        if "split_point" in chamber_routes and "merge_point" in chamber_routes:
            if "geometry" in chamber and pd.notna(chamber["geometry"]):
                ch_geom = (
                    wkt.loads(chamber["geometry"])
                    if isinstance(chamber["geometry"], str)
                    else chamber["geometry"]
                )
                centroid = ch_geom.centroid
                route = LineString(
                    [
                        chamber_routes["split_point"],
                        centroid,
                        chamber_routes["merge_point"],
                    ]
                )
                route_wkt = route.wkt

        chamber_attrs = sanitize_attrs(chamber)

        chamber_id = int(chamber["Id"])
        chamber_op_times = None
        if pd.notna(chamber.get("OperatingTimesId")):
            op_id = int(chamber["OperatingTimesId"])
            chamber_op_times = op_times_map.get(op_id)

        c_obj = {
            **chamber_attrs,
            "id": chamber_id,
            "name": chamber["Name"],
            "length": float(chamber["Length"]) if pd.notna(chamber["Length"]) else None,
            "width": float(chamber["Width"]) if pd.notna(chamber["Width"]) else None,
            "route_geometry": route_wkt,
            "operating_times": chamber_op_times,
        }

        if subchambers is not None:
            chamber_subchambers = subchambers[subchambers["ParentId"] == chamber["Id"]]
            c_obj["subchambers"] = []
            for _, sc in chamber_subchambers.iterrows():
                sc_obj = sanitize_attrs(sc)
                c_obj["subchambers"].append(sc_obj)

        chambers_list.append(c_obj)
    return chambers_list


def group_complexes(
    locks,
    chambers,
    subchambers,
    isrs,
    ris_df,
    fairways,
    berths,
    sections,
    network_graph=None,
    disk_locks=None,
    disk_bridges=None,
    operatingtimes=None,
):
    """
    Group locks into complexes and enrich with ISRS, RIS, Fairway, Berth, Section, and DISK data.
    """
    complexes = []

    # Convert locks to GeoDataFrame for spatial ops if needed
    if "geometry" in locks.columns and locks["geometry"].dtype == "object":
        locks = locks.copy()
        locks["geometry"] = locks["geometry"].apply(
            lambda x: wkt.loads(x) if x else None
        )
    locks_gdf = gpd.GeoDataFrame(locks, geometry="geometry")

    # Convert berths to GDF if needed
    berths_gdf = None
    if berths is not None:
        if "geometry" in berths.columns and berths["geometry"].dtype == "object":
            berths = berths.copy()
            berths["geometry"] = berths["geometry"].apply(
                lambda x: wkt.loads(x) if x else None
            )
        berths_gdf = (
            gpd.GeoDataFrame(berths, geometry="geometry")
            if "geometry" in berths.columns
            else berths
        )

    # Convert sections to GDF if needed
    sections_gdf = None
    if sections is not None:
        if "geometry" in sections.columns and sections["geometry"].dtype == "object":
            sections = sections.copy()
            sections["geometry"] = sections["geometry"].apply(
                lambda x: wkt.loads(x) if x else None
            )
        sections_gdf = (
            gpd.GeoDataFrame(sections, geometry="geometry")
            if "geometry" in sections.columns
            else sections
        )

    # Create spatial index for sections if not already present
    if sections_gdf is not None:
        pass

    # Pre-project DISK datasets for spatial joining
    disk_locks_rd = None
    if (
        disk_locks is not None
        and not disk_locks.empty
        and "geometry" in disk_locks.columns
    ):
        disk_locks_rd = disk_locks.to_crs("EPSG:28992")

    disk_bridges_rd = None
    if (
        disk_bridges is not None
        and not disk_bridges.empty
        and "geometry" in disk_bridges.columns
    ):
        disk_bridges_rd = disk_bridges.to_crs("EPSG:28992")

    # Pre-process operating times
    op_times_map = {}
    if operatingtimes is not None and not operatingtimes.empty:
        for _, row in operatingtimes.iterrows():
            if pd.notna(row.get("Id")):
                op_id = int(row["Id"])
                op_times_map[op_id] = {
                    "NormalSchedules": to_python(row.get("NormalSchedules")) or [],
                    "HolidaySchedules": to_python(row.get("HolidaySchedules")) or [],
                    "ExceptionSchedules": to_python(row.get("ExceptionSchedules"))
                    or [],
                }

    for idx, lock in tqdm(
        locks_gdf.iterrows(), total=len(locks_gdf), desc="Processing locks"
    ):
        lock_chambers = chambers[chambers["ParentId"] == lock["Id"]]
        lock_isrs_code = _resolve_isrs_code(lock, isrs)
        ris_info = _resolve_ris_info(lock_isrs_code, ris_df)
        fairway_data, chamber_routes = _resolve_fairway_data(
            lock, lock_chambers, fairways, sections_gdf
        )

        logger.debug("  Checking connected fairways and sections...")
        sections_data, internal_sections, connected_fairways = _find_connected_sections(
            lock, lock_chambers, sections_gdf, fairway_data, network_graph
        )

        logger.debug(
            "  Finding nearby berths (allowed fairways: %s)...", connected_fairways
        )
        berths_data = []
        if berths_gdf is not None:
            berths_data = find_nearby_berths(
                lock,
                berths_gdf,
                fairway_data.get("geometry_before_wkt"),
                fairway_data.get("geometry_after_wkt"),
                allowed_fairways=list(connected_fairways),
                disallowed_sections=list(internal_sections),
                sections_gdf=sections_gdf,
            )
        logger.debug("  Found %d berths.", len(berths_data))

        matched_disk_locks, matched_disk_bridges = match_disk_objects(
            lock, lock_chambers, disk_locks_rd, disk_bridges_rd
        )

        lock_attrs = sanitize_attrs(lock)

        disk_complex_id = None
        disk_complex_name = None
        for dl in matched_disk_locks:
            if dl.get("complexid"):
                disk_complex_id = dl.get("complexid")
                disk_complex_name = dl.get("complex_naam")
                break

        lock_id = int(lock["Id"])
        lock_op_times = None
        if pd.notna(lock.get("OperatingTimesId")):
            op_id = int(lock["OperatingTimesId"])
            lock_op_times = op_times_map.get(op_id)

        complex_obj = {
            **lock_attrs,
            "id": lock_id,
            "name": lock["Name"],
            "isrs_code": lock_isrs_code,
            **ris_info,
            **fairway_data,
            "berths": berths_data,
            "sections": sections_data,
            "disk_locks": matched_disk_locks,
            "disk_bridges": matched_disk_bridges,
            "disk_complex_id": disk_complex_id,
            "disk_complex_name": disk_complex_name,
            "operating_times": lock_op_times,
            "locks": [
                {
                    "id": lock_id,
                    "name": lock["Name"],
                    **lock_attrs,
                    "operating_times": lock_op_times,
                    "chambers": _build_chamber_objects(
                        lock_chambers, chamber_routes, subchambers, op_times_map
                    ),
                }
            ],
        }

        complexes.append(complex_obj)

    return complexes


def split_fairway(fairway_geom, lock_km, fairway_start_km, fairway_end_km):
    """
    Split the fairway geometry at the lock's location based on KM mark.
    """
    from shapely.ops import substring
    from shapely.geometry import LineString

    if not fairway_geom or not isinstance(fairway_geom, LineString):
        return None, None

    total_len = fairway_geom.length
    section_len_km = abs(fairway_end_km - fairway_start_km)

    if section_len_km == 0:
        return None, None

    # Determine ratio
    if fairway_end_km > fairway_start_km:
        ratio = (lock_km - fairway_start_km) / section_len_km
    else:
        # Decreasing KM mapping
        ratio = (fairway_start_km - lock_km) / section_len_km

    dist_on_line = ratio * total_len
    dist_on_line = max(0.0, min(total_len, dist_on_line))  # Clamp

    before = substring(fairway_geom, 0, dist_on_line)
    after = substring(fairway_geom, dist_on_line, total_len)

    return before, after


def process_fairway_geometry(fw_row, lock_row, buffer_dist=0):
    """
    Calculate fairway segments and distance using metric projection (EPSG:28992).
    """
    from shapely.ops import substring
    import logging

    logging.getLogger(__name__)

    fairway_data = {}

    # Extract geometries safely
    fw_geom = fw_row.geometry if hasattr(fw_row, "geometry") else None
    lock_geom = lock_row.geometry if hasattr(lock_row, "geometry") else None

    # KM-based Split (Fallback)
    if fw_geom and pd.notna(lock_row.get("RouteKmBegin")):
        geom_before, geom_after = split_fairway(
            fw_geom,
            lock_row["RouteKmBegin"],
            fw_row.get("RouteKmBegin", 0),
            fw_row.get("RouteKmEnd", 0),
        )
        if geom_before:
            fairway_data["geometry_before_wkt"] = geom_before.wkt
            fairway_data["geometry_after_wkt"] = geom_after.wkt

    # Accurate Spatial Projection (EPSG:28992)
    if lock_geom and fw_geom:
        # Create GeoSeries for projection
        gs_lock = gpd.GeoSeries([lock_geom], crs="EPSG:4326")
        gs_fw = gpd.GeoSeries([fw_geom], crs="EPSG:4326")

        # Reproject to RD New (EPSG:28992) for meters
        gs_lock = gs_lock.to_crs("EPSG:28992")
        gs_fw = gs_fw.to_crs("EPSG:28992")

        lock_point_rd = gs_lock.iloc[0]
        fw_line_rd = gs_fw.iloc[0]

        if lock_point_rd.geom_type != "Point":
            lock_point_rd = lock_point_rd.centroid

        # Project lock point to line (in meters)
        projected_dist = fw_line_rd.project(lock_point_rd)
        projected_point = fw_line_rd.interpolate(projected_dist)

        fairway_data["lock_to_fairway_distance_meters"] = lock_point_rd.distance(
            projected_point
        )

        # Split using spatial projection with buffer
        dist_split = max(0, projected_dist - buffer_dist)
        dist_merge = min(fw_line_rd.length, projected_dist + buffer_dist)

        before_spatial_rd = substring(fw_line_rd, 0, dist_split)
        after_spatial_rd = substring(fw_line_rd, dist_merge, fw_line_rd.length)

        # Project back to 4326 for WKT output
        before_spatial = (
            gpd.GeoSeries([before_spatial_rd], crs="EPSG:28992")
            .to_crs("EPSG:4326")
            .iloc[0]
        )
        after_spatial = (
            gpd.GeoSeries([after_spatial_rd], crs="EPSG:28992")
            .to_crs("EPSG:4326")
            .iloc[0]
        )

        fairway_data["geometry_before_wkt"] = before_spatial.wkt
        fairway_data["geometry_after_wkt"] = after_spatial.wkt

    return fairway_data


def find_nearby_berths(
    lock_row,
    berths_gdf,
    fairway_geom_before,
    fairway_geom_after,
    max_dist_m=2000,
    allowed_categories=None,
    allowed_fairways=None,
    disallowed_sections=None,
    sections_gdf=None,
):
    """
    Find berths associated with the lock's fairway and determine if they are before or after.
    Enforces a strict distance check (default 2km) and category filtering.
    """
    if allowed_categories is None:
        allowed_categories = ["WAITING_AREA"]

    nearby = []
    if berths_gdf is None:
        return nearby

    candidates = berths_gdf.copy()

    # Filter by Category (if present)
    if "Category" in candidates.columns and allowed_categories:
        candidates = candidates[
            candidates["Category"].isna()
            | candidates["Category"].isin(allowed_categories)
        ]

    # Filter by allowed FairwayIDs (which we computed via geometric overlap of the lock)
    if allowed_fairways and "FairwayId" in candidates.columns:
        candidates = candidates[candidates["FairwayId"].isin(allowed_fairways)]

    if candidates.empty:
        return nearby

    lock_geom = lock_row.geometry if hasattr(lock_row, "geometry") else None
    lock_row.get("RouteKmBegin")

    from pyproj import Geod
    from shapely.geometry import Point
    from shapely.ops import unary_union

    geod = Geod(ellps="WGS84")

    # Pre-select the disallowed sections geometries once to speed up testing
    disallowed_geoms = []
    if disallowed_sections and sections_gdf is not None:
        invalid_mask = sections_gdf["Id"].isin(disallowed_sections)
        disallowed_geoms = sections_gdf[invalid_mask].geometry.tolist()

    disallowed_union = unary_union(disallowed_geoms) if disallowed_geoms else None
    # Pre-buffer for performance (EPSG:4326 is in degrees, 0.00005 is roughly 5 meters)
    disallowed_mask = disallowed_union.buffer(0.00005) if disallowed_union else None

    # Pre-parse fairway geometries
    g_before = wkt.loads(fairway_geom_before) if fairway_geom_before else None
    g_after = wkt.loads(fairway_geom_after) if fairway_geom_after else None

    for _, berth in candidates.iterrows():
        is_nearby = False
        dist_m = None
        berth_km = berth.get("RouteKmBegin")

        # Check if the berth sits directly INSIDE the lock chamber (on a disallowed section)
        # Using a small buffer (5m) to ensure we overlap if the point is snapped to the section line
        # The user requested: "the fairway should be connected, but it should not be the same fairway section"
        if disallowed_mask and berth.geometry:
            # We enforce that the berth geometry is NOT inside the disallowed internal section boundaries
            if disallowed_mask.intersects(berth.geometry):
                continue

        # Calculate spatial distance if geometries exist
        if lock_geom and berth.geometry:
            # Ensure we are comparing Points for Geod.inv
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

        # Spatial Projection (Substrings)
        # We have fairway_geom_before and fairway_geom_after WKTs
        if g_before and g_after and berth.geometry:
            # Buffer slightly for robustness
            if g_before.distance(berth.geometry) < g_after.distance(berth.geometry):
                relation = "before"
            else:
                relation = "after"

        nearby.append(
            {
                "id": int(berth["Id"]),
                "name": berth.get("Name"),
                "km": float(berth_km) if pd.notna(berth_km) else None,
                "dist_m": round(dist_m, 1) if dist_m is not None else None,
                "geometry": berth.geometry.wkt
                if hasattr(berth, "geometry") and berth.geometry
                else None,
                "relation": relation,
            }
        )

    return nearby
