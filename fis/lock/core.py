import pathlib
from typing import List, Dict, Any
import logging
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.strtree import STRtree
from tqdm import tqdm

from shapely.geometry import Point, LineString
from shapely.ops import unary_union
from fis.utils import to_python, sanitize_attrs, stringify_id
from fis.lock.utils import find_chamber_doors, project_geometry
from fis import settings, utils
from fis.ris_index import load_ris_index

logger = logging.getLogger(__name__)


def load_data(export_dir: pathlib.Path, disk_dir: pathlib.Path):
    """Load necessary parquet files and normalize attributes."""

    def read_geo_or_parquet(dir_path, stem):
        gpq = dir_path / f"{stem}.geoparquet"
        pq = dir_path / f"{stem}.parquet"

        if not gpq.exists() and not pq.exists():
            raise FileNotFoundError(
                f"Missing essential data: neither {gpq} nor {pq} exist."
            )

        if gpq.exists():
            gdf = gpd.read_parquet(gpq)
            # Standardize on lowercase 'geometry'
            if "Geometry" in gdf.columns and "geometry" not in gdf.columns:
                gdf = gdf.rename(columns={"Geometry": "geometry"}).set_geometry(
                    "geometry"
                )
            elif "Geometry" in gdf.columns and "geometry" in gdf.columns:
                # If both exist, drop the uppercase one and ensure lowercase is active
                gdf = gdf.drop(columns=["Geometry"]).set_geometry("geometry")
            return gdf

        df = pd.read_parquet(pq)
        # Standardize geometry column
        if "Geometry" in df.columns:
            geoms = df["Geometry"].apply(
                lambda x: wkt.loads(x) if isinstance(x, str) else x
            )
            df = df.drop(columns=["Geometry"])
            # If 'geometry' also exists (e.g. as string), overwrite it with parsed geoms
            if "geometry" in df.columns:
                df = df.drop(columns=["geometry"])
            return gpd.GeoDataFrame(df, geometry=geoms, crs="EPSG:4326")
        elif "geometry" in df.columns:
            # Standardize existing 'geometry' column (if it's WKT)
            if df["geometry"].dtype == "object":
                df["geometry"] = df["geometry"].apply(
                    lambda x: wkt.loads(x) if isinstance(x, str) else x
                )
            return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
        return df

    locks = read_geo_or_parquet(export_dir, "lock")
    chambers = read_geo_or_parquet(export_dir, "chamber")
    subchambers = read_geo_or_parquet(export_dir, "subchamber")
    isrs = read_geo_or_parquet(export_dir, "isrs")
    fairways = read_geo_or_parquet(export_dir, "fairway")
    berths = read_geo_or_parquet(export_dir, "berth")
    sections = read_geo_or_parquet(export_dir, "section")

    # Load and normalize structures
    schema = utils.load_schema()
    locks = utils.normalize_attributes(locks, "locks", schema)
    chambers = utils.normalize_attributes(chambers, "chambers", schema)
    subchambers = utils.normalize_attributes(subchambers, "subchambers", schema)
    berths = utils.normalize_attributes(berths, "berths", schema)
    isrs = utils.normalize_attributes(isrs, "isrs", schema)
    sections = utils.normalize_attributes(sections, "sections", schema)
    fairways = utils.normalize_attributes(fairways, "fairways", schema)

    disk_locks = read_geo_or_parquet(disk_dir, "schutsluis")
    brug_vast = read_geo_or_parquet(disk_dir, "brug_vast")
    brug_beweegbaar = read_geo_or_parquet(disk_dir, "brug_beweegbaar")

    # Combine bridges
    bridges_list = [brug_vast, brug_beweegbaar]
    disk_bridges = pd.concat(bridges_list, ignore_index=True)
    if isinstance(bridges_list[0], gpd.GeoDataFrame):
        disk_bridges = gpd.GeoDataFrame(
            disk_bridges, geometry="geometry", crs=bridges_list[0].crs
        )

    operatingtimes = read_geo_or_parquet(export_dir, "operatingtimes")
    bridges = read_geo_or_parquet(export_dir, "bridge")
    openings = read_geo_or_parquet(export_dir, "opening")

    # Normalize bridges/openings/operatingtimes
    operatingtimes = utils.normalize_attributes(
        operatingtimes, "operatingtimes", schema
    )
    bridges = utils.normalize_attributes(bridges, "bridges", schema)
    openings = utils.normalize_attributes(openings, "openings", schema)

    # Load RIS Index
    ris_path = export_dir / "RisIndexNL.xlsx"
    ris_df = load_ris_index(ris_path)

    return {
        "locks": locks,
        "chambers": chambers,
        "subchambers": subchambers,
        "isrs": isrs,
        "fairways": fairways,
        "berths": berths,
        "sections": sections,
        "disk_locks": disk_locks,
        "disk_bridges": disk_bridges,
        "operatingtimes": operatingtimes,
        "bridges": bridges,
        "openings": openings,
        "ris_df": ris_df,
    }


def match_disk_objects(lock, lock_chambers, disk_locks_rd, disk_bridges_rd):
    """Spatially match DISK locks and bridges to a given FIS lock complex."""
    matched_disk_locks = []
    matched_disk_bridges = []

    complex_geoms_rd = []

    lock_geom_rd = None
    if hasattr(lock, "geometry") and lock.geometry:
        lock_geom_rd = (
            gpd.GeoSeries([lock.geometry], crs="EPSG:4326")
            .to_crs(settings.PROJECTED_CRS)
            .iloc[0]
        )
        complex_geoms_rd.append(lock_geom_rd)

    chamber_geoms_rd = []
    if not lock_chambers.empty and "geometry" in lock_chambers.columns:
        for _, c_row in lock_chambers.iterrows():
            if pd.notna(c_row["geometry"]):
                c_geom = (
                    wkt.loads(c_row["geometry"])
                    if isinstance(c_row["geometry"], str)
                    else c_row["geometry"]
                )
                c_geom_rd = (
                    gpd.GeoSeries([c_geom], crs="EPSG:4326")
                    .to_crs(settings.PROJECTED_CRS)
                    .iloc[0]
                )
                chamber_geoms_rd.append(c_geom_rd)
                complex_geoms_rd.append(c_geom_rd)

    if complex_geoms_rd:
        # For bridges, we use the complex buffered bounds
        complex_union_rd = unary_union(complex_geoms_rd)
        complex_buffered_rd = complex_union_rd.buffer(settings.DISK_MATCH_BUFFER_LOCK_M)

        # Match DISK Bridges
        bridge_mask = disk_bridges_rd.intersects(complex_buffered_rd)
        for _, b in disk_bridges_rd[bridge_mask].iterrows():
            matched_disk_bridges.append(sanitize_attrs(b))

        # Match DISK Locks
        # 1. Try strict chamber intersection first
        chamber_union_rd = unary_union(chamber_geoms_rd) if chamber_geoms_rd else None
        if chamber_union_rd:
            lock_mask_strict = disk_locks_rd.intersects(chamber_union_rd)
            for _, lock_row in disk_locks_rd[lock_mask_strict].iterrows():
                matched_disk_locks.append(sanitize_attrs(lock_row))

        # 2. If NO locks matched via chambers, fallback to settings buffer
        if not matched_disk_locks and lock_geom_rd:
            lock_buffer_rd = lock_geom_rd.buffer(settings.DISK_MATCH_BUFFER_LOCK_M)
            lock_mask_loose = disk_locks_rd.intersects(lock_buffer_rd)
            for _, lock_row in disk_locks_rd[lock_mask_loose].iterrows():
                matched_disk_locks.append(sanitize_attrs(lock_row))

    if not matched_disk_locks:
        logger.debug(
            "No matching DISK objects (schutsluis) found for Lock %s (%s). Using FIS data as complex representative.",
            lock["id"],
            lock["name"],
        )

    return matched_disk_locks, matched_disk_bridges


def find_fairway_junctions(sections_gdf, fairway_id):
    """
    Identify start and end junctions for a given fairway based on sections.
    """
    start_junction = None
    end_junction = None

    fw_sections = sections_gdf[sections_gdf["fairway_id"] == fairway_id]
    if fw_sections.empty:
        return start_junction, end_junction

    fw_sections = fw_sections.sort_values("route_km_begin")

    if pd.notna(fw_sections.iloc[0]["start_junction_id"]):
        start_junction = stringify_id(fw_sections.iloc[0]["start_junction_id"])

    if pd.notna(fw_sections.iloc[-1]["end_junction_id"]):
        end_junction = stringify_id(fw_sections.iloc[-1]["end_junction_id"])

    return start_junction, end_junction


def _resolve_isrs_code(lock, isrs):
    if pd.notna(lock["isrs_id"]):
        isrs_row = isrs[isrs["id"] == lock["isrs_id"]]
        if isrs_row.empty:
            raise ValueError(f"ISRS {lock['isrs_id']} not found.")
        return isrs_row.iloc[0]["code"]
    return None


def _resolve_ris_info(lock_isrs_code, ris_df):
    ris_info = {}
    if lock_isrs_code and lock_isrs_code in ris_df.index:
        match = ris_df.loc[[lock_isrs_code]]
        ris_info = {
            "ris_name": match.iloc[0]["name"],
            "ris_function": match.iloc[0]["function"],
        }
    return ris_info


def _get_max_chamber_length(lock_chambers) -> float:
    """Return the maximum usable (or structural) length across all chambers."""
    if lock_chambers.empty:
        return 0.0
    for col in ("dim_usable_length", "dim_structural_length"):
        if col in lock_chambers.columns:
            v = lock_chambers[col].max()
            if pd.notna(v):
                return float(v)
    return 0.0


def _extract_coords(geom):
    """Recursively extract all coordinate tuples from any Shapely geometry."""
    if geom is None or geom.is_empty:
        return []
    if hasattr(geom, "coords"):
        return list(geom.coords)
    if hasattr(geom, "geoms"):
        result = []
        for sub in geom.geoms:
            result.extend(_extract_coords(sub))
        return result
    return []


def _compute_asymmetric_lock_buffers(fw_obj, lock, lock_chambers):
    """
    Compute asymmetric upstream/downstream buffer distances for a lock complex.

    For each chamber the fairway line is clipped against the chamber polygon.
    The split is placed ``DETAILED_LOCK_SPLICING_BUFFER_M`` upstream of the
    earliest fairway-inside-chamber position, and the merge that same margin
    downstream of the latest position.  This covers both "branch" locks (chamber
    polygon beside the fairway, not touching it) and "point" locks (chamber
    polygon straddles the fairway).

    When a chamber does not intersect the fairway at all, the entrance/exit doors
    are projected onto the fairway as a proxy; if door detection also fails, all
    exterior vertices are projected.

    Returns:
        (buffer_before_m, buffer_after_m) in metres.
    """
    fw_geom = fw_obj.geometry if hasattr(fw_obj, "geometry") else None
    if isinstance(fw_geom, str):
        fw_geom = wkt.loads(fw_geom)

    lock_geom = lock.geometry if hasattr(lock, "geometry") else None
    if isinstance(lock_geom, str):
        lock_geom = wkt.loads(lock_geom)

    margin = settings.DETAILED_LOCK_SPLICING_BUFFER_M
    max_len = _get_max_chamber_length(lock_chambers)
    fallback = (max_len / 2) + margin

    if fw_geom is None or getattr(fw_geom, "is_empty", False):
        return fallback, fallback
    if lock_geom is None or getattr(lock_geom, "is_empty", False):
        return fallback, fallback

    gs_fw = gpd.GeoSeries([fw_geom], crs="EPSG:4326").to_crs(settings.PROJECTED_CRS)
    gs_lock = gpd.GeoSeries([lock_geom], crs="EPSG:4326").to_crs(settings.PROJECTED_CRS)

    fw_line_rd = gs_fw.iloc[0]
    lock_rd = gs_lock.iloc[0]
    if lock_rd.geom_type != "Point":
        lock_rd = lock_rd.centroid

    lock_proj = fw_line_rd.project(lock_rd)

    # Provisional direction points for the door-projection fallback only.
    prov_up_rd = fw_line_rd.interpolate(max(0.0, lock_proj - 100.0))
    prov_down_rd = fw_line_rd.interpolate(
        min(fw_line_rd.length, lock_proj + 100.0)
    )
    prov_split_wgs = (
        gpd.GeoSeries([prov_up_rd], crs=settings.PROJECTED_CRS)
        .to_crs("EPSG:4326")
        .iloc[0]
    )
    prov_merge_wgs = (
        gpd.GeoSeries([prov_down_rd], crs=settings.PROJECTED_CRS)
        .to_crs("EPSG:4326")
        .iloc[0]
    )

    min_start = lock_proj
    max_end = lock_proj

    if not lock_chambers.empty and "geometry" in lock_chambers.columns:
        for _, ch_row in lock_chambers.iterrows():
            ch_geom_val = ch_row.get("geometry")
            if ch_geom_val is None:
                continue
            if isinstance(ch_geom_val, str):
                if not ch_geom_val.strip():
                    continue
                ch_geom = wkt.loads(ch_geom_val)
            elif pd.isna(ch_geom_val):
                continue
            else:
                ch_geom = ch_geom_val
            if getattr(ch_geom, "is_empty", False):
                continue

            ch_rd = (
                gpd.GeoSeries([ch_geom], crs="EPSG:4326")
                .to_crs(settings.PROJECTED_CRS)
                .iloc[0]
            )

            # Primary: clip the fairway against the chamber polygon to find exactly
            # where the fairway runs through the chamber (handles "point locks" where
            # the chamber straddles the fairway, as well as offset branches).
            clipped = fw_line_rd.intersection(ch_rd)
            if not clipped.is_empty:
                for cx, cy in _extract_coords(clipped):
                    d = fw_line_rd.project(Point(cx, cy))
                    min_start = min(min_start, d)
                    max_end = max(max_end, d)
                continue  # arc-lengths captured; no need for door fallback

            # The fairway doesn't intersect this chamber (offset branch):
            # project the entrance/exit doors onto the fairway.
            door_start_wgs, door_end_wgs = find_chamber_doors(
                ch_geom, prov_split_wgs, prov_merge_wgs
            )
            if door_start_wgs is not None and door_end_wgs is not None:
                door_start_rd = project_geometry(
                    door_start_wgs, "EPSG:4326", settings.PROJECTED_CRS
                )
                door_end_rd = project_geometry(
                    door_end_wgs, "EPSG:4326", settings.PROJECTED_CRS
                )
                min_start = min(min_start, fw_line_rd.project(door_start_rd))
                max_end = max(max_end, fw_line_rd.project(door_end_rd))
            else:
                # Final fallback: project all exterior vertices.
                polys = (
                    list(ch_rd.geoms)
                    if ch_rd.geom_type == "MultiPolygon"
                    else [ch_rd]
                )
                for poly in polys:
                    for cx, cy in poly.exterior.coords:
                        d = fw_line_rd.project(Point(cx, cy))
                        min_start = min(min_start, d)
                        max_end = max(max_end, d)

    buffer_before_m = max(fallback, (lock_proj - min_start) + margin)
    buffer_after_m = max(fallback, (max_end - lock_proj) + margin)

    return buffer_before_m, buffer_after_m


def _find_internal_junctions_for_chambers(lock_chambers, network_graph):
    """
    Return a mapping of chamber_id → list of internal FIS junction dicts.

    A junction is "internal" when its geometry falls inside the chamber polygon
    (with a small tolerance buffer of ``CHAMBER_INTERSECTION_BUFFER_M``).

    The result is stored inside each chamber dict so that ``build_graph_features``
    can insert the junctions as intermediate nodes on the chamber route edge.

    Performance: all network node geometries are projected to the metric CRS once
    and a Shapely STRtree is built so that each chamber polygon can be queried in
    O(k log N) rather than O(N).
    """
    if network_graph is None or lock_chambers.empty:
        return {}

    # --- Build projected node list once ---
    node_ids_list = []
    node_geoms_wgs84 = []
    node_geoms_rd = []

    for node_id, node_data in network_graph.nodes(data=True):
        node_geom = node_data.get("geometry")
        if node_geom is None:
            continue
        if not isinstance(node_geom, Point):
            try:
                node_geom = Point(node_geom)
            except Exception:
                continue
        if getattr(node_geom, "is_empty", False):
            continue
        node_geoms_wgs84.append(node_geom)
        node_ids_list.append(stringify_id(node_id))

    if not node_ids_list:
        return {}

    # Project all node geometries to metric CRS in one batch
    projected = gpd.GeoSeries(node_geoms_wgs84, crs="EPSG:4326").to_crs(
        settings.PROJECTED_CRS
    )
    node_geoms_rd = list(projected)

    # Build STRtree for fast spatial queries
    strtree = STRtree(node_geoms_rd)

    internal_by_chamber = {}

    # Iterate over chambers that have a polygon geometry
    for _, ch_row in lock_chambers.iterrows():
        ch_geom_val = ch_row.get("geometry")
        if ch_geom_val is None:
            continue
        if isinstance(ch_geom_val, str):
            if not ch_geom_val.strip():
                continue
            ch_geom = wkt.loads(ch_geom_val)
        elif pd.isna(ch_geom_val):
            continue
        else:
            ch_geom = ch_geom_val
        if getattr(ch_geom, "is_empty", False):
            continue

        if ch_geom.geom_type not in ("Polygon", "MultiPolygon"):
            continue

        # Project the chamber polygon to metric CRS before buffering so the
        # CHAMBER_INTERSECTION_BUFFER_M tolerance is applied accurately.
        ch_geom_rd = (
            gpd.GeoSeries([ch_geom], crs="EPSG:4326")
            .to_crs(settings.PROJECTED_CRS)
            .iloc[0]
        )
        ch_geom_buffered_rd = ch_geom_rd.buffer(settings.CHAMBER_INTERSECTION_BUFFER_M)

        ch_id = stringify_id(ch_row["id"])
        junctions_inside = []

        # Query candidates via STRtree – much faster than iterating all nodes
        candidate_indices = strtree.query(ch_geom_buffered_rd, predicate="contains")
        for idx in candidate_indices:
            junctions_inside.append(
                {
                    "id": node_ids_list[idx],
                    "geometry": node_geoms_wgs84[idx],  # keep WGS84 for downstream use
                }
            )

        if junctions_inside:
            internal_by_chamber[ch_id] = junctions_inside

    return internal_by_chamber


def _resolve_fairway_data(
    lock, lock_chambers, fairways, sections_gdf, openings_data=None
):
    fairway_data = {}
    chamber_routes = {}
    if pd.notna(lock["fairway_id"]):
        fw_row = fairways[fairways["id"] == lock["fairway_id"]]
        if fw_row.empty:
            logger.warning(
                "Fairway %s not found for Lock %s (%s). Skipping fairway-specific enrichment.",
                lock["fairway_id"],
                lock["id"],
                lock["name"],
            )
            return fairway_data, chamber_routes

        fw_obj = fw_row.iloc[0]
        fairway_data = {
            "fairway_name": fw_obj["name"],
            "fairway_id": stringify_id(fw_obj["id"]),
        }

        # Compute asymmetric buffers from chamber polygon extents so that the
        # split is placed before the earliest chamber and the merge after the
        # latest chamber, even when chambers are staggered along the fairway.
        buffer_before_m, buffer_after_m = _compute_asymmetric_lock_buffers(
            fw_obj, lock, lock_chambers
        )

        geom_data = utils.process_fairway_geometry(
            fw_obj,
            lock,
            buffer_before_m=buffer_before_m,
            buffer_after_m=buffer_after_m,
            openings_data=openings_data,
        )
        fairway_data.update(geom_data)

        s_junc, e_junc = find_fairway_junctions(
            sections_gdf, stringify_id(fw_obj["id"])
        )
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

            # Save the correct fairway-derived split/merge positions under
            # protected keys so the section splicer can use them directly.
            # The section splicer later overwrites geometry_before/after_wkt
            # with section-specific geometry, but these protected keys remain
            # intact and hold the globally-correct cut coordinates.
            fairway_data["_fairway_split_point_wkt"] = split_point.wkt
            fairway_data["_fairway_merge_point_wkt"] = merge_point.wkt

    return fairway_data, chamber_routes


def _resolve_openings_optimized(
    lock, lock_chambers, bridges_by_complex, openings_by_parent, op_times_map
):
    """Optimized version of _resolve_openings using pre-grouped maps."""
    openings_data = []

    # Find relevant Parent Ids
    parent_ids = {stringify_id(lock["id"])}
    if not lock_chambers.empty and "id" in lock_chambers.columns:
        for cid in lock_chambers["id"].dropna():
            parent_ids.add(stringify_id(cid))

    lock_complex_name = lock.get("related_building_complex_name")
    if pd.notna(lock_complex_name):
        matching_bridges = bridges_by_complex.get(lock_complex_name, pd.DataFrame())
        if not matching_bridges.empty:
            for bid in matching_bridges["id"].dropna():
                parent_ids.add(stringify_id(bid))

    # Use pre-grouped openings_by_parent for O(1) per-parent lookup
    matched_rows = []
    for pid in parent_ids:
        if pid in openings_by_parent:
            matched_rows.append(openings_by_parent[pid])

    if not matched_rows:
        return openings_data

    matched_openings = pd.concat(matched_rows)
    for _, opening_row in matched_openings.iterrows():
        op_attrs = sanitize_attrs(opening_row)
        op_id = stringify_id(opening_row["id"])

        # Attach operating times to the opening
        operating_times = None
        if pd.notna(opening_row["operating_times_id"]):
            ot_id = stringify_id(opening_row["operating_times_id"])
            operating_times = op_times_map.get(ot_id)

        op_attrs.update(
            {
                "id": op_id,
                "name": opening_row["name"],
                "operating_times": operating_times,
            }
        )
        openings_data.append(op_attrs)

    return openings_data


def _build_chamber_objects_optimized(
    lock_chambers,
    chamber_routes,
    subchambers_by_parent,
    op_times_map,
    internal_junctions_by_chamber=None,
):
    """Optimized chamber builder."""
    chambers_list = []
    if lock_chambers.empty:
        return chambers_list

    if internal_junctions_by_chamber is None:
        internal_junctions_by_chamber = {}

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

        chamber_id = stringify_id(chamber["id"])
        chamber_op_times = None
        if pd.notna(chamber["operating_times_id"]):
            op_id = stringify_id(chamber["operating_times_id"])
            chamber_op_times = op_times_map.get(op_id)

        # Determine functional dimensions for generic navigation (length/width)
        # Prioritize usable bounds, fallback to structural
        usable_len = chamber.get("dim_usable_length")
        if pd.isna(usable_len):
            usable_len = chamber.get("dim_structural_length")

        gate_width = chamber.get("dim_gate_width")
        if pd.isna(gate_width):
            gate_width = chamber.get("dim_structural_width")

        c_obj = {
            **chamber_attrs,
            "id": chamber_id,
            "name": chamber["name"],
            # Primary navigation dimensions
            "length": float(usable_len) if pd.notna(usable_len) else None,
            "width": float(gate_width) if pd.notna(gate_width) else None,
            # Explicitly preserved measurements
            "structural_length": float(chamber.get("dim_structural_length"))
            if pd.notna(chamber.get("dim_structural_length"))
            else None,
            "structural_width": float(chamber.get("dim_structural_width"))
            if pd.notna(chamber.get("dim_structural_width"))
            else None,
            "gate_width": float(chamber.get("dim_gate_width"))
            if pd.notna(chamber.get("dim_gate_width"))
            else None,
            "route_geometry": route_wkt,
            "operating_times": chamber_op_times,
            # FIS junction nodes that lie inside this chamber polygon (may be empty).
            # Used by build_graph_features to insert intermediate nodes on the
            # chamber_route edge (e.g. NL_J2501 / 8864190 inside Weurt chamber 47538).
            "internal_junctions": internal_junctions_by_chamber.get(chamber_id, []),
        }

        if chamber_id in subchambers_by_parent:
            chamber_subchambers = subchambers_by_parent[chamber_id]
            c_obj["subchambers"] = []
            for _, sc in chamber_subchambers.iterrows():
                sc_obj = sanitize_attrs(sc)
                c_obj["subchambers"].append(sc_obj)
        else:
            c_obj["subchambers"] = []

        chambers_list.append(c_obj)
    return chambers_list


def _find_connected_sections_optimized(
    lock, lock_chambers, sections_gdf, sections_rd, fairway_data, network_graph
):
    """Optimized connected sections finder using spatial index."""
    sections_data = []
    internal_sections = set()
    connected_fairways = set()
    matched_section_ids = set()

    if fairway_data.get("fairway_id"):
        connected_fairways.add(fairway_data["fairway_id"])

    # 1. Attribute-based matching
    fsid = lock["section_id"]
    if pd.notna(fsid):
        matches = sections_gdf[sections_gdf["id"] == fsid]

        for _, s_row in matches.iterrows():
            sid = stringify_id(s_row["id"])
            if sid not in matched_section_ids:
                fid = stringify_id(s_row.get("fairway_id"))
                internal_sections.add(sid)
                if fid:
                    connected_fairways.add(fid)
                sections_data.append(
                    {
                        "id": sid,
                        "name": s_row["name"],
                        "fairway_id": fid,
                        "length": float(s_row["dim_structural_length"])
                        if pd.notna(s_row.get("dim_structural_length"))
                        else None,
                        "geometry": s_row.geometry.wkt
                        if hasattr(s_row, "geometry") and s_row.geometry
                        else None,
                        "relation": "direct",
                    }
                )
                matched_section_ids.add(sid)

    # 2. Graph neighbors
    if network_graph:
        for j_id in [
            fairway_data.get("start_junction_id"),
            fairway_data.get("end_junction_id"),
        ]:
            if j_id and network_graph.has_node(j_id):
                for nbr in network_graph.neighbors(j_id):
                    edge_data = network_graph.get_edge_data(j_id, nbr)
                    if edge_data:
                        # Support both CamelCase (original FIS) and snake_case (schema-mapped)
                        fid_val = edge_data.get(
                            "fairway_id", edge_data.get("FairwayId")
                        )
                        if fid_val is not None:
                            connected_fairways.add(stringify_id(fid_val))

    # 3. Spatial matching using pre-built spatial index
    complex_geoms = (
        [lock.geometry] if hasattr(lock, "geometry") and lock.geometry else []
    )
    if not lock_chambers.empty and "geometry" in lock_chambers.columns:
        for _, c_row in lock_chambers.iterrows():
            if pd.notna(c_row["geometry"]):
                c_geom = (
                    wkt.loads(c_row["geometry"])
                    if isinstance(c_row["geometry"], str)
                    else c_row["geometry"]
                )
                complex_geoms.append(c_geom)

    if complex_geoms:
        complex_union = unary_union([g for g in complex_geoms if g])
        if complex_union:
            buffered_union = complex_union.buffer(
                settings.LOCK_SECTION_MATCH_BUFFER_DEG
            )
            # Use spatial index query for performance
            possible_matches_index = sections_gdf.sindex.query(
                buffered_union, predicate="intersects"
            )
            intersecting = sections_gdf.iloc[possible_matches_index]

            for _, s_row in intersecting.iterrows():
                sid = stringify_id(s_row["id"])
                if sid in matched_section_ids:
                    continue

                fid = stringify_id(s_row.get("fairway_id"))

                internal_sections.add(sid)
                if fid:
                    connected_fairways.add(fid)

                sections_data.append(
                    {
                        "id": sid,
                        "name": s_row["name"],
                        "fairway_id": fid,
                        "length": float(s_row["dim_structural_length"])
                        if pd.notna(s_row.get("dim_structural_length"))
                        else None,
                        "geometry": s_row.geometry.wkt
                        if hasattr(s_row, "geometry") and s_row.geometry
                        else None,
                        "relation": "overlap",
                    }
                )
                matched_section_ids.add(sid)
    return sections_data, internal_sections, connected_fairways


def group_complexes(data: Dict[str, Any], network_graph=None) -> List[Dict]:
    """
    Group locks into complexes and enrich with ISRS, RIS, Fairway, Berth, Section, and DISK data.
    """
    locks = data["locks"]
    chambers = data["chambers"]
    subchambers = data["subchambers"]
    isrs = data["isrs"]
    ris_df = data["ris_df"]
    fairways = data["fairways"]
    berths = data["berths"]
    sections = data["sections"]
    disk_locks = data["disk_locks"]
    disk_bridges = data["disk_bridges"]
    operatingtimes = data["operatingtimes"]
    bridges = data["bridges"]
    openings = data["openings"]
    complexes = []

    # Expect GeoDataFrames at this stage
    locks_gdf = locks
    berths_gdf = berths
    sections_gdf = sections

    # Ensure RIS Index is indexed for fast lookup
    if "isrs_code" in ris_df.columns:
        ris_df = ris_df.drop_duplicates(subset=["isrs_code"]).set_index("isrs_code")

    # Pre-group components for O(1) loop lookup
    def get_parent_map(df):
        p_map = {}
        if "parent_id" in df.columns:
            for pid, group in df.groupby("parent_id"):
                p_map[stringify_id(pid)] = group
        return p_map

    chambers_by_parent = get_parent_map(chambers)
    subchambers_by_parent = get_parent_map(subchambers)
    openings_by_parent = get_parent_map(openings)

    bridges_by_complex = {}
    if "related_building_complex_name" in bridges.columns:
        for name, group in bridges.groupby("related_building_complex_name"):
            bridges_by_complex[name] = group

    # Pre-project DISK datasets for spatial joining
    disk_locks_rd = disk_locks.to_crs(settings.PROJECTED_CRS)
    disk_bridges_rd = disk_bridges.to_crs(settings.PROJECTED_CRS)

    # Pre-project sections for spatial matching
    sections_rd = sections_gdf.to_crs(settings.PROJECTED_CRS)

    # Pre-process operating times
    op_times_map = {}
    if not operatingtimes.empty:
        for _, row in operatingtimes.iterrows():
            if pd.notna(row["id"]):
                op_id = stringify_id(row["id"])
                op_times_map[op_id] = {
                    "normal_schedules": to_python(row["normal_schedules"]) or [],
                    "holiday_schedules": to_python(row["holiday_schedules"]) or [],
                    "exception_schedules": to_python(row["exception_schedules"]) or [],
                }

    # Pre-compute complex groups before processing individual locks.
    # This identifies multi-branch complexes (e.g. Oranjesluizen) that share
    # boundary junctions and should be processed as a unit.
    complex_groups = detect_complex_groups(locks_gdf, sections_gdf)
    # Invert: lock_id → group_id for O(1) lookup inside the loop
    lock_to_group: dict[str, str] = {}
    for group_id, members in complex_groups.items():
        for lid in members:
            lock_to_group[lid] = group_id

    for idx, lock in tqdm(
        locks_gdf.iterrows(),
        total=len(locks_gdf),
        desc="Processing locks",
        mininterval=2.0,
    ):
        lock_id_str = stringify_id(lock["id"])
        lock_chambers = chambers_by_parent.get(
            lock_id_str, pd.DataFrame(columns=chambers.columns)
        )

        lock_isrs_code = _resolve_isrs_code(lock, isrs)
        ris_info = _resolve_ris_info(lock_isrs_code, ris_df)

        # Resolve associated bridge openings FIRST to allow dynamic buffer calculation
        openings_data = _resolve_openings_optimized(
            lock, lock_chambers, bridges_by_complex, openings_by_parent, op_times_map
        )

        fairway_data, chamber_routes = _resolve_fairway_data(
            lock, lock_chambers, fairways, sections_gdf, openings_data=openings_data
        )

        logger.debug("  Checking connected fairways and sections...")
        sections_data, internal_sections, connected_fairways = (
            _find_connected_sections_optimized(
                lock,
                lock_chambers,
                sections_gdf,
                sections_rd,
                fairway_data,
                network_graph,
            )
        )

        logger.debug(
            "  Finding nearby berths (allowed fairways: %s)...", connected_fairways
        )
        berths_data = utils.find_nearby_berths(
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

        # openings_data resolved earlier for fairway buffer calculation

        lock_id = stringify_id(lock["id"])
        lock_op_times = None
        if pd.notna(lock["operating_times_id"]):
            op_id = stringify_id(lock["operating_times_id"])
            lock_op_times = op_times_map.get(op_id)

        # Find FIS junction nodes that lie physically inside each chamber polygon.
        # These are stored in the chamber dict so that build_graph_features can
        # insert them as intermediate nodes on the chamber_route edge.
        internal_junctions_by_chamber = _find_internal_junctions_for_chambers(
            lock_chambers, network_graph
        )

        complex_obj = {
            **lock_attrs,
            "id": lock_id,
            "name": lock["name"],
            "isrs_code": lock_isrs_code,
            # complex_group_id identifies which multi-lock complex this lock belongs
            # to (from the detect_complex_groups pre-pass).  For single-lock complexes
            # this equals lock_id.  build_graph_features uses it to name shared
            # split/merge nodes in multi-branch complexes.
            "complex_group_id": lock_to_group.get(lock_id, lock_id),
            **ris_info,
            **fairway_data,
            "berths": berths_data,
            "sections": sections_data,
            "disk_locks": matched_disk_locks,
            "disk_bridges": matched_disk_bridges,
            "disk_complex_id": disk_complex_id,
            "disk_complex_name": disk_complex_name,
            "operating_times": lock_op_times,
            "openings": openings_data,
            "locks": [
                {
                    "id": lock_id,
                    "name": lock["name"],
                    **lock_attrs,
                    "operating_times": lock_op_times,
                    "chambers": _build_chamber_objects_optimized(
                        lock_chambers,
                        chamber_routes,
                        subchambers_by_parent,
                        op_times_map,
                        internal_junctions_by_chamber=internal_junctions_by_chamber,
                    ),
                }
            ],
        }

        complexes.append(complex_obj)

    return complexes


def detect_complex_groups(
    locks: pd.DataFrame,
    sections_gdf: pd.DataFrame,
) -> dict:
    """
    Group FIS locks that form a single navigational complex.

    Two locks belong to the same complex group when they share at least one
    boundary junction node (``start_junction_id`` / ``end_junction_id`` on their
    fairway sections).  This captures multi-branch complexes like Oranjesluizen
    where the fairway forks before the individual lock chambers so they must
    share the same upstream (split) and downstream (merge) nodes in the graph.

    Returns:
        A dict mapping ``complex_group_id → [lock_id, ...]`` where
        ``complex_group_id`` is the string ID of the lock with the lowest
        numeric ID in the group (or the first alphabetically for ISRS codes).

    Example::

        {
            "50750": ["50750", "59464015"],  # Oranjesluizen
            "49032": ["49032"],              # Weurt (single lock, no partner)
        }
    """
    if locks.empty or sections_gdf.empty:
        return {(sid := stringify_id(lid)): [sid] for lid in locks["id"]}

    # Build: lock_id → set of junction IDs on its fairway sections
    # Only direct boundary junctions (start/end of every section on the lock's
    # fairway) are used.  No neighbour expansion is performed so that locks that
    # merely share a nearby junction are not incorrectly grouped.
    lock_junctions: dict[str, set] = {}
    for _, lock in locks.iterrows():
        lid = stringify_id(lock["id"])
        fid = stringify_id(lock.get("fairway_id"))
        if not fid:
            lock_junctions[lid] = set()
            continue
        fw_secs = sections_gdf[sections_gdf["fairway_id"] == fid]
        junctions: set = set()
        for _, sec in fw_secs.iterrows():
            for jcol in ("start_junction_id", "end_junction_id"):
                j = stringify_id(sec.get(jcol))
                if j:
                    junctions.add(j)
        lock_junctions[lid] = junctions

    # Union-Find grouping
    parent: dict[str, str] = {lid: lid for lid in lock_junctions}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x: str, y: str) -> None:
        rx, ry = find(x), find(y)
        if rx != ry:
            # Keep the smaller ID as the group representative
            try:
                if int(rx) > int(ry):
                    rx, ry = ry, rx
            except (ValueError, TypeError):
                if rx > ry:
                    rx, ry = ry, rx
            parent[ry] = rx

    lock_ids = list(lock_junctions)
    for i, lid_a in enumerate(lock_ids):
        for lid_b in lock_ids[i + 1 :]:
            if lock_junctions[lid_a] & lock_junctions[lid_b]:
                union(lid_a, lid_b)

    # Collect groups
    groups: dict[str, list] = {}
    for lid in lock_ids:
        root = find(lid)
        groups.setdefault(root, []).append(lid)

    return groups
