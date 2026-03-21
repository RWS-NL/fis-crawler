import pathlib
from typing import List, Dict, Any
import logging
import pandas as pd
import geopandas as gpd
from shapely import wkt
from tqdm import tqdm

from shapely.geometry import Point, LineString
from shapely.ops import unary_union
from fis.utils import to_python, sanitize_attrs
from fis import settings, utils

logger = logging.getLogger(__name__)


def load_data(export_dir: pathlib.Path, disk_dir: pathlib.Path):
    """Load necessary parquet files and normalize attributes."""

    def read_geo_or_parquet(dir_path, stem):
        gpq = dir_path / f"{stem}.geoparquet"
        pq = dir_path / f"{stem}.parquet"
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
        if pq.exists():
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
    bridges = read_geo_or_parquet(export_dir, "bridge")
    openings = read_geo_or_parquet(export_dir, "opening")

    # Normalize bridges/openings if present
    if bridges is not None:
        bridges = utils.normalize_attributes(bridges, "bridges", schema)
    if openings is not None:
        openings = utils.normalize_attributes(openings, "openings", schema)

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
                for _, lock_row in disk_locks_rd[lock_mask_strict].iterrows():
                    matched_disk_locks.append(sanitize_attrs(lock_row))

            # 2. If NO locks matched via chambers, fallback to settings buffer
            if not matched_disk_locks and lock_geom_rd:
                lock_buffer_rd = lock_geom_rd.buffer(settings.DISK_MATCH_BUFFER_LOCK_M)
                lock_mask_loose = disk_locks_rd.intersects(lock_buffer_rd)
                for _, lock_row in disk_locks_rd[lock_mask_loose].iterrows():
                    matched_disk_locks.append(sanitize_attrs(lock_row))

    return matched_disk_locks, matched_disk_bridges


def find_fairway_junctions(sections_gdf, fairway_id):
    """
    Identify start and end junctions for a given fairway based on sections.
    """
    start_junction = None
    end_junction = None

    if sections_gdf is None:
        return start_junction, end_junction

    fw_sections = sections_gdf[sections_gdf["fairway_id"] == int(fairway_id)]
    if fw_sections.empty:
        return start_junction, end_junction

    fw_sections = fw_sections.sort_values("route_km_begin")

    if pd.notna(fw_sections.iloc[0]["start_junction_id"]):
        start_junction = int(fw_sections.iloc[0]["start_junction_id"])

    if pd.notna(fw_sections.iloc[-1]["end_junction_id"]):
        end_junction = int(fw_sections.iloc[-1]["end_junction_id"])

    return start_junction, end_junction


def _resolve_isrs_code(lock, isrs):
    if pd.notna(lock.get("isrs_id")) and isrs is not None:
        isrs_row = isrs[isrs["id"] == lock["isrs_id"]]
        if not isrs_row.empty:
            return isrs_row.iloc[0]["code"]
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


def _resolve_fairway_data(
    lock, lock_chambers, fairways, sections_gdf, openings_data=None
):
    fairway_data = {}
    chamber_routes = {}
    if fairways is not None and pd.notna(lock.get("fairway_id")):
        fw_row = fairways[fairways["id"] == lock["fairway_id"]]
        if not fw_row.empty:
            fw_obj = fw_row.iloc[0]
            fairway_data = {
                "fairway_name": fw_obj["name"],
                "fairway_id": int(fw_obj["id"]),
            }
            max_length = 0
            if "dim_length" in lock_chambers.columns:
                max_length = lock_chambers["dim_length"].max()
            if pd.isna(max_length):
                max_length = 0

            buffer_dist = (max_length / 2) + 50
            geom_data = utils.process_fairway_geometry(
                fw_obj, lock, buffer_dist=buffer_dist, openings_data=openings_data
            )
            fairway_data.update(geom_data)

            if sections_gdf is not None:
                s_junc, e_junc = find_fairway_junctions(sections_gdf, int(fw_obj["id"]))
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
    matched_section_ids = set()

    if fairway_data.get("fairway_id"):
        connected_fairways.add(fairway_data["fairway_id"])

    # 1. Attribute-based matching (section_id, fairway_id)
    if sections_gdf is not None:
        fsid = lock.get("section_id")
        if pd.notna(fsid):
            matches = sections_gdf[sections_gdf["id"] == int(fsid)]
            for _, s_row in matches.iterrows():
                sid = int(s_row["id"])
                if sid not in matched_section_ids:
                    fid = (
                        int(s_row["fairway_id"])
                        if pd.notna(s_row.get("fairway_id"))
                        else None
                    )
                    internal_sections.add(sid)
                    if fid:
                        connected_fairways.add(fid)
                    sections_data.append(
                        {
                            "id": sid,
                            "name": s_row["name"],
                            "fairway_id": fid,
                            "length": float(s_row["dim_length"])
                            if pd.notna(s_row.get("dim_length"))
                            else None,
                            "geometry": s_row.geometry.wkt
                            if hasattr(s_row, "geometry") and s_row.geometry
                            else None,
                            "relation": "direct",
                        }
                    )
                    matched_section_ids.add(sid)

    if network_graph:
        for j_id in [
            fairway_data.get("start_junction_id"),
            fairway_data.get("end_junction_id"),
        ]:
            if j_id and network_graph.has_node(j_id):
                for nbr in network_graph.neighbors(j_id):
                    edge_data = network_graph.get_edge_data(j_id, nbr)
                    if edge_data and "FairwayId" in edge_data:
                        connected_fairways.add(int(edge_data["fairway_id"]))

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
            complex_union = unary_union([g for g in complex_geoms if g])
            if complex_union:
                buffered_union = complex_union.buffer(
                    settings.LOCK_SECTION_MATCH_BUFFER_DEG
                )
                intersecting = sections_gdf[sections_gdf.intersects(buffered_union)]

                for _, s_row in intersecting.iterrows():
                    sid = int(s_row["id"])
                    if sid in matched_section_ids:
                        continue

                    fid = (
                        int(s_row["fairway_id"])
                        if pd.notna(s_row.get("fairway_id"))
                        else None
                    )

                    internal_sections.add(sid)
                    if fid:
                        connected_fairways.add(fid)

                    sections_data.append(
                        {
                            "id": sid,
                            "name": s_row["name"],
                            "fairway_id": fid,
                            "length": float(s_row["dim_length"])
                            if pd.notna(s_row.get("dim_length"))
                            else None,
                            "geometry": s_row.geometry.wkt
                            if hasattr(s_row, "geometry") and s_row.geometry
                            else None,
                            "relation": "overlap",
                        }
                    )
                    matched_section_ids.add(sid)
    return sections_data, internal_sections, connected_fairways


def _resolve_openings(lock, lock_chambers, bridges, openings, op_times_map):
    """
    Find openings associated with a lock. This checks:
    1. Openings directly parented to the Lock (parent_id = lock "id").
    2. Openings directly parented to any of the Lock's chambers.
    3. Openings parented to a Bridge that shares the lock's RelatedBuildingComplexName.
    """
    openings_data = []
    if openings is None or openings.empty:
        return openings_data

    # Find relevant Parent Ids
    parent_ids = {int(lock["id"])}
    if "id" in lock_chambers.columns:
        for cid in lock_chambers["id"].dropna():
            parent_ids.add(int(cid))

    if bridges is not None and not bridges.empty:
        lock_complex_name = lock.get("RelatedBuildingComplexName")
        if pd.notna(lock_complex_name):
            # Find bridges belonging to the same complex
            matching_bridges = bridges[
                bridges["RelatedBuildingComplexName"] == lock_complex_name
            ]
            for bid in matching_bridges["id"].dropna():
                parent_ids.add(int(bid))

    # Filter openings mapped to any of these parents
    matched_openings = openings[openings["parent_id"].isin(parent_ids)]
    for _, opening_row in matched_openings.iterrows():
        op_attrs = sanitize_attrs(opening_row)
        op_id = int(opening_row["id"])

        # Attach operating times to the opening
        operating_times = None
        if pd.notna(opening_row.get("OperatingTimesId")):
            ot_id = int(opening_row["OperatingTimesId"])
            operating_times = op_times_map.get(ot_id)

        op_attrs.update(
            {
                "id": op_id,
                "name": opening_row.get("name"),
                "operating_times": operating_times,
            }
        )
        openings_data.append(op_attrs)

    return openings_data


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

        chamber_id = int(chamber["id"])
        chamber_op_times = None
        if pd.notna(chamber.get("OperatingTimesId")):
            op_id = int(chamber["OperatingTimesId"])
            chamber_op_times = op_times_map.get(op_id)

        c_obj = {
            **chamber_attrs,
            "id": chamber_id,
            "name": chamber["name"],
            "length": float(chamber["dim_length"])
            if pd.notna(chamber["dim_length"])
            else None,
            "width": float(chamber["dim_width"])
            if pd.notna(chamber["dim_width"])
            else None,
            "route_geometry": route_wkt,
            "operating_times": chamber_op_times,
        }

        if subchambers is not None:
            chamber_subchambers = subchambers[subchambers["parent_id"] == chamber["id"]]
            c_obj["subchambers"] = []
            for _, sc in chamber_subchambers.iterrows():
                sc_obj = sanitize_attrs(sc)
                c_obj["subchambers"].append(sc_obj)

        chambers_list.append(c_obj)
    return chambers_list


def group_complexes(data: Dict[str, Any], network_graph=None) -> List[Dict]:
    """
    Group locks into complexes and enrich with ISRS, RIS, Fairway, Berth, Section, and DISK data.
    """
    locks = data.get("locks")
    chambers = data.get("chambers")
    subchambers = data.get("subchambers")
    isrs = data.get("isrs")
    ris_df = data.get("ris_df")
    fairways = data.get("fairways")
    berths = data.get("berths")
    sections = data.get("sections")
    disk_locks = data.get("disk_locks")
    disk_bridges = data.get("disk_bridges")
    operatingtimes = data.get("operatingtimes")
    bridges = data.get("bridges")
    openings = data.get("openings")
    complexes = []

    # Expect GeoDataFrames at this stage
    locks_gdf = locks
    berths_gdf = berths
    sections_gdf = sections

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
        disk_locks_rd = disk_locks.to_crs(settings.PROJECTED_CRS)

    disk_bridges_rd = None
    if (
        disk_bridges is not None
        and not disk_bridges.empty
        and "geometry" in disk_bridges.columns
    ):
        disk_bridges_rd = disk_bridges.to_crs(settings.PROJECTED_CRS)

    # Pre-process operating times
    op_times_map = {}
    if operatingtimes is not None and not operatingtimes.empty:
        for _, row in operatingtimes.iterrows():
            if pd.notna(row.get("id")):
                op_id = int(row["id"])
                op_times_map[op_id] = {
                    "NormalSchedules": to_python(row.get("NormalSchedules")) or [],
                    "HolidaySchedules": to_python(row.get("HolidaySchedules")) or [],
                    "ExceptionSchedules": to_python(row.get("ExceptionSchedules"))
                    or [],
                }

    for idx, lock in tqdm(
        locks_gdf.iterrows(),
        total=len(locks_gdf),
        desc="Processing locks",
        mininterval=2.0,
    ):
        lock_chambers = chambers[chambers["parent_id"] == lock["id"]]
        lock_isrs_code = _resolve_isrs_code(lock, isrs)
        ris_info = _resolve_ris_info(lock_isrs_code, ris_df)

        # Resolve associated bridge openings FIRST to allow dynamic buffer calculation
        openings_data = _resolve_openings(
            lock, lock_chambers, bridges, openings, op_times_map
        )

        fairway_data, chamber_routes = _resolve_fairway_data(
            lock, lock_chambers, fairways, sections_gdf, openings_data=openings_data
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

        lock_id = int(lock["id"])
        lock_op_times = None
        if pd.notna(lock.get("OperatingTimesId")):
            op_id = int(lock["OperatingTimesId"])
            lock_op_times = op_times_map.get(op_id)

        complex_obj = {
            **lock_attrs,
            "id": lock_id,
            "name": lock["name"],
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
            "openings": openings_data,
            "locks": [
                {
                    "id": lock_id,
                    "name": lock["name"],
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
