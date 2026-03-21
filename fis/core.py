import logging
import pathlib
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, LineString
from shapely.ops import unary_union

from fis.utils import process_fairway_geometry, find_nearby_berths, sanitize_attrs
from fis.lock.core import find_fairway_junctions

logger = logging.getLogger(__name__)


def load_data(export_dir: pathlib.Path):
    """Load necessary parquet files."""

    def read_geo_or_parquet(stem):
        gpq = export_dir / f"{stem}.geoparquet"
        pq = export_dir / f"{stem}.parquet"
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

    locks = read_geo_or_parquet("lock")
    chambers = read_geo_or_parquet("chamber")
    isrs = read_geo_or_parquet("isrs")
    fairways = read_geo_or_parquet("fairway")
    berths = read_geo_or_parquet("berth")
    sections = read_geo_or_parquet("section")

    if locks is None or chambers is None:
        raise FileNotFoundError("Missing essential lock/chamber data.")

    return locks, chambers, isrs, fairways, berths, sections


def group_complexes(locks, chambers, isrs, ris_df, fairways, berths, sections):
    """
    Group locks into complexes and enrich with ISRS, RIS, Fairway, Berth, and Section data.
    """
    from fis import utils

    # Normalize all inputs
    schema = utils.load_schema()
    locks = utils.normalize_attributes(locks, "locks", schema)
    chambers = utils.normalize_attributes(chambers, "chambers", schema)
    if isrs is not None:
        isrs = utils.normalize_attributes(isrs, "isrs", schema)
    if fairways is not None:
        fairways = utils.normalize_attributes(fairways, "fairways", schema)
    if berths is not None:
        berths = utils.normalize_attributes(berths, "berths", schema)
    if sections is not None:
        sections = utils.normalize_attributes(sections, "sections", schema)

    complexes = []

    # Convert locks to GeoDataFrame for spatial ops if needed
    if "geometry" not in locks.columns and "Geometry" in locks.columns:
        locks["geometry"] = locks["Geometry"].apply(
            lambda x: wkt.loads(x) if isinstance(x, str) else x
        )
    locks_gdf = gpd.GeoDataFrame(locks, geometry="geometry")

    # Convert berths to GDF if needed
    berths_gdf = None
    if berths is not None:
        if "geometry" not in berths.columns and "Geometry" in berths.columns:
            berths["geometry"] = berths["Geometry"].apply(
                lambda x: wkt.loads(x) if isinstance(x, str) else x
            )
        berths_gdf = (
            gpd.GeoDataFrame(berths, geometry="geometry")
            if "geometry" in berths.columns
            else berths
        )

    # Convert sections to GDF if needed
    sections_gdf = None
    if sections is not None:
        if "geometry" not in sections.columns and "Geometry" in sections.columns:
            sections["geometry"] = sections["Geometry"].apply(
                lambda x: wkt.loads(x) if isinstance(x, str) else x
            )
        sections_gdf = (
            gpd.GeoDataFrame(sections, geometry="geometry")
            if "geometry" in sections.columns
            else sections
        )

    for idx, lock in locks_gdf.iterrows():
        # Get chambers for this lock
        lock_chambers = chambers[chambers["parent_id"] == lock["id"]]

        # Resolve ISRS
        lock_isrs_code = None
        if pd.notna(lock.get("isrs_id")) and isrs is not None:
            isrs_row = isrs[isrs["id"] == lock["isrs_id"]]
            if not isrs_row.empty:
                lock_isrs_code = isrs_row.iloc[0]["code"]

        # RIS Enrichment
        ris_info = {}
        if lock_isrs_code and ris_df is not None:
            match = ris_df[ris_df["isrs_code"] == lock_isrs_code]
            if not match.empty:
                ris_info = {
                    "ris_name": match.iloc[0]["name"],
                    "ris_function": match.iloc[0]["function"],
                }

        # Fairway Mapping
        fairway_data = {}
        fw_obj = None  # Keep reference for processing
        if fairways is not None and pd.notna(lock.get("fairway_id")):
            fw_row = fairways[fairways["id"] == lock["fairway_id"]]
            if not fw_row.empty:
                fw_obj = fw_row.iloc[0]
                fairway_data = {
                    "fairway_name": fw_obj["name"],
                    "fairway_id": int(fw_obj["id"]),
                }
                # Delegate complexity to helper function
                geom_data = process_fairway_geometry(fw_obj, lock)
                fairway_data.update(geom_data)

                # Junction Identification
                start_junction, end_junction = find_fairway_junctions(
                    sections_gdf, fw_obj["id"]
                )

                fairway_data["start_junction_id"] = start_junction
                fairway_data["end_junction_id"] = end_junction

        # Chamber Route Generation (Virtual Fairways)
        chamber_routes = {}
        bwkt = fairway_data.get("geometry_before_wkt")
        awkt = fairway_data.get("geometry_after_wkt")
        if bwkt and awkt:
            # Load geometries
            g_before = wkt.loads(bwkt)
            g_after = wkt.loads(awkt)

            chamber_routes["split_point"] = Point(g_before.coords[-1])
            chamber_routes["merge_point"] = Point(g_after.coords[0])

        # Berth Identification
        berths_data = []
        if berths_gdf is not None:
            berths_data = find_nearby_berths(
                lock,
                berths_gdf,
                fairway_data.get("geometry_before_wkt"),
                fairway_data.get("geometry_after_wkt"),
            )

        # Section Overlap Identification
        sections_data = []
        if sections_gdf is not None:
            # Define complex geometry: Union of lock + chambers
            # Start with lock geometry
            complex_geoms = (
                [lock.geometry] if hasattr(lock, "geometry") and lock.geometry else []
            )

            # Add chamber geometries
            if "geometry" in lock_chambers.columns:
                for _, c_row in lock_chambers.iterrows():
                    if pd.isna(c_row["geometry"]):
                        continue
                    c_geom = (
                        wkt.loads(c_row["geometry"])
                        if isinstance(c_row["geometry"], str)
                        else c_row["geometry"]
                    )
                    complex_geoms.append(c_geom)

            if complex_geoms:
                complex_union = unary_union([g for g in complex_geoms if g])
                if complex_union:
                    intersecting = sections_gdf[sections_gdf.intersects(complex_union)]

                    for _, s_row in intersecting.iterrows():
                        s_attrs = sanitize_attrs(s_row)
                        s_attrs.update(
                            {
                                "id": int(s_row["id"]),
                                "name": s_row["name"],
                                "fairway_id": int(s_row["fairway_id"])
                                if pd.notna(s_row.get("fairway_id"))
                                else None,
                                "dim_length": float(s_row["length"])
                                if pd.notna(s_row.get("length"))
                                else None,
                                "relation": "overlap",
                            }
                        )
                        sections_data.append(s_attrs)

        lock_attrs = sanitize_attrs(lock)
        complex_obj = {
            **lock_attrs,
            "id": int(lock["id"]),
            "name": lock["name"],
            "isrs_code": lock_isrs_code,
            **ris_info,
            **fairway_data,
            "berths": berths_data,
            "sections": sections_data,
            "locks": [{"id": int(lock["id"]), "name": lock["name"], "chambers": []}],
        }

        # Add chambers
        for _, chamber in lock_chambers.iterrows():
            # Add Chamber Route (Virtual Fairway)
            route_wkt = None
            if (
                "split_point" in chamber_routes
                and "merge_point" in chamber_routes
                and "geometry" in chamber
                and pd.notna(chamber["geometry"])
            ):
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
            chamber_attrs.update(
                {
                    "id": int(chamber["id"]),
                    "name": chamber["name"],
                    "dim_length": float(chamber["dim_length"])
                    if pd.notna(chamber.get("dim_length"))
                    else None,
                    "dim_width": float(chamber["dim_width"])
                    if pd.notna(chamber.get("dim_width"))
                    else None,
                    "route_geometry": route_wkt,
                }
            )
            complex_obj["locks"][0]["chambers"].append(chamber_attrs)

        complexes.append(complex_obj)

    return complexes
