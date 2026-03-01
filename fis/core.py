import logging
import pathlib
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, LineString
from shapely.ops import unary_union

from fis.utils import process_fairway_geometry, find_nearby_berths
from fis.graph import find_fairway_junctions

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
    complexes = []

    # Convert locks to GeoDataFrame for spatial ops if needed
    if "Geometry" in locks.columns and locks["Geometry"].dtype == "object":
        locks["geometry"] = locks["Geometry"].apply(
            lambda x: wkt.loads(x) if x else None
        )
    locks_gdf = gpd.GeoDataFrame(locks, geometry="geometry")

    # Convert berths to GDF if needed
    berths_gdf = None
    if berths is not None:
        if "Geometry" in berths.columns and berths["Geometry"].dtype == "object":
            berths["geometry"] = berths["Geometry"].apply(
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
        if "Geometry" in sections.columns and sections["Geometry"].dtype == "object":
            sections["geometry"] = sections["Geometry"].apply(
                lambda x: wkt.loads(x) if x else None
            )
        sections_gdf = (
            gpd.GeoDataFrame(sections, geometry="geometry")
            if "geometry" in sections.columns
            else sections
        )

    for idx, lock in locks_gdf.iterrows():
        # Get chambers for this lock (using confirmed ParentId key)
        lock_chambers = chambers[chambers["ParentId"] == lock["Id"]]

        # Resolve ISRS
        lock_isrs_code = None
        if pd.notna(lock.get("IsrsId")) and isrs is not None:
            isrs_row = isrs[isrs["Id"] == lock["IsrsId"]]
            if not isrs_row.empty:
                lock_isrs_code = isrs_row.iloc[0]["Code"]

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
        if fairways is not None and pd.notna(lock.get("FairwayId")):
            fw_row = fairways[fairways["Id"] == lock["FairwayId"]]
            if not fw_row.empty:
                fw_obj = fw_row.iloc[0]
                fairway_data = {
                    "fairway_name": fw_obj["Name"],
                    "fairway_id": int(fw_obj["Id"]),
                }
                # Delegate complexity to helper function
                geom_data = process_fairway_geometry(fw_obj, lock)
                fairway_data.update(geom_data)

                # Junction Identification
                start_junction, end_junction = find_fairway_junctions(
                    sections_gdf, fw_obj["Id"]
                )

                fairway_data["start_junction_id"] = start_junction
                fairway_data["end_junction_id"] = end_junction

        # Chamber Route Generation (Virtual Fairways)
        chamber_routes = {}
        if (
            "geometry_before_wkt" in fairway_data
            and "geometry_after_wkt" in fairway_data
        ):
            try:
                bwkt = fairway_data["geometry_before_wkt"]
                awkt = fairway_data["geometry_after_wkt"]
                if bwkt and awkt:
                    # Load geometries
                    g_before = wkt.loads(bwkt)
                    g_after = wkt.loads(awkt)

                    split_point = Point(g_before.coords[-1])
                    merge_point = Point(g_after.coords[0])

                    chamber_routes["split_point"] = split_point
                    chamber_routes["merge_point"] = merge_point
            except Exception as e:
                logger.warning(
                    f"Failed to generate chamber route endpoints for lock {lock['Id']}: {e}"
                )

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
            if "Geometry" in lock_chambers.columns:
                for _, c_row in lock_chambers.iterrows():
                    if pd.notna(c_row["Geometry"]):
                        try:
                            c_geom = wkt.loads(c_row["Geometry"])
                            complex_geoms.append(c_geom)
                        except Exception:
                            pass

            if complex_geoms:
                complex_union = unary_union([g for g in complex_geoms if g])
                if complex_union:
                    intersecting = sections_gdf[sections_gdf.intersects(complex_union)]

                    for _, s_row in intersecting.iterrows():
                        sections_data.append(
                            {
                                "id": int(s_row["Id"]),
                                "name": s_row["Name"],
                                "fairway_id": int(s_row["FairwayId"])
                                if pd.notna(s_row.get("FairwayId"))
                                else None,
                                "length": float(s_row["Length"])
                                if pd.notna(s_row.get("Length"))
                                else None,
                                "geometry": s_row.geometry.wkt
                                if hasattr(s_row, "geometry") and s_row.geometry
                                else None,
                                "relation": "overlap",
                            }
                        )

        complex_obj = {
            "id": int(lock["Id"]),
            "name": lock["Name"],
            "isrs_code": lock_isrs_code,
            "geometry": lock.geometry.wkt
            if hasattr(lock, "geometry") and lock.geometry
            else None,
            **ris_info,
            **fairway_data,
            "berths": berths_data,
            "sections": sections_data,
            "locks": [{"id": int(lock["Id"]), "name": lock["Name"], "chambers": []}],
        }

        # Add chambers
        for _, chamber in lock_chambers.iterrows():
            # Add Chamber Route (Virtual Fairway)
            route_wkt = None
            if "split_point" in chamber_routes and "merge_point" in chamber_routes:
                try:
                    if "Geometry" in chamber and pd.notna(chamber["Geometry"]):
                        ch_geom = wkt.loads(chamber["Geometry"])
                        centroid = ch_geom.centroid
                        route = LineString(
                            [
                                chamber_routes["split_point"],
                                centroid,
                                chamber_routes["merge_point"],
                            ]
                        )
                        route_wkt = route.wkt
                except Exception:
                    pass

            c_obj = {
                "id": int(chamber["Id"]),
                "name": chamber["Name"],
                "length": float(chamber["Length"])
                if pd.notna(chamber["Length"])
                else None,
                "width": float(chamber["Width"])
                if pd.notna(chamber["Width"])
                else None,
                "geometry": chamber["Geometry"]
                if "Geometry" in chamber and pd.notna(chamber["Geometry"])
                else None,
                "route_geometry": route_wkt,
            }
            complex_obj["locks"][0]["chambers"].append(c_obj)

        complexes.append(complex_obj)

    return complexes
