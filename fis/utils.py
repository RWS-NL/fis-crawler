import logging
import pandas as pd
from shapely.ops import nearest_points

logger = logging.getLogger(__name__)


def process_fairway_geometry(fw_obj, lock):
    """
    Process fairway geometry relative to a lock.
    Splits the fairway into 'before' and 'after' segments based on the lock's location.
    """
    fairway_data = {}

    if hasattr(fw_obj, "geometry") and fw_obj.geometry:
        # Project lock to fairway
        # Note: Lock geometry is usually a Point (Complex Centroid)
        if hasattr(lock, "geometry") and lock.geometry:
            try:
                # Snap lock to fairway
                fairway_geom = fw_obj.geometry
                lock_geom = lock.geometry

                # Projection distance check
                dist = fairway_geom.distance(lock_geom)
                fairway_data["lock_to_fairway_distance"] = dist

                # Project
                projected_dist = fairway_geom.project(lock_geom)
                fairway_geom.interpolate(projected_dist)

                # Split geometry
                # Simple approach: substring
                # But shapely substring handles distance along line

                # Determine Km range
                float(fw_obj["RouteKmBegin"])
                float(fw_obj["RouteKmEnd"])
                fw_obj["Direction"]  # N/A or similar?

                # Using shapely substring for geometry
                # projected_dist is from the start of the LINESTRING, not necessarily KM

                geom_before = None
                geom_after = None

                total_length = fairway_geom.length

                if 0 < projected_dist < total_length:
                    from shapely.ops import substring

                    geom_before = substring(fairway_geom, 0, projected_dist)
                    geom_after = substring(fairway_geom, projected_dist, total_length)

                if geom_before:
                    fairway_data["geometry_before_wkt"] = geom_before.wkt
                if geom_after:
                    fairway_data["geometry_after_wkt"] = geom_after.wkt

            except Exception as e:
                logger.warning(
                    f"Error processing fairway geometry for lock {lock['Id']}: {e}"
                )

    return fairway_data


def find_nearby_berths(
    lock, berths_gdf, geometry_before_wkt=None, geometry_after_wkt=None
):
    """
    Find berths that are spatialy related to the lock.
    """
    nearby_berths = []

    # 1. Filter by FairwayId if available
    lock_fairway_id = lock.get("FairwayId")
    if pd.notna(lock_fairway_id):
        # Allow loose matching or exact?
        # Berths might be on the same fairway
        candidates = berths_gdf[berths_gdf["FairwayId"] == lock_fairway_id]

        # 2. Spatial filter (Buffer around lines/lock?)
        # Or just return all on fairway for now

        for _, berth in candidates.iterrows():
            # Basic info
            b_obj = {
                "id": int(berth["Id"]),
                "name": berth["Name"],
                "geometry": berth.geometry.wkt
                if hasattr(berth, "geometry") and berth.geometry
                else None,
            }
            nearby_berths.append(b_obj)

    return nearby_berths


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

    # Use nearest_points via ops or manual distance minimization

    # Door 1: Nearest to split_point (upstream/start)
    door_start = nearest_points(target_geom, split_point)[0]

    # Door 2: Nearest to merge_point (downstream/end)
    door_end = nearest_points(target_geom, merge_point)[0]

    return door_start, door_end
