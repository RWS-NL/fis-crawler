import logging
from typing import List, Dict
import geopandas as gpd
from shapely import wkt
from shapely.geometry import mapping, LineString
from pyproj import Geod

from fis import utils

logger = logging.getLogger(__name__)
geod = Geod(ellps="WGS84")


def generate_berth_graph_features(berths: List[Dict]) -> List[Dict]:
    """
    Generates node and edge features for berths.
    Each berth gets a node and an 'access' edge connecting it to the
    fairway junction node created during splicing.
    """
    features = []
    for berth in berths:
        raw_id = berth.get("id", berth.get("Id"))
        if raw_id is None:
            logger.warning("Skipping berth without 'id'/'Id': %s", berth)
            continue
        bid = utils.stringify_id(raw_id)
        conn_wkt = berth.get("connection_geometry")
        if not conn_wkt:
            continue

        conn_pt = wkt.loads(conn_wkt)
        berth_geom = berth.get("geometry")
        if not berth_geom:
            continue
        berth_pt = wkt.loads(berth_geom) if isinstance(berth_geom, str) else berth_geom
        if berth_pt.geom_type != "Point":
            berth_pt = berth_pt.centroid

        # 1. Connection node on the fairway
        conn_id = f"berth_{bid}_connection"
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(conn_pt),
                "properties": {
                    "id": conn_id,
                    "feature_type": "node",
                    "node_type": "junction",
                    "node_id": conn_id,
                },
            }
        )

        # 2. Berth node itself
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(berth_pt),
                "properties": {
                    "id": f"berth_{bid}",
                    "feature_type": "node",
                    "node_type": "berth",
                    "node_id": f"berth_{bid}",
                    "name": berth.get("Name"),
                    "berth_id": bid,
                    "isrs_id": berth.get("IsrsId"),
                },
            }
        )

        # 3. Access edge
        access_line = LineString([conn_pt, berth_pt])
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(access_line),
                "properties": {
                    "id": f"berth_access_{bid}",
                    "feature_type": "fairway_segment",
                    "segment_type": "berth_access",
                    "berth_id": bid,
                    "source_node": conn_id,
                    "target_node": f"berth_{bid}",
                    "length_m": geod.geometry_length(access_line),
                },
            }
        )

    return features


def build_berths_gdf(berths: List[Dict]) -> gpd.GeoDataFrame:
    """Builds a GeoDataFrame of berths from the source dicts."""
    if not berths:
        return None
    rows = []
    for berth in berths:
        row = berth.copy()
        geom_wkt = row.get("geometry")
        if geom_wkt:
            row["geometry"] = (
                wkt.loads(geom_wkt) if isinstance(geom_wkt, str) else geom_wkt
            )
        rows.append(row)
    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
