import logging
from typing import List, Dict
import geopandas as gpd
from shapely import wkt
from shapely.geometry import mapping, LineString
from pyproj import Geod

from fis import utils

logger = logging.getLogger(__name__)
geod = Geod(ellps="WGS84")


def generate_terminal_graph_features(terminals: List[Dict]) -> List[Dict]:
    """
    Generates node and edge features for terminals.
    Each terminal gets a node and an 'access' edge connecting it to the
    fairway junction node created during splicing.
    """
    features = []
    for term in terminals:
        raw_id = term.get("id", term.get("Id"))
        if raw_id is None:
            logger.warning("Skipping terminal without 'id'/'Id': %s", term)
            continue
        tid = utils.stringify_id(raw_id)
        conn_wkt = term.get("connection_geometry")
        if not conn_wkt:
            # Terminal was not mapped to a section or section was not spliced
            continue

        conn_pt = wkt.loads(conn_wkt)
        term_geom = term.get("geometry")
        if not term_geom:
            continue
        term_pt = wkt.loads(term_geom) if isinstance(term_geom, str) else term_geom
        if term_pt.geom_type != "Point":
            term_pt = term_pt.centroid

        # 1. Connection node on the fairway (where the split happened)
        conn_id = f"terminal_{tid}_connection"
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

        # 2. Terminal node itself
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(term_pt),
                "properties": {
                    "id": f"terminal_{tid}",
                    "feature_type": "node",
                    "node_type": "terminal",
                    "node_id": f"terminal_{tid}",
                    "name": term.get("Name"),
                    "terminal_id": tid,
                    "isrs_id": term.get("IsrsId"),
                },
            }
        )

        # 3. Access edge
        # LineString from connection point on fairway to terminal point
        access_line = LineString([conn_pt, term_pt])
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(access_line),
                "properties": {
                    "id": f"terminal_access_{tid}",
                    "feature_type": "fairway_segment",
                    "segment_type": "terminal_access",
                    "terminal_id": tid,
                    "source_node": conn_id,
                    "target_node": f"terminal_{tid}",
                    "length_m": geod.geometry_length(access_line),
                },
            }
        )

    return features


def build_terminals_gdf(terminals: List[Dict]) -> gpd.GeoDataFrame:
    """Builds a GeoDataFrame of terminals from the source dicts."""
    if not terminals:
        return None
    rows = []
    for term in terminals:
        row = term.copy()
        geom_wkt = row.get("geometry")
        if geom_wkt:
            row["geometry"] = (
                wkt.loads(geom_wkt) if isinstance(geom_wkt, str) else geom_wkt
            )
        rows.append(row)
    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
