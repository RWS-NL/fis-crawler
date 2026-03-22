import logging
from typing import List, Dict
import pandas as pd
from shapely import wkt
from shapely.geometry import Point, mapping, LineString
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


def generate_simplified_passages(
    complexes: List[Dict], structure_type: str
) -> List[Dict]:
    """
    Generates simplified node and edge features for a list of structures.
    Used in 'simplified' mode or for standalone structures.
    """
    features = []
    for comp in complexes:
        cid = utils.stringify_id(comp["id"])
        bwkt = comp.get("geometry_before_wkt")
        awkt = comp.get("geometry_after_wkt")

        if not bwkt or not awkt:
            logger.debug(
                f"Skipping simplified passage for {structure_type} {cid}: missing split/merge wkt"
            )
            continue

        geom_before = wkt.loads(bwkt)
        geom_after = wkt.loads(awkt)

        pt_split = Point(geom_before.coords[-1])
        pt_merge = Point(geom_after.coords[0])
        line_passage = LineString([pt_split, pt_merge])

        split_id = f"{structure_type}_{cid}_split"
        merge_id = f"{structure_type}_{cid}_merge"

        # Common properties for nodes and edges
        base_props = {
            "structure_type": structure_type,
            "structure_id": cid,
            "name": comp.get("name", comp.get("name")),
        }

        # 1. Create split node
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(pt_split),
                "properties": base_props
                | {
                    "id": split_id,
                    "feature_type": "node",
                    "node_type": f"{structure_type}_split",
                },
            }
        )

        # 2. Create merge node
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(pt_merge),
                "properties": base_props
                | {
                    "id": merge_id,
                    "feature_type": "node",
                    "node_type": f"{structure_type}_merge",
                },
            }
        )

        # Aggregate constraints
        agg_constraints = {}
        constituent_ids = []

        if structure_type == "bridge":
            widths, heights = [], []
            for op in comp.get("openings", []):
                constituent_ids.append(str(op["id"]))
                w, h = op.get("dim_width"), op.get("dim_height")
                if pd.notna(w):
                    widths.append(float(w))
                if pd.notna(h):
                    heights.append(float(h))

            if widths:
                agg_constraints["dim_width"] = min(widths)
            if heights:
                agg_constraints["dim_height"] = min(heights)

        elif structure_type == "lock":
            widths, lengths = [], []
            for child in comp.get("locks", []):
                for ch in child.get("chambers", []):
                    constituent_ids.append(str(ch["id"]))
                    w, ch_len = ch.get("dim_width"), ch.get("dim_length")
                    if pd.notna(w):
                        widths.append(float(w))
                    if pd.notna(ch_len):
                        lengths.append(float(ch_len))

            if widths:
                agg_constraints["dim_width"] = max(widths)
            if lengths:
                agg_constraints["dim_length"] = max(lengths)

        # 3. Create passage edge
        sections = comp.get("sections", [])
        best_sec_id = None
        if sections:
            best_sec = next(
                (s for s in sections if s.get("relation") == "overlap"), sections[0]
            )
            best_sec_id = best_sec.get("id")

        edge_props = base_props | {
            "id": f"{structure_type}_passage_{cid}",
            "feature_type": "fairway_segment",
            "segment_type": f"{structure_type}_passage",
            "section_id": best_sec_id,
            "source_node": split_id,
            "target_node": merge_id,
            "length_m": geod.geometry_length(line_passage),
            "constituent_ids": ",".join(constituent_ids),
        }
        edge_props.update(agg_constraints)

        features.append(
            {
                "type": "Feature",
                "geometry": mapping(line_passage),
                "properties": edge_props,
            }
        )

    return features
