import logging
from typing import List, Dict
import pandas as pd
from shapely import wkt
from shapely.geometry import Point, mapping, LineString
from pyproj import Geod

from fis import utils

logger = logging.getLogger(__name__)
geod = Geod(ellps="WGS84")


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
