import logging
from typing import List, Dict

from shapely import wkt
from shapely.geometry import Point, LineString

from fis import settings

logger = logging.getLogger(__name__)


def identify_embedded_structures(
    lock_complexes: List[Dict], bridge_complexes: List[Dict]
) -> Dict[str, Dict]:
    """
    Identifies bridges that are functionally embedded within a lock complex.
    """
    from fis.lock.graph import build_chambers_gdf
    from fis.bridge.graph import build_openings_gdf

    chambers_gdf = build_chambers_gdf(lock_complexes)
    openings_gdf = build_openings_gdf(bridge_complexes)

    if chambers_gdf.empty or openings_gdf.empty:
        return {}

    openings_rd = openings_gdf.to_crs(settings.PROJECTED_CRS)
    chambers_rd = chambers_gdf.to_crs(settings.PROJECTED_CRS)

    all_candidates = []

    for _, op_row in openings_rd.iterrows():
        op_id = str(op_row["id"])
        op_name = str(op_row.get("name", op_row.get("Name", ""))).lower()
        for _, ch_row in chambers_rd.iterrows():
            ch_id = str(ch_row["id"])
            ch_name = str(ch_row.get("name", ch_row.get("Name", ""))).lower()
            dist = op_row.geometry.distance(ch_row.geometry)
            if dist > settings.EMBEDDED_STRUCTURE_MAX_DIST_M:
                continue
            score = _calculate_semantic_spatial_score(op_name, ch_name, dist)
            if score > 1.0:
                all_candidates.append((score, dist, op_id, ch_id, ch_row))

    all_candidates.sort(key=lambda x: (-x[0], x[1]))

    matches = {}
    matched_chs = set()

    for score, dist, op_id, ch_id, ch_row in all_candidates:
        if op_id not in matches and ch_id not in matched_chs:
            matches[op_id] = {"ch_id": ch_id, "ch_row": ch_row}
            matched_chs.add(ch_id)

    return matches


def _calculate_semantic_spatial_score(op_name: str, ch_name: str, dist: float) -> float:
    score = 0.0
    if op_name and ch_name:
        keywords = [
            "oost",
            "west",
            "midden",
            "zuid",
            "noord",
            "klein",
            "groot",
            "boven",
            "beneden",
            "hoofd",
            "jacht",
            "spui",
        ]
        for kw in keywords:
            if kw in op_name and kw in ch_name:
                score += 10.0
        if op_name in ch_name or ch_name in op_name:
            score += 5.0
    dist_score = max(0.0, 5.0 - (dist / 100.0))
    score += dist_score
    return score


def inject_embedded_bridges(
    features: List[Dict],
    lock_complexes: List[Dict],
    bridge_complexes: List[Dict],
    embedded_bridges: Dict[str, Dict],
) -> List[Dict]:
    """
    Injects embedded bridges directly into their governing lock chamber graphs.
    """
    if not embedded_bridges:
        return features

    logger.info(
        "Injecting %d embedded bridges into lock chambers.", len(embedded_bridges)
    )
    chamber_edges, opening_geoms = _index_chamber_routes_and_openings(
        features, embedded_bridges
    )

    new_features = []
    items_to_remove = set()

    for op_id, match_data in embedded_bridges.items():
        ch_id = match_data["ch_id"]
        if ch_id not in chamber_edges or op_id not in opening_geoms:
            continue

        op_geom = opening_geoms[op_id]

        best_edge = None
        best_dist = float("inf")
        best_line_geom = None

        priority = {"chamber_approach": 2, "chamber_exit": 2, "chamber_route": 1}

        for edge in chamber_edges[ch_id]:
            line_geom = _parse_geom(edge["geometry"])
            if isinstance(line_geom, LineString):
                dist = line_geom.distance(op_geom)
                if best_edge is None or dist < best_dist - 1e-3:
                    best_dist = dist
                    best_edge = edge
                    best_line_geom = line_geom
                elif abs(dist - best_dist) <= 1e-3:
                    e_type = edge["properties"].get("segment_type")
                    b_type = best_edge["properties"].get("segment_type")
                    if priority.get(e_type, 1) > priority.get(b_type, 1):
                        best_dist = dist
                        best_edge = edge
                        best_line_geom = line_geom

        if not best_edge:
            continue

        items_to_remove.add(best_edge["properties"]["id"])

        new_feats = _splice_chamber_route_for_bridge(
            best_edge, best_line_geom, op_geom, op_id
        )
        new_features.extend(new_feats)

        b_id = _find_bridge_id_from_opening(bridge_complexes, op_id)
        if b_id:
            items_to_remove.add(f"bridge_{b_id}_split")
            items_to_remove.add(f"bridge_{b_id}_merge")
            for feat in features:
                p = feat["properties"]
                if p.get("bridge_id") == b_id and p.get("segment_type") in (
                    "bridge_approach",
                    "bridge_exit",
                ):
                    items_to_remove.add(p["id"])

    filtered_features = [
        f for f in features if f["properties"].get("id") not in items_to_remove
    ]
    filtered_features.extend(new_features)
    return filtered_features


def _index_chamber_routes_and_openings(all_features, embedded_bridges):
    chamber_edges = {}
    opening_geoms = {}
    for f in all_features:
        p = f["properties"]
        if p.get("feature_type") == "fairway_segment" and p.get("segment_type") in (
            "chamber_approach",
            "chamber_route",
            "chamber_exit",
        ):
            ch_id = _extract_chamber_id(p)
            if ch_id:
                chamber_edges.setdefault(ch_id, []).append(f)
        if p.get("feature_type") == "node" and p.get("node_type") == "opening_start":
            op_id = str(p.get("opening_id"))
            if op_id in embedded_bridges:
                opening_geoms[op_id] = _parse_geom(f["geometry"])
    return chamber_edges, opening_geoms


def _find_bridge_id_from_opening(bridge_complexes, op_id):
    for b in bridge_complexes:
        for op in b.get("openings", []):
            if str(op["id"]) == op_id:
                return str(b["id"])
    return None


def _splice_chamber_route_for_bridge(
    edge_feature, line_geom: LineString, op_geom: Point, op_id: str
) -> List[Dict]:
    from pyproj import Geod

    geod = Geod(ellps="WGS84")
    from shapely.geometry import mapping

    op_start = f"opening_{op_id}_start"
    op_end = f"opening_{op_id}_end"

    proj_dist = line_geom.project(op_geom)

    parts = _cut_line_at_distance(line_geom, proj_dist)
    orig_p = edge_feature["properties"]
    new_features = []

    p1, p2 = orig_p.copy(), orig_p.copy()
    p1["id"], p2["id"] = f"{orig_p['id']}_part1", f"{orig_p['id']}_part2"
    p1["target_node"], p2["source_node"] = op_start, op_end

    if parts[0] and parts[0].length > 0:
        coords = list(parts[0].coords)
        coords[-1] = op_geom.coords[0]
        parts[0] = LineString(coords)
        p1["length_m"] = geod.geometry_length(parts[0])
        new_features.append(
            {"type": "Feature", "geometry": mapping(parts[0]), "properties": p1}
        )
    else:
        p1["length_m"] = 0.0
        new_features.append(
            {
                "type": "Feature",
                "geometry": mapping(LineString([Point(line_geom.coords[0]), op_geom])),
                "properties": p1,
            }
        )

    if parts[1] and parts[1].length > 0:
        coords = list(parts[1].coords)
        coords[0] = op_geom.coords[0]
        parts[1] = LineString(coords)
        p2["length_m"] = geod.geometry_length(parts[1])
        new_features.append(
            {"type": "Feature", "geometry": mapping(parts[1]), "properties": p2}
        )
    else:
        p2["length_m"] = 0.0
        new_features.append(
            {
                "type": "Feature",
                "geometry": mapping(LineString([op_geom, Point(line_geom.coords[-1])])),
                "properties": p2,
            }
        )

    return new_features


def _cut_line_at_distance(line: LineString, distance: float) -> List[LineString]:
    if distance <= 0.0:
        return [None, LineString(line)]
    if distance >= line.length:
        return [LineString(line), None]

    coords = list(line.coords)
    for i, p in enumerate(coords):
        projected_dist = line.project(Point(p))
        if projected_dist == distance:
            return [
                LineString(coords[: i + 1]) if i > 0 else None,
                LineString(coords[i:]) if i < len(coords) - 1 else None,
            ]
        if projected_dist > distance:
            cp = line.interpolate(distance)
            return [
                LineString(coords[:i] + [(cp.x, cp.y)]) if i > 0 else None,
                LineString([(cp.x, cp.y)] + coords[i:])
                if i < len(coords) - 1
                else None,
            ]
    return [LineString(line), None]


def _extract_chamber_id(p: Dict) -> str:
    if "chamber_id" in p:
        return str(p["chamber_id"])
    s_node = str(p.get("source_node", ""))
    if "chamber_" in s_node:
        return s_node.split("_")[1]
    return ""


def _parse_geom(geom_data):
    if isinstance(geom_data, str):
        return wkt.loads(geom_data)
    if isinstance(geom_data, dict):
        if geom_data.get("type") == "Point":
            return Point(geom_data["coordinates"])
        if geom_data.get("type") == "LineString":
            return LineString(geom_data["coordinates"])
    return None
