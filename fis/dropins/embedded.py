import logging
from typing import List, Dict, Tuple, Optional

import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, LineString, mapping

from fis import settings
from fis.utils import stringify_id

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

    # Pre-index bridge information for O(1) lookup
    bridge_info = {
        str(b.get("id")): (str(b.get("section_id", "")), str(b.get("fairway_id", "")))
        for b in bridge_complexes
    }

    # Pre-index lock complex information for O(1) lookup
    lock_lookup = {}
    for c in lock_complexes:
        l_sects = {str(s.get("id", "")) for s in c.get("sections", [])}
        l_sects.add(str(c.get("section_id", "")))
        l_fws = {str(s.get("fairway_id", "")) for s in c.get("sections", [])}
        l_fws.add(str(c.get("fairway_id", "")))

        for lk in c.get("locks", []):
            lock_lookup[str(lk.get("id"))] = (l_sects, l_fws)

    # Use spatial index for optimization
    for _, op_row in openings_rd.iterrows():
        op_id = str(op_row["id"])

        # We need the opening's parent bridge to check section/fairway IDs
        b_id = str(op_row.get("bridge_id", ""))
        bridge_section_id, bridge_fairway_id = bridge_info.get(b_id, ("", ""))

        # Buffer the opening to find nearby chambers
        buffer_geom = op_row.geometry.buffer(settings.EMBEDDED_STRUCTURE_MAX_DIST_M)
        possible_matches_index = chambers_rd.sindex.query(
            buffer_geom, predicate="intersects"
        )
        nearby_chambers = chambers_rd.iloc[possible_matches_index]

        for _, ch_row in nearby_chambers.iterrows():
            ch_id = str(ch_row["id"])
            dist = op_row.geometry.distance(ch_row.geometry)

            # Check for intersection or strict topology-aware proximity
            is_embedded = False
            if dist < 1.0:  # Basically intersecting or touching
                is_embedded = True
            elif dist < 100.0:
                # If not intersecting, they must be reasonably close AND share the same fairway or section
                ch_lock_id = str(ch_row.get("lock_id", ""))
                lock_sections, lock_fairways = lock_lookup.get(
                    ch_lock_id, (set(), set())
                )

                if (bridge_section_id and bridge_section_id in lock_sections) or (
                    bridge_fairway_id and bridge_fairway_id in lock_fairways
                ):
                    is_embedded = True

            if is_embedded:
                all_candidates.append((dist, op_id, ch_id, ch_row))

    # Sort purely by distance (closest first)
    all_candidates.sort(key=lambda x: x[0])

    matches = {}
    matched_chs = set()

    for dist, op_id, ch_id, ch_row in all_candidates:
        if op_id not in matches and ch_id not in matched_chs:
            matches[op_id] = {"ch_id": ch_id, "ch_row": ch_row}
            matched_chs.add(ch_id)

    return matches


def inject_embedded_bridges(
    features: List[Dict],
    lock_complexes: List[Dict],
    bridge_complexes: List[Dict],
    embedded_bridges: Dict[str, Dict],
) -> List[Dict]:
    """
    Injects embedded bridges directly into their governing lock chamber graphs.
    Uses UTM for consistent Cartesian splicing.
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

        op_geom_4326 = opening_geoms[op_id]

        # Determine UTM CRS based on the bridge opening
        utm_crs = gpd.GeoSeries([op_geom_4326], crs="EPSG:4326").estimate_utm_crs()
        op_geom_rd = (
            gpd.GeoSeries([op_geom_4326], crs="EPSG:4326").to_crs(utm_crs).iloc[0]
        )

        best_edge, best_line_geom_rd = _find_best_chamber_edge(
            chamber_edges[ch_id], op_geom_rd, utm_crs
        )

        if not best_edge:
            logger.warning(
                "Could not find a suitable lock chamber edge to splice embedded opening %s into Chamber %s",
                op_id,
                ch_id,
            )
            continue

        items_to_remove.add(best_edge["properties"]["id"])

        new_feats = _splice_edge_at_point(
            best_edge,
            best_line_geom_rd,
            op_geom_rd,
            utm_crs,
            node_start=f"opening_{op_id}_start",
            node_end=f"opening_{op_id}_end",
        )
        new_features.extend(new_feats)

        # Cleanup standalone bridge features
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

    return _filter_and_merge_features(features, items_to_remove, new_features)


def inject_embedded_junctions(
    features: List[Dict],
    lock_complexes: List[Dict],
    sections_gdf,
) -> List[Dict]:
    """
    Injects existing fairway junctions that fall inside a lock chamber polygon
    into the chamber route to preserve network topology.
    """
    if sections_gdf is None or sections_gdf.empty:
        return features

    junctions = _extract_all_junctions(sections_gdf)
    if not junctions:
        return features

    chamber_routes = [
        f
        for f in features
        if f["properties"].get("feature_type") == "fairway_segment"
        and f["properties"].get("segment_type") == "chamber_route"
    ]
    if not chamber_routes:
        return features

    new_features = []
    items_to_remove = set()

    # Pre-build spatial index for junctions
    j_ids = list(junctions.keys())
    j_geoms = [junctions[jid] for jid in j_ids]
    junctions_gdf = gpd.GeoDataFrame({"id": j_ids}, geometry=j_geoms, crs="EPSG:4326")

    # Pre-index chamber polygons
    chamber_polys = {}
    for lc in lock_complexes:
        for lock in lc.get("locks", []):
            for ch in lock.get("chambers", []):
                cid = stringify_id(ch["id"])
                poly_wkt = ch.get("geometry")
                if poly_wkt:
                    chamber_polys[cid] = (
                        wkt.loads(poly_wkt) if isinstance(poly_wkt, str) else poly_wkt
                    )

    for edge in chamber_routes:
        ch_id = _extract_chamber_id(edge["properties"])
        ch_poly = chamber_polys.get(ch_id)
        if not ch_poly:
            continue

        edge_geom_4326 = _parse_geom(edge["geometry"])
        if not edge_geom_4326:
            continue

        # Use UTM for this specific edge area
        utm_crs = gpd.GeoSeries([edge_geom_4326], crs="EPSG:4326").estimate_utm_crs()
        edge_geom_rd = (
            gpd.GeoSeries([edge_geom_4326], crs="EPSG:4326").to_crs(utm_crs).iloc[0]
        )

        # Query spatial index
        possible_idx = junctions_gdf.sindex.query(ch_poly, predicate="intersects")
        nearby_junctions = junctions_gdf.iloc[possible_idx]

        for _, j_row in nearby_junctions.iterrows():
            j_id = j_row["id"]
            j_pt_4326 = j_row.geometry
            j_pt_rd = (
                gpd.GeoSeries([j_pt_4326], crs="EPSG:4326").to_crs(utm_crs).iloc[0]
            )

            # Tolerance check in meters (UTM)
            if edge_geom_rd.distance(j_pt_rd) < 10.0:  # 10m tolerance for centerline
                # Gate the injection on a tight tolerance (10cm)
                distance_to_edge = edge_geom_rd.distance(j_pt_rd)
                if distance_to_edge > 0.1:
                    continue

                # Check if already an endpoint
                if (
                    j_pt_rd.distance(Point(edge_geom_rd.coords[0])) < 0.1
                    or j_pt_rd.distance(Point(edge_geom_rd.coords[-1])) < 0.1
                ):
                    continue
            else:
                continue

            logger.info("Injecting junction %s into chamber %s", j_id, ch_id)
            items_to_remove.add(edge["properties"]["id"])

            # Add junction node feature (in 4326)
            new_features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(j_pt_4326),
                    "properties": {
                        "id": j_id,
                        "feature_type": "node",
                        "node_type": "junction",
                        "node_id": j_id,
                    },
                }
            )

            # Splice edge at junction
            spliced = _splice_edge_at_point(
                edge, edge_geom_rd, j_pt_rd, utm_crs, node_start=j_id, node_end=j_id
            )
            new_features.extend(spliced)

            # Update current edge for potential multiple junctions in one chamber
            part2 = next(
                (f for f in spliced if f["properties"]["id"].endswith("_part2")),
                None,
            )
            if part2:
                edge = part2
                edge_geom_rd = (
                    gpd.GeoSeries([_parse_geom(edge["geometry"])], crs="EPSG:4326")
                    .to_crs(utm_crs)
                    .iloc[0]
                )

    return _filter_and_merge_features(features, items_to_remove, new_features)


def _find_best_chamber_edge(
    edges: List[Dict], op_geom_rd: Point, utm_crs: str
) -> Tuple[Optional[Dict], Optional[LineString]]:
    best_edge = None
    best_dist = float("inf")
    best_line_geom_rd = None
    priority = {"chamber_approach": 2, "chamber_exit": 2, "chamber_route": 1}

    for edge in edges:
        geom_4326 = _parse_geom(edge["geometry"])
        if not isinstance(geom_4326, LineString):
            continue

        line_geom_rd = (
            gpd.GeoSeries([geom_4326], crs="EPSG:4326").to_crs(utm_crs).iloc[0]
        )
        dist = line_geom_rd.distance(op_geom_rd)

        if best_edge is None or dist < best_dist - 1e-3:
            best_dist = dist
            best_edge = edge
            best_line_geom_rd = line_geom_rd
        elif abs(dist - best_dist) <= 1e-3:
            e_type = edge["properties"].get("segment_type")
            b_type = best_edge["properties"].get("segment_type")
            if priority.get(e_type, 1) > priority.get(b_type, 1):
                best_dist = dist
                best_edge = edge
                best_line_geom_rd = line_geom_rd

    return best_edge, best_line_geom_rd


def _splice_edge_at_point(
    edge_feature: Dict,
    line_geom_rd: LineString,
    point_rd: Point,
    utm_crs: str,
    node_start: str,
    node_end: str,
) -> List[Dict]:
    """Splice a projected LineString at a projected Point and return 4326 features."""
    proj_dist = line_geom_rd.project(point_rd)
    parts_rd = _cut_line_at_distance(line_geom_rd, proj_dist)

    orig_p = edge_feature["properties"]
    new_features = []

    p1, p2 = orig_p.copy(), orig_p.copy()
    p1["id"], p2["id"] = f"{orig_p['id']}_part1", f"{orig_p['id']}_part2"
    p1["target_node"], p2["source_node"] = node_start, node_end

    for i, part_rd in enumerate(parts_rd):
        props = p1 if i == 0 else p2
        if part_rd and part_rd.length > 0:
            # Force exact connection to the point
            coords = list(part_rd.coords)
            if i == 0:
                coords[-1] = (point_rd.x, point_rd.y)
            else:
                coords[0] = (point_rd.x, point_rd.y)
            part_rd = LineString(coords)

            part_4326 = (
                gpd.GeoSeries([part_rd], crs=utm_crs).to_crs("EPSG:4326").iloc[0]
            )
            props["length_m"] = part_rd.length
            new_features.append(
                {"type": "Feature", "geometry": mapping(part_4326), "properties": props}
            )
        else:
            # Fallback for zero-length or precision issues
            start_pt = Point(
                line_geom_rd.coords[0] if i == 0 else (point_rd.x, point_rd.y)
            )
            end_pt = Point(
                (point_rd.x, point_rd.y) if i == 0 else line_geom_rd.coords[-1]
            )
            fallback_line_rd = LineString([start_pt, end_pt])
            line_4326 = (
                gpd.GeoSeries([fallback_line_rd], crs=utm_crs)
                .to_crs("EPSG:4326")
                .iloc[0]
            )
            props["length_m"] = fallback_line_rd.length
            new_features.append(
                {"type": "Feature", "geometry": mapping(line_4326), "properties": props}
            )

    return new_features


def _extract_all_junctions(sections_gdf) -> Dict[str, Point]:
    junctions = {}
    for _, sec in sections_gdf.iterrows():
        geom = sec.geometry
        if not geom or geom.is_empty:
            continue
        sj = str(sec.get("StartJunctionId", sec.get("start_junction_id", "")))
        ej = str(sec.get("EndJunctionId", sec.get("end_junction_id", "")))
        if sj and sj != "nan" and sj not in junctions:
            junctions[sj] = Point(geom.coords[0])
        if ej and ej != "nan" and ej not in junctions:
            junctions[ej] = Point(geom.coords[-1])
    return junctions


def _filter_and_merge_features(features, to_remove, new_features):
    filtered = [f for f in features if f["properties"].get("id") not in to_remove]
    filtered.extend(new_features)
    return filtered


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
