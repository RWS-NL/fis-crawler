import logging
import pathlib
import pickle
from typing import List, Dict, Any, Tuple, Set, Optional

import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, mapping, shape, LineString
from tqdm import tqdm
from pyproj import Geod
import networkx as nx

from fis.lock.core import load_data as lock_load_data, group_complexes as group_locks
from fis.bridge.core import group_bridge_complexes as group_bridges
from fis.splicer import FairwaySplicer, ObstacleCut

from fis.lock.graph import build_graph_features as lock_graph_features
from fis.bridge.graph import build_graph_features as bridge_graph_features

logger = logging.getLogger(__name__)
geod = Geod(ellps="WGS84")


def build_integrated_dropins_graph(
    export_dir: pathlib.Path,
    disk_dir: pathlib.Path,
    output_dir: pathlib.Path,
    bbox=None,
):
    """Main orchestrator to build the completely integrated Drop-ins graph."""
    lock_complexes, bridge_complexes, sections, openings = _load_and_group_dropins(
        export_dir, disk_dir, bbox
    )

    embedded_bridges = _identify_embedded_structures(lock_complexes, bridge_complexes)
    dropins_by_section = _map_dropins_to_sections(lock_complexes, bridge_complexes)
    all_features = _splice_fairways(sections, dropins_by_section, embedded_bridges)

    logger.info("Generating internal domain graph features for locks...")
    all_features.extend(lock_graph_features(lock_complexes))

    logger.info("Generating internal domain graph features for bridges...")
    all_features.extend(bridge_graph_features(bridge_complexes))

    all_features = _inject_embedded_bridges(
        all_features, lock_complexes, bridge_complexes, embedded_bridges
    )

    _export_graph(all_features, lock_complexes, bridge_complexes, output_dir)
    logger.info("Done! Exported integrated dropins graph to %s", output_dir)


def _load_and_group_dropins(
    export_dir: pathlib.Path, disk_dir: pathlib.Path, bbox=None
) -> Tuple[List[Dict], List[Dict], pd.DataFrame, pd.DataFrame]:
    """Loads all parquet files and delegates to the grouped domain builders."""
    res = lock_load_data(export_dir, disk_dir)
    (
        locks,
        chambers,
        subchambers,
        isrs,
        fairways,
        berths,
        sections,
        disk_locks,
        disk_bridges,
        operatingtimes,
        bridges,
        openings,
    ) = res

    if bbox:
        import shapely.geometry

        bbox_poly = shapely.geometry.box(*bbox)

        def filter_df(df, name):
            if df is None or df.empty or "geometry" not in df.columns:
                return df
            geoms = df["geometry"].apply(
                lambda x: wkt.loads(x)
                if isinstance(x, str) and x
                else (x if not isinstance(x, str) else None)
            )
            mask = gpd.GeoSeries(geoms, crs="EPSG:4326").intersects(bbox_poly)
            return df[mask].copy()

        locks = filter_df(locks, "locks")
        bridges = filter_df(bridges, "bridges")
        sections = filter_df(sections, "sections")

    logger.info("Grouping Locks...")
    lock_complexes = group_locks(
        locks,
        chambers,
        subchambers,
        isrs,
        None,
        fairways,
        berths,
        sections,
        None,
        disk_locks,
        disk_bridges,
        operatingtimes,
        bridges,
        openings,
    )

    logger.info("Grouping Bridges...")
    bridge_complexes = group_bridges(
        bridges, openings, sections, disk_bridges, operatingtimes
    )

    return lock_complexes, bridge_complexes, sections, openings


def _identify_embedded_structures(
    lock_complexes: List[Dict], bridge_complexes: List[Dict]
) -> Dict[str, Dict]:
    """
    Identifies bridges that are functionally embedded within a lock complex.

    A bridge is considered embedded if it is located inside or immediately
    adjacent to a lock chamber, such that it interrupts the chamber's local
    topology rather than the main fairway.

    Args:
        lock_complexes: A list of dicts representing grouped lock complexes.
        bridge_complexes: A list of dicts representing grouped bridge complexes.

    Returns:
        A dictionary mapping the opening ID (str) to a dict containing the matching
        chamber's ID `ch_id` and the chamber's DataFrame row `ch_row`.
    """
    from fis.lock.graph import build_chambers_gdf
    from fis.bridge.graph import build_openings_gdf

    chambers_gdf = build_chambers_gdf(lock_complexes)
    openings_gdf = build_openings_gdf(bridge_complexes)

    if chambers_gdf.empty or openings_gdf.empty:
        return {}

    openings_rd = openings_gdf.to_crs("EPSG:28992")
    chambers_rd = chambers_gdf.to_crs("EPSG:28992")
    matches = {}

    for _, op_row in openings_rd.iterrows():
        op_id = str(op_row["id"])
        op_name = str(op_row.get("name", op_row.get("Name", ""))).lower()
        candidates = []
        for _, ch_row in chambers_rd.iterrows():
            ch_id = str(ch_row["id"])
            ch_name = str(ch_row.get("name", ch_row.get("Name", ""))).lower()
            dist = op_row.geometry.distance(ch_row.geometry)
            if dist > 500:
                continue
            score = _calculate_semantic_spatial_score(op_name, ch_name, dist)
            candidates.append((score, dist, ch_id, ch_row))

        if candidates:
            candidates.sort(key=lambda x: (-x[0], x[1]))
            best_score, best_dist, best_ch_id, best_ch_row = candidates[0]
            if best_score > 1.0:
                matches[op_id] = {"ch_id": best_ch_id, "ch_row": best_ch_row}

    return matches


def _calculate_semantic_spatial_score(op_name: str, ch_name: str, dist: float) -> float:
    """
    Computes a heuristic score combining string name matching and spatial proximity.

    Higher scores strongly indicate two features are part of the same complex
    (e.g., 'Krammersluis oost' bridge mapping to 'Krammersluizer oost' chamber).

    Args:
        op_name: The name of the bridge opening.
        ch_name: The name of the lock chamber.
        dist: Spatial distance between them in meters.

    Returns:
        float: Computed matching confidence score. Score > 1.0 generally implies a match.
    """
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


def _map_dropins_to_sections(
    lock_complexes: List[Dict], bridge_complexes: List[Dict]
) -> Dict[Any, List[Dict]]:
    """
    Creates a reverse mapping of fairway section ID to all drop-ins (locks/bridges)
    that are spatially associated with that section.

    Args:
        lock_complexes: Grouped lock complexes.
        bridge_complexes: Grouped bridge complexes.

    Returns:
        Dict mapping section ID to a list of dicts `{"type": str, "obj": Dict}` representing
        the drop-ins on that section.
    """
    dropins_by_section = {}
    for lock in lock_complexes:
        for sec in lock.get("sections", []):
            dropins_by_section.setdefault(sec["id"], []).append(
                {"type": "lock", "obj": lock}
            )
    for bridge in bridge_complexes:
        for sec in bridge.get("sections", []):
            dropins_by_section.setdefault(sec["id"], []).append(
                {"type": "bridge", "obj": bridge}
            )
    return dropins_by_section


def _splice_fairways(
    sections: pd.DataFrame,
    dropins_by_section: Dict[Any, List[Dict]],
    embedded_bridges: Dict[str, Dict],
) -> List[Dict]:
    """
    Iterates over all fairway sections and splices them into sub-segments
    based on the obstacles (drop-ins) that lie upon them.

    Embedded bridges are explicitly omitted from slicing the main fairway,
    since they only interrupt internal lock chamber routes.

    Args:
        sections: DataFrame of fairway sections.
        dropins_by_section: Precomputed mapping of section IDs to drop-in obstacles.
        embedded_bridges: Mapping of bridge opening IDs that are embedded in locks.

    Returns:
        A list of generated GeoJSON-style feature dicts representing the spliced segments.
    """
    all_features = []
    sections_gdf = _prepare_sections_gdf(sections)
    embedded_ids = {str(k) for k in embedded_bridges.keys()}

    for _, sec in tqdm(
        sections_gdf.iterrows(),
        total=len(sections_gdf),
        desc="Splicing fairways",
        mininterval=2.0,
    ):
        line_geom = sec.geometry
        if not line_geom or line_geom.is_empty:
            continue

        sid = sec["Id"]
        dropins_on_sec = dropins_by_section.get(sid, [])
        visible_dropins = [
            d for d in dropins_on_sec if not _is_embedded(d, embedded_ids)
        ]

        if not visible_dropins:
            _handle_clear_section(all_features, sec)
            continue

        _slice_section_with_dropins(all_features, sec, visible_dropins, dropins_on_sec)
    return all_features


def _is_embedded(dropin: Dict, embedded_ids: Set[str]) -> bool:
    """
    Checks if a given dropin obstacle matches any of the known embedded bridge IDs.

    Args:
        dropin: The obstacle dict `{"type": ..., "obj": ...}`.
        embedded_ids: A set of opening IDs considered embedded.

    Returns:
        True if the obstacle is an embedded bridge, False otherwise.
    """
    if dropin["type"] != "bridge":
        return False
    for op in dropin["obj"].get("openings", []):
        if str(op["id"]) in embedded_ids:
            return True
    return False


def _prepare_sections_gdf(sections: pd.DataFrame) -> gpd.GeoDataFrame:
    if sections is not None and "geometry" in sections.columns:
        sections = sections.copy()
        sections["geometry"] = sections["geometry"].apply(
            lambda x: wkt.loads(x) if isinstance(x, str) else x
        )
    return gpd.GeoDataFrame(sections, geometry="geometry", crs="EPSG:4326")


def _handle_clear_section(all_features, sec):
    sid = sec["Id"]
    fairway_id = sec.get("FairwayId")
    name = sec.get("Name", sec.get("FairwayName"))
    start_junc = sec.get("StartJunctionId")
    end_junc = sec.get("EndJunctionId")
    line_geom = sec.geometry

    source_id = str(int(start_junc)) if pd.notna(start_junc) else None
    target_id = str(int(end_junc)) if pd.notna(end_junc) else None

    all_features.append(
        {
            "type": "Feature",
            "geometry": mapping(line_geom),
            "properties": {
                "id": f"fairway_segment_section_{sid}",
                "feature_type": "fairway_segment",
                "segment_type": "clear",
                "section_id": sid,
                "fairway_id": fairway_id,
                "name": name,
                "source_node": source_id,
                "target_node": target_id,
                "length_m": geod.geometry_length(line_geom),
            },
        }
    )
    _yield_junction_nodes(all_features, line_geom, True, True, start_junc, end_junc)


def _slice_section_with_dropins(
    all_features, sec, visible_dropins, original_dropins_on_sec
):
    line_geom = sec.geometry
    line_rd_series = gpd.GeoSeries([line_geom], crs="EPSG:4326")
    utm_crs = line_rd_series.estimate_utm_crs()
    line_rd = line_rd_series.to_crs(utm_crs).iloc[0]

    splicer = FairwaySplicer(line_rd)
    cuts = _generate_obstacle_cuts(line_rd, visible_dropins, utm_crs)
    segments = splicer.splice(cuts)

    for i, segment in enumerate(segments):
        seg_4326 = (
            gpd.GeoSeries([segment.geometry], crs=utm_crs).to_crs("EPSG:4326").iloc[0]
        )
        source_node, is_start_junc = _determine_source_node(
            segment, sec.get("StartJunctionId"), original_dropins_on_sec, seg_4326
        )
        target_node, is_end_junc = _determine_target_node(
            segment, sec.get("EndJunctionId"), original_dropins_on_sec, seg_4326
        )

        all_features.append(
            {
                "type": "Feature",
                "geometry": mapping(seg_4326),
                "properties": {
                    "id": f"fairway_segment_section_{sec['Id']}_{i}",
                    "feature_type": "fairway_segment",
                    "segment_type": "clear"
                    if is_start_junc and is_end_junc
                    else "approach_or_exit",
                    "section_id": sec["Id"],
                    "fairway_id": sec.get("FairwayId"),
                    "name": sec.get("Name", sec.get("FairwayName")),
                    "source_node": source_node,
                    "target_node": target_node,
                    "length_m": geod.geometry_length(seg_4326),
                },
            }
        )
        _yield_junction_nodes(
            all_features,
            seg_4326,
            is_start_junc,
            is_end_junc,
            sec.get("StartJunctionId"),
            sec.get("EndJunctionId"),
        )


def _determine_source_node(
    segment: Any, start_junc: Any, dropins: List[Dict], seg_4326: Any
) -> Tuple[Optional[str], bool]:
    """
    Determines the correct source node ID for a spliced segment.
    If the segment starts from a drop-in obstacle, evaluates the obstacle type
    and uses the appropriate `_split` or `_merge` node suffix.

    Args:
        segment: The generated SplicedSegment object.
        start_junc: The default StartJunctionId from the section.
        dropins: The list of drop-in obstacles present on the section.
        seg_4326: The current segment's geometry in 4326 projection.

    Returns:
        A tuple of (node_id or None, is_start_of_fairway Boolean).
    """
    is_start = True
    node = str(int(start_junc)) if pd.notna(start_junc) else None
    if segment.source_obstacle_id:
        dtype, did = segment.source_obstacle_id.split("_")
        node = f"{dtype}_{did}_merge"
        _assign_geom_wkt(dropins, dtype, int(did), "geometry_after_wkt", seg_4326.wkt)
        is_start = False
    return node, is_start


def _determine_target_node(
    segment: Any, end_junc: Any, dropins: List[Dict], seg_4326: Any
) -> Tuple[Optional[str], bool]:
    """
    Determines the correct target node ID for a spliced segment.
    If the segment ends at a drop-in obstacle, evaluates the obstacle type
    and uses the appropriate `_split` or `_merge` node suffix.

    Args:
        segment: The generated SplicedSegment object.
        end_junc: The default EndJunctionId from the section.
        dropins: The list of drop-in obstacles present on the section.
        seg_4326: The current segment's geometry in 4326 projection.

    Returns:
        A tuple of (node_id or None, is_end_of_fairway Boolean).
    """
    is_end = True
    node = str(int(end_junc)) if pd.notna(end_junc) else None
    if segment.target_obstacle_id:
        dtype, did = segment.target_obstacle_id.split("_")
        node = f"{dtype}_{did}_split"
        _assign_geom_wkt(dropins, dtype, int(did), "geometry_before_wkt", seg_4326.wkt)
        is_end = False
    return node, is_end


def _generate_obstacle_cuts(
    line_rd: Any, dropins_on_sec: List[Dict], utm_crs: str
) -> List[ObstacleCut]:
    """
    Generates ObstacleCuts for each drop-in obstacle along a fairway section.

    Calculates the 1D projection distance of each obstacle's centroid onto the
    underlying line geometry. Differentiates buffers depending on drop-in type
    (e.g., 50m fallback for locks, 10m for bridges).

    Args:
        line_rd: Geopandas Series geometry of the fairway section projected in UTM.
        dropins_on_sec: The drop-in attributes mapping to this section.
        utm_crs: String identifier of the metric CRS being used for distances.

    Returns:
        A list of ObstacleCut geometric objects.
    """
    cuts = []
    LOCK_BUFFER_BASE = 50.0
    BRIDGE_BUFFER = 10.0

    for dropin in dropins_on_sec:
        obj = dropin["obj"]
        geom_wkt = obj.get("geometry")
        if not geom_wkt:
            continue

        geom = wkt.loads(geom_wkt)
        geom_rd = gpd.GeoSeries([geom], crs="EPSG:4326").to_crs(utm_crs).iloc[0]
        if geom_rd.geom_type != "Point":
            geom_rd = geom_rd.centroid

        dist = line_rd.project(geom_rd)
        if dropin["type"] == "lock":
            max_len = 0.0
            for child in obj.get("locks", []):
                for ch in child.get("chambers", []):
                    if ch.get("length"):
                        max_len = max(max_len, float(ch["length"]))
            buffer_dist = (max_len / 2.0) + LOCK_BUFFER_BASE
        else:
            buffer_dist = BRIDGE_BUFFER

        cuts.append(
            ObstacleCut(
                id=f"{dropin['type']}_{obj['id']}",
                geometry=geom_rd,
                projected_distance=dist,
                buffer_distance=buffer_dist,
            )
        )
    return cuts


def _yield_junction_nodes(all_features, line, is_start, is_end, start_junc, end_junc):
    if is_start and pd.notna(start_junc):
        all_features.append(
            {
                "type": "Feature",
                "geometry": mapping(Point(line.coords[0])),
                "properties": {
                    "id": str(int(start_junc)),
                    "feature_type": "node",
                    "node_type": "junction",
                    "node_id": str(int(start_junc)),
                },
            }
        )
    if is_end and pd.notna(end_junc):
        all_features.append(
            {
                "type": "Feature",
                "geometry": mapping(Point(line.coords[-1])),
                "properties": {
                    "id": str(int(end_junc)),
                    "feature_type": "node",
                    "node_type": "junction",
                    "node_id": str(int(end_junc)),
                },
            }
        )


def _assign_geom_wkt(dropins_list, dtype, did, key, wkt_str):
    for dropin in dropins_list:
        if dropin["type"] == dtype and int(dropin["obj"]["id"]) == did:
            dropin["obj"][key] = wkt_str
            break


def _inject_embedded_bridges(
    features: List[Dict],
    lock_complexes: List[Dict],
    bridge_complexes: List[Dict],
    embedded_bridges: Dict[str, Dict],
) -> List[Dict]:
    """
    Injects embedded bridges directly into their governing lock chamber graphs.

    An embedded bridge interrupts the lock chamber route rather than the main fairway.
    This function discovers the specific chamber route segments traversing the chamber
    and splices them with the bridge node, dynamically generating bridge passage segments.

    Args:
        features: The accumulating drop-in feature collection.
        lock_complexes: The grouped lock complexes.
        bridge_complexes: The grouped bridge complexes.
        embedded_bridges: The mapping of embedded opening ID to its chamber details.

    Returns:
        The updated feature collection with integrated embedded bridges.
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
                    # On tie, prioritize approach/exit over the internal chamber route
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
            # Remove any bridge approaches generated by bridge_graph_features
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
        pd = line.project(Point(p))
        if pd == distance:
            return [
                LineString(coords[: i + 1]) if i > 0 else None,
                LineString(coords[i:]) if i < len(coords) - 1 else None,
            ]
        if pd > distance:
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


def _export_graph(
    all_features: List[Dict],
    lock_complexes: List[Dict],
    bridge_complexes: List[Dict],
    output_dir: pathlib.Path,
):
    logger.info("Exporting drop-ins network graph and components...")
    nodes_rows, edges_rows = _separate_features(all_features)

    if not nodes_rows or not edges_rows:
        raise ValueError("Cannot export graph: Nodes or Edges list is empty.")

    nodes_gdf = gpd.GeoDataFrame(nodes_rows, geometry="geometry", crs="EPSG:4326")
    edges_gdf = gpd.GeoDataFrame(edges_rows, geometry="geometry", crs="EPSG:4326")

    output_dir.mkdir(parents=True, exist_ok=True)

    G = nx.MultiDiGraph()
    _populate_graph(G, nodes_gdf, edges_gdf)

    with open(output_dir / "graph.pickle", "wb") as f:
        pickle.dump(G, f)

    logger.info(
        "Generated graph with %d nodes and %d edges",
        G.number_of_nodes(),
        G.number_of_edges(),
    )
    _export_dataframes(
        lock_complexes, bridge_complexes, nodes_gdf, edges_gdf, output_dir
    )


def _separate_features(all_features: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    nodes_rows, edges_rows = [], []
    seen_nodes = set()
    for f in all_features:
        props = f["properties"]
        geom = shape(f["geometry"]) if f["geometry"] else None
        if not geom:
            continue
        ftype = props.get("feature_type")
        if ftype == "node":
            if props["id"] not in seen_nodes:
                seen_nodes.add(props["id"])
                nodes_rows.append(props | {"geometry": geom})
        elif ftype == "fairway_segment":
            edges_rows.append(props | {"geometry": geom})
    return nodes_rows, edges_rows


def _populate_graph(
    G: nx.MultiDiGraph, nodes_gdf: gpd.GeoDataFrame, edges_gdf: gpd.GeoDataFrame
):
    for _, row in nodes_gdf.iterrows():
        node_attr = {k: v for k, v in row.items() if k != "geometry"}
        node_attr["geometry_wkt"] = row.geometry.wkt
        G.add_node(row["id"], **node_attr)
    for _, row in edges_gdf.iterrows():
        if pd.isna(row.get("source_node")) or pd.isna(row.get("target_node")):
            logger.debug(
                f"Skipping edge {row.get('id')} due to missing node assignment."
            )
            continue
        edge_attr = {
            k: v
            for k, v in row.items()
            if k not in ["source_node", "target_node", "geometry"]
        }
        edge_attr["geometry_wkt"] = row.geometry.wkt
        G.add_edge(row["source_node"], row["target_node"], **edge_attr)


def _export_dataframes(
    lock_complexes, bridge_complexes, nodes_gdf, edges_gdf, output_dir
):
    from fis.lock.graph import (
        build_locks_gdf,
        build_chambers_gdf,
        build_subchambers_gdf,
        build_berths_gdf,
    )
    from fis.bridge.graph import build_bridges_gdf, build_openings_gdf

    gdfs = {
        "nodes": nodes_gdf,
        "edges": edges_gdf,
        "locks": build_locks_gdf(lock_complexes),
        "chambers": build_chambers_gdf(lock_complexes),
        "subchambers": build_subchambers_gdf(lock_complexes),
        "berths": build_berths_gdf(lock_complexes),
        "bridges": build_bridges_gdf(bridge_complexes),
        "openings": build_openings_gdf(bridge_complexes),
    }

    for name, gdf in gdfs.items():
        if not gdf.empty:
            gdf.to_parquet(output_dir / f"{name}.geoparquet")
            gdf.to_file(output_dir / f"{name}.geojson", driver="GeoJSON")
            logger.info("Exported %s with %d rows", name, len(gdf))
