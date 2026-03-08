import logging
import pathlib
import pickle
from typing import List, Dict, Any, Tuple

import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, mapping, shape
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

    # 1. Load and group domain objects
    lock_complexes, bridge_complexes, sections, openings = _load_and_group_dropins(
        export_dir, disk_dir, bbox
    )

    # 2. Map dropins to the internal fairway sections they intersect
    dropins_by_section = _map_dropins_to_sections(lock_complexes, bridge_complexes)

    # 3. Splice the fairways and generate routing geometries
    all_features = _splice_fairways(sections, dropins_by_section)

    # 4. Integrate the domain-specific routing features
    logger.info("Generating internal domain graph features for locks...")
    all_features.extend(lock_graph_features(lock_complexes))

    logger.info("Generating internal domain graph features for bridges...")
    # NOTE: Bridge graph generation requires us to pass the mutated bridge_complexes, which now have geometry_before_wkt and geometry_after_wkt
    all_features.extend(bridge_graph_features(bridge_complexes))

    all_features = _rewire_bridge_lock_topology(
        all_features, lock_complexes, bridge_complexes
    )

    # 5. Export
    _export_graph(all_features, lock_complexes, bridge_complexes, output_dir)

    logger.info("Done! Exported integrated dropins graph to %s", output_dir)


def _load_and_group_dropins(
    export_dir: pathlib.Path, disk_dir: pathlib.Path, bbox=None
) -> Tuple[List[Dict], List[Dict], pd.DataFrame, pd.DataFrame]:
    """Loads all parquet files and delegates to the grouped domain builders."""
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
    ) = lock_load_data(export_dir, disk_dir)

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
            logger.info(f"Filtered {name} from {len(df)} to {mask.sum()} using bbox")
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


def _map_dropins_to_sections(
    lock_complexes: List[Dict], bridge_complexes: List[Dict]
) -> Dict[Any, List[Dict]]:
    """Maps lock and bridge objects to their intersecting sections."""
    dropins_by_section = {}

    # Map locks
    for lock in lock_complexes:
        for sec in lock.get("sections", []):
            sid = sec["id"]
            dropins_by_section.setdefault(sid, []).append({"type": "lock", "obj": lock})

    # Map bridges
    for bridge in bridge_complexes:
        for sec in bridge.get("sections", []):
            sid = sec["id"]
            dropins_by_section.setdefault(sid, []).append(
                {"type": "bridge", "obj": bridge}
            )

    return dropins_by_section


def _splice_fairways(
    sections: pd.DataFrame, dropins_by_section: Dict[Any, List[Dict]]
) -> List[Dict]:
    """Uses FairwaySplicer to divide fairways around the mapped drop-in structures."""
    all_features = []

    if (
        sections is not None
        and "geometry" in sections.columns
        and sections["geometry"].dtype == "object"
    ):
        sections = sections.copy()
        sections["geometry"] = sections["geometry"].apply(
            lambda x: wkt.loads(x) if x else None
        )
    sections_gdf = gpd.GeoDataFrame(sections, geometry="geometry", crs="EPSG:4326")

    logger.info("Integrating all drop-ins into Fairways...")

    for _, sec in tqdm(
        sections_gdf.iterrows(), total=len(sections_gdf), desc="Splicing fairways"
    ):
        sid = sec["Id"]
        fairway_id = sec.get("FairwayId")
        start_junc = sec.get("StartJunctionId")
        end_junc = sec.get("EndJunctionId")
        name = sec.get("Name", sec.get("FairwayName"))

        line_geom = sec.geometry
        if not line_geom or line_geom.is_empty:
            continue

        dropins_on_sec = dropins_by_section.get(sid, [])

        def yield_junction_nodes(line, is_start, is_end):
            _yield_junction_nodes(
                all_features, line, is_start, is_end, start_junc, end_junc
            )

        if not dropins_on_sec:
            _handle_clear_section(
                all_features, sid, fairway_id, name, start_junc, end_junc, line_geom
            )
            continue

        line_rd_series = gpd.GeoSeries([line_geom], crs="EPSG:4326")
        utm_crs = line_rd_series.estimate_utm_crs()
        line_rd = line_rd_series.to_crs(utm_crs).iloc[0]

        splicer = FairwaySplicer(line_rd)

        cuts = _generate_obstacle_cuts(line_rd, dropins_on_sec, utm_crs)
        segments = splicer.splice(cuts)

        for i, segment in enumerate(segments):
            seg_4326 = (
                gpd.GeoSeries([segment.geometry], crs=utm_crs)
                .to_crs("EPSG:4326")
                .iloc[0]
            )

            source_node = str(int(start_junc)) if pd.notna(start_junc) else None
            is_start_junction = True

            if segment.source_obstacle_id:
                dtype, did = segment.source_obstacle_id.split("_")
                source_node = f"{dtype}_{did}_merge"
                _assign_geom_wkt(
                    dropins_on_sec, dtype, int(did), "geometry_after_wkt", seg_4326.wkt
                )
                is_start_junction = False

            target_node = str(int(end_junc)) if pd.notna(end_junc) else None
            is_end_junction = True

            if segment.target_obstacle_id:
                dtype, did = segment.target_obstacle_id.split("_")
                target_node = f"{dtype}_{did}_split"
                _assign_geom_wkt(
                    dropins_on_sec, dtype, int(did), "geometry_before_wkt", seg_4326.wkt
                )
                is_end_junction = False

            all_features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(seg_4326),
                    "properties": {
                        "id": f"fairway_segment_section_{sid}_{i}",
                        "feature_type": "fairway_segment",
                        "segment_type": "clear"
                        if is_start_junction and is_end_junction
                        else "approach_or_exit",
                        "section_id": sid,
                        "fairway_id": fairway_id,
                        "name": name,
                        "source_node": source_node,
                        "target_node": target_node,
                        "length_m": geod.geometry_length(seg_4326),
                    },
                }
            )

            yield_junction_nodes(seg_4326, is_start_junction, is_end_junction)

    return all_features


def _yield_junction_nodes(
    all_features, line, is_start_node, is_end_node, start_junc, end_junc
):
    """Helper to avoid repetitive Point/node generation."""
    if is_start_node and pd.notna(start_junc):
        start_p = Point(line.coords[0])
        all_features.append(
            {
                "type": "Feature",
                "geometry": mapping(start_p),
                "properties": {
                    "id": str(int(start_junc)),
                    "feature_type": "node",
                    "node_type": "junction",
                    "node_id": str(int(start_junc)),
                },
            }
        )
    if is_end_node and pd.notna(end_junc):
        end_p = Point(line.coords[-1])
        all_features.append(
            {
                "type": "Feature",
                "geometry": mapping(end_p),
                "properties": {
                    "id": str(int(end_junc)),
                    "feature_type": "node",
                    "node_type": "junction",
                    "node_id": str(int(end_junc)),
                },
            }
        )


def _handle_clear_section(
    all_features, sid, fairway_id, name, start_junc, end_junc, line_geom
):
    """Handles parsing a fairway section with no dropins on it."""
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


def _generate_obstacle_cuts(
    line_rd, dropins_on_sec: List[Dict], utm_crs
) -> List[ObstacleCut]:
    """Generates geometric cuts to provide to the Splicer."""
    cuts = []

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
            max_len = 0
            for lock_child in obj.get("locks", []):
                for ch in lock_child.get("chambers", []):
                    if ch.get("length"):
                        max_len = max(max_len, float(ch["length"]))
            buffer_dist = (max_len / 2) + 50
        else:
            buffer_dist = 10

        did = obj["id"]
        cuts.append(
            ObstacleCut(
                id=f"{dropin['type']}_{did}",
                geometry=geom_rd,
                projected_distance=dist,
                buffer_distance=buffer_dist,
            )
        )

    return cuts


def _rewire_bridge_lock_topology(
    all_features: List[Dict], lock_complexes: List[Dict], bridge_complexes: List[Dict]
) -> List[Dict]:
    """Spatially and semantically matches bridge openings to lock chambers and rewires the graph topology."""
    from fis.lock.graph import build_chambers_gdf
    from fis.bridge.graph import build_openings_gdf
    from shapely.geometry import LineString, Point

    chambers_gdf = build_chambers_gdf(lock_complexes)
    openings_gdf = build_openings_gdf(bridge_complexes)

    if chambers_gdf.empty or openings_gdf.empty:
        return all_features

    openings_rd = openings_gdf.to_crs("EPSG:28992")
    chambers_rd = chambers_gdf.to_crs("EPSG:28992")

    openings_buf = openings_rd.copy()
    openings_buf.geometry = openings_buf.geometry.buffer(500)

    possible_matches = gpd.sjoin(
        openings_buf, chambers_rd, how="inner", predicate="intersects"
    )
    matches = {}

    for op_id, group in possible_matches.groupby("id_left"):
        op_row = openings_rd[openings_rd["id"] == op_id].iloc[0]
        op_name = str(op_row.get("name", op_row.get("Name", ""))).lower()
        op_geom = op_row.geometry

        candidates = []
        for _, row in group.iterrows():
            ch_id = row["id_right"]
            if (
                hasattr(chambers_rd, "id")
                and len(chambers_rd[chambers_rd["id"] == ch_id]) > 0
            ):
                ch_row = chambers_rd[chambers_rd["id"] == ch_id].iloc[0]
            else:
                ch_row = row
            ch_name = str(ch_row.get("name", ch_row.get("Name", ""))).lower()
            dist = (
                op_geom.distance(ch_row.geometry)
                if hasattr(ch_row, "geometry")
                else row.get("dist", 0.0)
            )

            score = 0
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
                        score += 10

            if op_name and ch_name and (op_name in ch_name or ch_name in op_name):
                score += 5

            dist_score = max(0, 5 - (dist / 100))
            score += dist_score
            candidates.append((score, dist, ch_id))

        if candidates:
            candidates.sort(key=lambda x: (-x[0], x[1]))
            if candidates[0][0] > 1.0:
                matches[str(op_id)] = str(candidates[0][2])

    if not matches:
        return all_features

    logger.info(
        f"Topological Rewriting: Found {len(matches)} automatic bridge->chamber semantic overlaps."
    )

    items_to_remove = set()
    chamber_edges = {}

    for f in all_features:
        p = f["properties"]
        if p.get("feature_type") == "fairway_segment" and p.get("segment_type") in (
            "chamber_approach",
            "chamber_route",
            "chamber_exit",
        ):
            ch_id = str(p.get("chamber_id") or p.get("id").split("_")[2])
            if "chamber_id" in p:
                ch_id = str(p["chamber_id"])
            else:
                s_node = str(p.get("source_node", ""))
                t_node = str(p.get("target_node", ""))
                if "chamber_" in s_node:
                    ch_id = s_node.split("_")[1]
                elif "chamber_" in t_node:
                    ch_id = t_node.split("_")[1]

            if ch_id not in chamber_edges:
                chamber_edges[ch_id] = []
            chamber_edges[ch_id].append(f)

    bridge_openings_geoms = {}
    for f in all_features:
        p = f["properties"]
        if p.get("feature_type") == "node" and p.get("node_type") == "opening_start":
            op_id = str(p.get("opening_id"))
            if op_id in matches:
                bridge_openings_geoms[op_id] = (
                    wkt.loads(f["geometry"])
                    if isinstance(f["geometry"], str)
                    else Point(f["geometry"]["coordinates"])
                )

        if p.get("feature_type") == "fairway_segment" and p.get("segment_type") in (
            "bridge_approach",
            "bridge_exit",
        ):
            op_id = str(p.get("opening_id"))
            if op_id in matches:
                items_to_remove.add(p["id"])

    new_features = []

    def cut(line, distance):
        if distance <= 0.0:
            return [LineString(line), None]
        if distance >= line.length:
            return [None, LineString(line)]
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
                p1 = coords[:i] + [(cp.x, cp.y)]
                p2 = [(cp.x, cp.y)] + coords[i:]
                return [
                    LineString(p1) if len(p1) >= 2 else None,
                    LineString(p2) if len(p2) >= 2 else None,
                ]
        return [LineString(line), None]

    for op_id, ch_id in matches.items():
        if op_id not in bridge_openings_geoms or ch_id not in chamber_edges:
            continue

        op_geom = bridge_openings_geoms[op_id]

        best_edge = None
        min_dist = float("inf")

        for ce in chamber_edges[ch_id]:
            if not ce.get("geometry"):
                continue
            geom = (
                wkt.loads(ce["geometry"])
                if isinstance(ce["geometry"], str)
                else LineString(ce["geometry"]["coordinates"])
            )
            dist = geom.distance(op_geom)
            if dist < min_dist:
                min_dist = dist
                best_edge = ce

        if best_edge:
            orig_p = best_edge["properties"]
            items_to_remove.add(orig_p["id"])

            geom = (
                wkt.loads(best_edge["geometry"])
                if isinstance(best_edge["geometry"], str)
                else LineString(best_edge["geometry"]["coordinates"])
            )
            proj_dist = geom.project(op_geom)
            split_point = geom.interpolate(proj_dist)

            parts = cut(geom, proj_dist)

            op_start_node = f"opening_{op_id}_start"
            op_end_node = f"opening_{op_id}_end"

            if parts[0] and parts[0].length > 0:
                p1 = orig_p.copy()
                p1["id"] = f"{orig_p['id']}_part1"
                p1["target_node"] = op_start_node
                p1["length_m"] = geod.geometry_length(parts[0])
                new_features.append(
                    {"type": "Feature", "geometry": mapping(parts[0]), "properties": p1}
                )
            else:
                p1 = orig_p.copy()
                p1["id"] = f"{orig_p['id']}_part1"
                p1["target_node"] = op_start_node
                p1["length_m"] = 0.0
                new_features.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(
                            LineString([Point(geom.coords[0]), split_point])
                        ),
                        "properties": p1,
                    }
                )

            if parts[1] and parts[1].length > 0:
                p2 = orig_p.copy()
                p2["id"] = f"{orig_p['id']}_part2"
                p2["source_node"] = op_end_node
                p2["length_m"] = geod.geometry_length(parts[1])
                new_features.append(
                    {"type": "Feature", "geometry": mapping(parts[1]), "properties": p2}
                )
            else:
                p2 = orig_p.copy()
                p2["id"] = f"{orig_p['id']}_part2"
                p2["source_node"] = op_end_node
                p2["length_m"] = 0.0
                new_features.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(
                            LineString([split_point, Point(geom.coords[-1])])
                        ),
                        "properties": p2,
                    }
                )

    final_features = [
        f for f in all_features if f["properties"].get("id") not in items_to_remove
    ]
    final_features.extend(new_features)

    connected_nodes = set()
    for f in final_features:
        if f["properties"].get("feature_type") == "fairway_segment":
            if pd.notna(f["properties"].get("source_node")):
                connected_nodes.add(f["properties"]["source_node"])
            if pd.notna(f["properties"].get("target_node")):
                connected_nodes.add(f["properties"]["target_node"])

    cleaned_features = []
    for f in final_features:
        if f["properties"].get("feature_type") == "node" and f["properties"].get(
            "node_type"
        ) in ("bridge_split", "bridge_merge"):
            if f["properties"]["id"] not in connected_nodes:
                continue
        cleaned_features.append(f)

    return cleaned_features


def _export_graph(
    all_features: List[Dict],
    lock_complexes: List[Dict],
    bridge_complexes: List[Dict],
    output_dir: pathlib.Path,
):
    """Exports the completed unified dropins graph and component features."""
    logger.info("Exporting drop-ins network graph and components...")

    nodes_rows = []
    edges_rows = []
    seen_nodes = set()

    for f in all_features:
        props = f["properties"]
        geom = shape(f["geometry"]) if f["geometry"] else None
        if not geom:
            continue

        # Only output unique nodes by ID
        if props.get("feature_type") == "node":
            node_id = props["id"]
            if node_id in seen_nodes:
                continue
            seen_nodes.add(node_id)
            nodes_rows.append(props | {"geometry": geom})

        elif props.get("feature_type") == "fairway_segment":
            edges_rows.append(props | {"geometry": geom})

    nodes_gdf = gpd.GeoDataFrame(nodes_rows, geometry="geometry", crs="EPSG:4326")
    edges_gdf = gpd.GeoDataFrame(edges_rows, geometry="geometry", crs="EPSG:4326")

    output_dir.mkdir(parents=True, exist_ok=True)

    G = nx.MultiDiGraph()
    for _, row in nodes_gdf.iterrows():
        node_attr = {k: v for k, v in row.items() if k != "geometry"}
        node_attr["geometry_wkt"] = row.geometry.wkt
        G.add_node(row["id"], **node_attr)

    for _, row in edges_gdf.iterrows():
        if pd.notna(row.get("source_node")) and pd.notna(row.get("target_node")):
            edge_attr = {
                k: v
                for k, v in row.items()
                if k not in ["source_node", "target_node", "geometry"]
            }
            edge_attr["geometry_wkt"] = row.geometry.wkt
            G.add_edge(row["source_node"], row["target_node"], **edge_attr)

    with open(output_dir / "graph.pickle", "wb") as f:
        pickle.dump(G, f)

    logger.info(
        "Generated graph with %d nodes and %d edges",
        G.number_of_nodes(),
        G.number_of_edges(),
    )

    from fis.lock.graph import (
        build_locks_gdf,
        build_chambers_gdf,
        build_subchambers_gdf,
        build_berths_gdf,
    )
    from fis.bridge.graph import build_bridges_gdf, build_openings_gdf

    gdfs_to_export = {
        "nodes": nodes_gdf,
        "edges": edges_gdf,
        "locks": build_locks_gdf(lock_complexes),
        "chambers": build_chambers_gdf(lock_complexes),
        "subchambers": build_subchambers_gdf(lock_complexes),
        "berths": build_berths_gdf(lock_complexes),
        "bridges": build_bridges_gdf(bridge_complexes),
        "openings": build_openings_gdf(bridge_complexes),
    }

    for name, gdf in gdfs_to_export.items():
        if gdf.empty:
            continue
        try:
            gdf.to_parquet(output_dir / f"{name}.geoparquet")
            gdf.to_file(output_dir / f"{name}.geojson", driver="GeoJSON")
            logger.info("Exported %s with %d rows", name, len(gdf))
        except Exception as e:
            logger.warning("Failed to export %s: %s", name, e)


def _assign_geom_wkt(dropins_list, dtype, did, key, wkt_str):
    """Update the target drop-in's boundary geometry properties."""
    for dropin in dropins_list:
        obj_id = dropin["obj"]["id"]
        if dropin["type"] == dtype and int(obj_id) == did:
            dropin["obj"][key] = wkt_str
            break
