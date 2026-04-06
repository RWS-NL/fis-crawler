import logging
import pandas as pd
import geopandas as gpd
import json
from shapely import wkt
from shapely.geometry import Point, mapping, LineString, shape
from pyproj import Geod
from fis.lock.utils import find_chamber_doors
from fis import utils

logger = logging.getLogger(__name__)

geod = Geod(ellps="WGS84")

CRS = "EPSG:4326"


def build_nodes_gdf(complexes) -> gpd.GeoDataFrame:
    """Return a Point GeoDataFrame of all routing nodes across all lock complexes."""
    features = build_graph_features(complexes)
    rows = [
        f["properties"] | {"geometry": _geom_from_feature(f)}
        for f in features
        if f["properties"].get("feature_type") == "node"
    ]
    if not rows:
        return gpd.GeoDataFrame(
            columns=["id", "node_type", "lock_id", "geometry"], crs=CRS
        )
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)


def build_edges_gdf(complexes) -> gpd.GeoDataFrame:
    """Return a LineString GeoDataFrame of all routing edges across all lock complexes."""
    features = build_graph_features(complexes)
    rows = [
        f["properties"] | {"geometry": _geom_from_feature(f)}
        for f in features
        if f["properties"].get("feature_type") == "fairway_segment"
    ]
    if not rows:
        return gpd.GeoDataFrame(
            columns=["id", "segment_type", "lock_id", "geometry"], crs=CRS
        )
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)


def build_berths_gdf(complexes) -> gpd.GeoDataFrame:
    """Return a Point GeoDataFrame of all berths with all scalar attributes."""
    _SKIP = {"geometry"}
    rows = []
    for c in complexes:
        for berth in c.get("berths", []):
            if not berth.get("geometry"):
                continue
            geom = wkt.loads(berth["geometry"])
            attrs = {
                k: utils.stringify_id(v) if k.endswith("_id") or k == "id" else v
                for k, v in berth.items()
                if k not in _SKIP and not isinstance(v, (list, dict))
            }
            rows.append(
                {**attrs, "lock_id": utils.stringify_id(c["id"]), "geometry": geom}
            )
    if not rows:
        return gpd.GeoDataFrame(columns=["id", "name", "lock_id", "geometry"], crs=CRS)
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)


def build_locks_gdf(complexes) -> gpd.GeoDataFrame:
    """Return a Polygon GeoDataFrame of lock complex geometries with all metadata.

    All scalar attributes from the complex dict are included automatically;
    nested collections (locks, berths, sections) and geometry WKT strings
    (geometry_before_wkt, geometry_after_wkt) are excluded.
    """
    # Keys that are nested lists or internal geometry WKTs — not useful as columns
    _SKIP = {
        "geometry",
        "locks",
        "berths",
        "sections",
        "geometry_before_wkt",
        "geometry_after_wkt",
    }

    rows = []
    for c in complexes:
        if not c.get("geometry"):
            continue
        geom = wkt.loads(c["geometry"])
        attrs = {}
        for k, v in c.items():
            if k in _SKIP:
                continue
            if isinstance(v, (list, dict)):
                attrs[k] = json.dumps(v)
            elif k.endswith("_id") or k == "id":
                attrs[k] = utils.stringify_id(v)
            else:
                attrs[k] = v
        rows.append({**attrs, "geometry": geom})
    if not rows:
        return gpd.GeoDataFrame(columns=["id", "name", "geometry"], crs=CRS)
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)


def build_chambers_gdf(complexes) -> gpd.GeoDataFrame:
    """Return a Polygon GeoDataFrame of chamber geometries with all scalar attributes."""
    _SKIP = {"geometry", "route_geometry", "subchambers"}
    rows = []
    for c in complexes:
        for l_obj in c.get("locks", []):
            for chamber in l_obj.get("chambers", []):
                geom_wkt = chamber.get("geometry")
                if not geom_wkt or not isinstance(geom_wkt, str):
                    continue
                geom = wkt.loads(geom_wkt)
                attrs = {
                    k: utils.stringify_id(v) if k.endswith("_id") or k == "id" else v
                    for k, v in chamber.items()
                    if k not in _SKIP and not isinstance(v, (list, dict))
                }
                rows.append(
                    {
                        **attrs,
                        "lock_id": utils.stringify_id(c["id"]),
                        "lock_name": c.get("name"),
                        "geometry": geom,
                    }
                )
    if not rows:
        return gpd.GeoDataFrame(
            columns=["id", "name", "lock_id", "lock_name", "geometry"], crs=CRS
        )
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)


def build_subchambers_gdf(complexes) -> gpd.GeoDataFrame:
    """Return a Polygon GeoDataFrame of subchamber geometries with all scalar attributes."""
    _SKIP = {"geometry"}
    rows = []
    for c in complexes:
        for l_obj in c.get("locks", []):
            for chamber in l_obj.get("chambers", []):
                for sc in chamber.get("subchambers", []):
                    geom_wkt = sc.get("geometry")
                    if not geom_wkt or not isinstance(geom_wkt, str):
                        continue
                    geom = wkt.loads(geom_wkt)
                    attrs = {
                        k: utils.stringify_id(v)
                        if k.endswith("_id") or k == "id"
                        else v
                        for k, v in sc.items()
                        if k not in _SKIP and not isinstance(v, (list, dict))
                    }
                    rows.append(
                        {
                            **attrs,
                            "lock_id": utils.stringify_id(c["id"]),
                            "lock_name": c.get("name"),
                            "chamber_id": utils.stringify_id(chamber["id"]),
                            "chamber_name": chamber.get("name"),
                            "geometry": geom,
                        }
                    )
    if not rows:
        return gpd.GeoDataFrame(
            columns=["id", "name", "lock_id", "chamber_id", "geometry"], crs=CRS
        )
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)


def _geom_from_feature(feature):
    """Convert a GeoJSON feature geometry dict to a Shapely geometry."""

    return shape(feature["geometry"])


def build_graph_features(complexes):
    """
    Flatten hierarchical complex objects into a list of GeoJSON features (Nodes and Edges).
    """
    features = []

    for c in complexes:
        # Lock Feature
        geom = wkt.loads(c["geometry"]) if c.get("geometry") else None
        if geom:
            # Basic properties
            props = {}
            for k, v in c.items():
                if k in [
                    "geometry",
                    "locks",
                    "berths",
                    "geometry_before_wkt",
                    "geometry_after_wkt",
                ]:
                    continue
                if isinstance(v, (list, dict)):
                    props[k] = json.dumps(v)
                else:
                    props[k] = v
            props["feature_type"] = "lock"
            features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(geom),
                    "properties": {
                        **props,
                        "id": str(props.get("id")),
                        "feature_type": "lock",
                    },
                }
            )

        # Nodes and Fairway Segments
        lock_id = utils.stringify_id(c["id"])

        features.extend(
            _process_fairway_connections(
                c,
                lock_id,
            )
        )

        # Intersecting Fairway Sections
        for section in c.get("sections", []):
            if section.get("geometry"):
                s_geom = wkt.loads(section["geometry"])
                features.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(s_geom),
                        "properties": {
                            "id": utils.stringify_id(section.get("id")),
                            "feature_type": "fairway_section",
                            "name": section.get("name"),
                            "lock_id": lock_id,
                            "section_id": utils.stringify_id(section.get("id")),
                            "fairway_id": utils.stringify_id(section.get("fairway_id")),
                            "length": section.get("length"),
                            "length_m": geod.geometry_length(s_geom),
                            "relation": section.get("relation"),
                        },
                    }
                )

        # Chambers and Chamber Routes
        features.extend(_process_chambers(c, lock_id))

        # Berths
        features.extend(_process_berths(c))

    return features


def _process_fairway_connections(
    c,
    lock_id,
):
    """
    Helper to process fairway connections (upstream/downstream) and key nodes.
    Only generates the internal split/merge nodes for each section.
    """
    features = []

    # Process split points
    split_points = c.get("split_points", {})
    split_nodes_assigned = c.get("split_nodes", {})
    for sec_id, wkt_str in split_points.items():
        if wkt_str:
            geom = wkt.loads(wkt_str)
            split_node_id = split_nodes_assigned.get(
                sec_id, f"lock_{lock_id}_{sec_id}_split"
            )
            # Only create a node feature if it's not a pre-existing junction
            # Junction nodes are created separately via _yield_junction_nodes
            if not split_node_id.isdigit() and "junction" not in split_node_id:
                features.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(geom),
                        "properties": {
                            "id": split_node_id,
                            "feature_type": "node",
                            "node_type": "lock_split",
                            "node_id": split_node_id,
                            "lock_id": c["id"],
                        },
                    }
                )

    # Process merge points
    merge_points = c.get("merge_points", {})
    merge_nodes_assigned = c.get("merge_nodes", {})
    for sec_id, wkt_str in merge_points.items():
        if wkt_str:
            geom = wkt.loads(wkt_str)
            merge_node_id = merge_nodes_assigned.get(
                sec_id, f"lock_{lock_id}_{sec_id}_merge"
            )
            if not merge_node_id.isdigit() and "junction" not in merge_node_id:
                features.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(geom),
                        "properties": {
                            "id": merge_node_id,
                            "feature_type": "node",
                            "node_type": "lock_merge",
                            "node_id": merge_node_id,
                            "lock_id": c["id"],
                        },
                    }
                )

    # Fallback to single split/merge if split_points/merge_points not populated (e.g. not via splicing)
    if not split_points and c.get("geometry_before_wkt"):
        g_before = wkt.loads(c["geometry_before_wkt"])
        split_point = Point(g_before.coords[-1])
        split_node_id = f"lock_{lock_id}_split"
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(split_point),
                "properties": {
                    "id": split_node_id,
                    "feature_type": "node",
                    "node_type": "lock_split",
                    "node_id": split_node_id,
                    "lock_id": c["id"],
                },
            }
        )

    if not merge_points and c.get("geometry_after_wkt"):
        g_after = wkt.loads(c["geometry_after_wkt"])
        merge_point = Point(g_after.coords[0])
        merge_node_id = f"lock_{lock_id}_merge"
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(merge_point),
                "properties": {
                    "id": merge_node_id,
                    "feature_type": "node",
                    "node_type": "lock_merge",
                    "node_id": merge_node_id,
                    "lock_id": c["id"],
                },
            }
        )

    return features


def _process_berths(c):
    """
    Helper to process berths and generate related graph features.
    """
    features = []
    _SKIP = {"geometry"}
    for berth in c.get("berths", []):
        if berth.get("geometry"):
            b_geom = wkt.loads(berth["geometry"])
            attrs = {
                k: v
                for k, v in berth.items()
                if k not in _SKIP and not isinstance(v, (list, dict))
            }
            features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(b_geom),
                    "properties": {
                        **attrs,
                        "id": str(berth.get("id")),
                        "feature_type": "berth",
                        "lock_id": c["id"],
                    },
                }
            )
    return features


def _process_chambers(c, lock_id):
    """
    Helper to process chambers and generate related graph features.

    Chamber "start" and "end" nodes follow the geometry direction of the fairway
    (relative to the split and merge points).
    """
    features = []
    fairway_id = utils.stringify_id(c.get("fairway_id"))

    # Determine global split and merge points for generic calculations
    global_split_point = None
    if c.get("geometry_before_wkt"):
        global_split_point = Point(wkt.loads(c["geometry_before_wkt"]).coords[-1])

    global_merge_point = None
    if c.get("geometry_after_wkt"):
        global_merge_point = Point(wkt.loads(c["geometry_after_wkt"]).coords[0])

    for l_obj in c.get("locks", []):
        for chamber in l_obj.get("chambers", []):
            chamber_id = utils.stringify_id(chamber.get("id"))
            chamber_node_start_id = f"chamber_{chamber_id}_start"
            chamber_node_end_id = f"chamber_{chamber_id}_end"

            c_geom = None
            if chamber.get("geometry") and pd.notna(chamber["geometry"]):
                c_geom = (
                    wkt.loads(chamber["geometry"])
                    if isinstance(chamber["geometry"], str)
                    else chamber["geometry"]
                )

            # Try to find doors using global points as reference vector
            door_start = None
            door_end = None
            if c_geom and global_split_point and global_merge_point:
                door_start, door_end = find_chamber_doors(
                    c_geom, global_split_point, global_merge_point
                )

            # Chamber Nodes
            if door_start and door_end:
                features.extend(
                    _build_chamber_route_features(
                        c,
                        lock_id,
                        fairway_id,
                        chamber,
                        chamber_id,
                        chamber_node_start_id,
                        chamber_node_end_id,
                        door_start,
                        door_end,
                    )
                )

            else:
                logger.warning(
                    "Could not find entry/exit doors for chamber %s (Lock %s). Falling back to centroid node.",
                    chamber_id,
                    lock_id,
                )
                # Fallback to centroid if doors not found or points missing
                chamber_node_id = f"chamber_{chamber_id}"
                centroid = c_geom.centroid if c_geom else None
                if centroid:
                    features.append(
                        {
                            "type": "Feature",
                            "geometry": mapping(centroid),
                            "properties": {
                                "id": chamber_node_id,
                                "feature_type": "node",
                                "node_type": "chamber",
                                "node_id": chamber_node_id,
                                "lock_id": c["id"],
                                "chamber_id": chamber_id,
                            },
                        }
                    )

                if chamber.get("route_geometry"):
                    # Fallback to some valid split/merge node
                    split_nodes = list(c.get("split_points", {}).keys())
                    split_node_id = (
                        f"lock_{lock_id}_{split_nodes[0]}_split"
                        if split_nodes
                        else f"lock_{lock_id}_split"
                    )
                    merge_nodes = list(c.get("merge_points", {}).keys())
                    merge_node_id = (
                        f"lock_{lock_id}_{merge_nodes[0]}_merge"
                        if merge_nodes
                        else f"lock_{lock_id}_merge"
                    )
                    features.append(
                        {
                            "type": "Feature",
                            "geometry": mapping(wkt.loads(chamber["route_geometry"])),
                            "properties": {
                                "feature_type": "fairway_segment",
                                "segment_type": "chamber_route",  # Fallback type
                                "lock_id": c["id"],
                                "chamber_id": chamber_id,
                                "fairway_id": c.get("fairway_id"),
                                "source_node": split_node_id,
                                "target_node": merge_node_id,
                                "intermediate_node": chamber_node_id,
                                "length_m": geod.geometry_length(
                                    wkt.loads(chamber["route_geometry"])
                                ),
                            },
                        }
                    )

            # Chamber Geometry Feature (Polygon)
            if c_geom:
                features.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(c_geom),
                        "properties": {
                            "feature_type": "chamber",
                            "name": chamber.get("name"),
                            "lock_id": lock_id,
                            "chamber_id": chamber_id,
                            "dim_length": chamber.get("dim_length"),
                            "dim_width": chamber.get("dim_width"),
                        },
                    }
                )

    return features


def _find_best_section_id(line, sections, context=""):
    """Find the section ID with the most overlap with the given line.
    Raises ValueError if no valid section can be associated.
    """
    if not sections:
        raise ValueError(
            f"No sections provided to find best match for geometry {context}"
        )

    # 1. Pre-parse geometries once
    parsed_sections = []
    for s in sections:
        s_geom_wkt = s.get("geometry")
        if not s_geom_wkt:
            continue
        s_geom = wkt.loads(s_geom_wkt) if isinstance(s_geom_wkt, str) else s_geom_wkt
        if s_geom:
            parsed_sections.append((utils.stringify_id(s.get("id")), s_geom))

    if not parsed_sections:
        raise ValueError(
            f"Sections provided but none had valid geometries for matching {context}"
        )

    best_sid = None
    max_overlap = -1.0

    # 2. Try intersection length (in meters)
    for sid, s_geom in parsed_sections:
        if line.intersects(s_geom):
            intersection = line.intersection(s_geom)
            if not intersection.is_empty:
                # Handle geometries that can return length (LineString/MultiLineString)
                overlap = 0.0
                if intersection.geom_type in ("LineString", "MultiLineString"):
                    overlap = geod.geometry_length(intersection)
                elif intersection.geom_type == "GeometryCollection":
                    for part in intersection.geoms:
                        if part.geom_type in ("LineString", "MultiLineString"):
                            overlap += geod.geometry_length(part)

                # Ignore point touches (overlap == 0) for the primary selection
                if overlap > 1e-3 and overlap > max_overlap:
                    max_overlap = overlap
                    best_sid = sid

    # 3. Fallback to proximity (using midpoint for robustness) if no significant overlap
    if best_sid is None:
        midpoint = line.interpolate(0.5, normalized=True)
        min_dist = float("inf")
        for sid, s_geom in parsed_sections:
            dist = midpoint.distance(s_geom)
            if dist < min_dist:
                min_dist = dist
                best_sid = sid
            elif abs(dist - min_dist) < 1e-3:
                # If distances are equal, pick the one that touches the line endpoints
                if line.intersects(s_geom):
                    best_sid = sid

    # 4. Final fallback to the first valid section
    if best_sid is None:
        best_sid = parsed_sections[0][0]

    return best_sid


def _build_chamber_route_features(
    c,
    lock_id,
    fairway_id,
    chamber,
    chamber_id,
    chamber_node_start_id,
    chamber_node_end_id,
    door_start,
    door_end,
):
    """
    Helper to extract routing lines from split nodes to doors and through chambers.
    """

    features = []

    # Start Node
    features.append(
        {
            "type": "Feature",
            "geometry": mapping(door_start),
            "properties": {
                "id": chamber_node_start_id,
                "feature_type": "node",
                "node_type": "chamber_start",
                "node_id": chamber_node_start_id,
                "lock_id": lock_id,
                "chamber_id": chamber_id,
            },
        }
    )
    # End Node
    features.append(
        {
            "type": "Feature",
            "geometry": mapping(door_end),
            "properties": {
                "id": chamber_node_end_id,
                "feature_type": "node",
                "node_type": "chamber_end",
                "node_id": chamber_node_end_id,
                "lock_id": lock_id,
                "chamber_id": chamber_id,
            },
        }
    )

    # Approach
    best_app_sec = None
    best_app_overlap = -1
    best_app_line = None
    best_split_node = None

    split_points = c.get("split_points", {})
    split_nodes = c.get("split_nodes", {})

    if not split_points and c.get("geometry_before_wkt"):
        # Fallback
        sp = Point(wkt.loads(c["geometry_before_wkt"]).coords[-1])
        split_points = {"fallback": sp.wkt}
        split_nodes = {"fallback": f"lock_{lock_id}_split"}

    for sec_id, wkt_str in split_points.items():
        sp = wkt.loads(wkt_str)
        app_line = LineString([sp, door_start]) if not sp.equals(door_start) else None

        overlap = 0
        if app_line:
            for sec in c.get("sections", []):
                if utils.stringify_id(sec.get("id")) == sec_id or sec_id == "fallback":
                    sec_geom = (
                        wkt.loads(sec["geometry"])
                        if isinstance(sec["geometry"], str)
                        else sec["geometry"]
                    )
                    if app_line.intersects(sec_geom):
                        intersection = app_line.intersection(sec_geom)
                        if intersection.geom_type in ("LineString", "MultiLineString"):
                            overlap = geod.geometry_length(intersection)
                        elif intersection.geom_type == "GeometryCollection":
                            overlap = sum(
                                geod.geometry_length(p)
                                for p in intersection.geoms
                                if p.geom_type in ("LineString", "MultiLineString")
                            )
                    break

        if not app_line:
            best_app_overlap = float("inf")
            best_app_sec = sec_id
            best_app_line = app_line
            best_split_node = split_nodes.get(sec_id, f"lock_{lock_id}_{sec_id}_split")
            break

        if overlap > best_app_overlap:
            best_app_overlap = overlap
            best_app_sec = sec_id
            best_app_line = app_line
            best_split_node = split_nodes.get(sec_id, f"lock_{lock_id}_{sec_id}_split")

    if best_app_sec == "fallback":
        best_app_sec = (
            _find_best_section_id(best_app_line, c.get("sections", []))
            if best_app_line
            else ""
        )

    # Exit
    best_ex_sec = None
    best_ex_overlap = -1
    best_ex_line = None
    best_merge_node = None

    merge_points = c.get("merge_points", {})
    merge_nodes = c.get("merge_nodes", {})

    if not merge_points and c.get("geometry_after_wkt"):
        # Fallback
        mp = Point(wkt.loads(c["geometry_after_wkt"]).coords[0])
        merge_points = {"fallback": mp.wkt}
        merge_nodes = {"fallback": f"lock_{lock_id}_merge"}

    for sec_id, wkt_str in merge_points.items():
        mp = wkt.loads(wkt_str)
        ex_line = LineString([door_end, mp]) if not door_end.equals(mp) else None

        overlap = 0
        if ex_line:
            for sec in c.get("sections", []):
                if utils.stringify_id(sec.get("id")) == sec_id or sec_id == "fallback":
                    sec_geom = (
                        wkt.loads(sec["geometry"])
                        if isinstance(sec["geometry"], str)
                        else sec["geometry"]
                    )
                    if ex_line.intersects(sec_geom):
                        intersection = ex_line.intersection(sec_geom)
                        if intersection.geom_type in ("LineString", "MultiLineString"):
                            overlap = geod.geometry_length(intersection)
                        elif intersection.geom_type == "GeometryCollection":
                            overlap = sum(
                                geod.geometry_length(p)
                                for p in intersection.geoms
                                if p.geom_type in ("LineString", "MultiLineString")
                            )
                    break

        if not ex_line:
            best_ex_overlap = float("inf")
            best_ex_sec = sec_id
            best_ex_line = ex_line
            best_merge_node = merge_nodes.get(sec_id, f"lock_{lock_id}_{sec_id}_merge")
            break

        if overlap > best_ex_overlap:
            best_ex_overlap = overlap
            best_ex_sec = sec_id
            best_ex_line = ex_line
            best_merge_node = merge_nodes.get(sec_id, f"lock_{lock_id}_{sec_id}_merge")

    if best_ex_sec == "fallback":
        best_ex_sec = (
            _find_best_section_id(best_ex_line, c.get("sections", []))
            if best_ex_line
            else ""
        )

    # Chamber (Start -> End)
    chamber_line = LineString([door_start, door_end])
    best_sec_route = _find_best_section_id(
        chamber_line,
        c.get("sections", []),
        context=f"Route for Chamber {chamber_id} (Lock {lock_id})",
    )

    # Edges
    # Approach (Split -> Start)
    if best_app_line:
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(best_app_line),
                "properties": {
                    "id": f"fairway_segment_{lock_id}_{chamber_id}_approach",
                    "feature_type": "fairway_segment",
                    "segment_type": "chamber_approach",
                    "structure_type": "lock",
                    "structure_id": lock_id,
                    "lock_id": lock_id,
                    "chamber_id": chamber_id,
                    "fairway_id": fairway_id,
                    "name": c.get("fairway_name"),
                    "section_id": best_app_sec,
                    "source_node": best_split_node,
                    "target_node": chamber_node_start_id,
                    "length_m": geod.geometry_length(best_app_line),
                },
            }
        )

    # Chamber Route
    features.append(
        {
            "type": "Feature",
            "geometry": mapping(chamber_line),
            "properties": {
                "id": f"fairway_segment_{lock_id}_{chamber_id}_route",
                "feature_type": "fairway_segment",
                "segment_type": "chamber_route",
                "structure_type": "lock",
                "structure_id": lock_id,
                "lock_id": lock_id,
                "chamber_id": chamber_id,
                "fairway_id": fairway_id,
                "name": c.get("fairway_name"),
                "section_id": best_sec_route,
                "dim_length": chamber.get("dim_length"),
                "dim_width": chamber.get("dim_width"),
                "source_node": chamber_node_start_id,
                "target_node": chamber_node_end_id,
                "length_m": geod.geometry_length(chamber_line),
            },
        }
    )

    # Exit (End -> Merge)
    if best_ex_line:
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(best_ex_line),
                "properties": {
                    "id": f"fairway_segment_{lock_id}_{chamber_id}_exit",
                    "feature_type": "fairway_segment",
                    "segment_type": "chamber_exit",
                    "structure_type": "lock",
                    "structure_id": lock_id,
                    "lock_id": lock_id,
                    "chamber_id": chamber_id,
                    "fairway_id": fairway_id,
                    "name": c.get("fairway_name"),
                    "section_id": best_ex_sec,
                    "source_node": chamber_node_end_id,
                    "target_node": best_merge_node,
                    "length_m": geod.geometry_length(best_ex_line),
                },
            }
        )

    return features
