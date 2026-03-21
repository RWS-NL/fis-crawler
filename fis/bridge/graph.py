import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, LineString, mapping
from pyproj import Geod
import json
import pandas as pd

geod = Geod(ellps="WGS84")
CRS = "EPSG:4326"


def build_nodes_gdf(complexes) -> gpd.GeoDataFrame:
    features = build_graph_features(complexes)
    rows = [
        f["properties"] | {"geometry": wkt.loads(f["geometry"])}
        if isinstance(f["geometry"], str)
        else f["properties"] | {"geometry": Point(f["geometry"]["coordinates"])}
        for f in features
        if f["properties"].get("feature_type") == "node"
    ]
    if not rows:
        return gpd.GeoDataFrame(
            columns=["id", "node_type", "bridge_id", "geometry"], crs=CRS
        )
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)


def build_edges_gdf(complexes) -> gpd.GeoDataFrame:
    features = build_graph_features(complexes)
    rows = [
        f["properties"] | {"geometry": wkt.loads(f["geometry"])}
        if isinstance(f["geometry"], str)
        else f["properties"] | {"geometry": LineString(f["geometry"]["coordinates"])}
        for f in features
        if f["properties"].get("feature_type") == "fairway_segment"
    ]
    if not rows:
        return gpd.GeoDataFrame(
            columns=["id", "segment_type", "bridge_id", "geometry"], crs=CRS
        )
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)


def build_bridges_gdf(complexes) -> gpd.GeoDataFrame:
    rows = []
    for c in complexes:
        if not c.get("geometry"):
            continue
        geom = wkt.loads(c["geometry"])

        attrs = {
            k: v
            for k, v in c.items()
            if k
            not in [
                "geometry",
                "openings",
                "sections",
                "geometry_before_wkt",
                "geometry_after_wkt",
            ]
            and not isinstance(v, (list, dict))
        }
        rows.append({**attrs, "geometry": geom})

    if not rows:
        return gpd.GeoDataFrame(columns=["id", "name", "geometry"], crs=CRS)
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)


def build_openings_gdf(complexes) -> gpd.GeoDataFrame:
    rows = []
    for c in complexes:
        if not c.get("geometry"):
            continue
        wkt.loads(c["geometry"])

        openings = c.get("openings", [])
        if not openings:
            openings = [
                {
                    "id": -int(c["id"]),
                    "width": None,
                    "height": None,
                    "geometry": c.get("geometry"),
                }
            ]

        for op in openings:
            assert "geometry" in op and op["geometry"], (
                f"Bridge opening {op['id']} is missing a geometry."
            )
            op_geom = wkt.loads(op["geometry"])
            if not isinstance(op_geom, Point):
                op_geom = op_geom.centroid
            attrs = {
                k: v
                for k, v in op.items()
                if k not in ["geometry"] and not isinstance(v, (list, dict))
            }
            rows.append({**attrs, "bridge_id": c["id"], "geometry": op_geom})

    if not rows:
        return gpd.GeoDataFrame(columns=["id", "bridge_id", "geometry"], crs=CRS)
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)


def build_graph_features(complexes):
    """
    Flatten hierarchical bridge complexes into a list of GeoJSON features (Nodes and Edges).
    """
    features = []

    for c in complexes:
        bridge_id = c["id"]

        split_node_id = f"bridge_{bridge_id}_split"
        merge_node_id = f"bridge_{bridge_id}_merge"

        split_point = None
        if c.get("geometry_before_wkt"):
            g_before = wkt.loads(c["geometry_before_wkt"])
            split_point = Point(g_before.coords[-1])
            features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(split_point),
                    "properties": {
                        "id": split_node_id,
                        "feature_type": "node",
                        "node_type": "bridge_split",
                        "node_id": split_node_id,
                        "bridge_id": bridge_id,
                    },
                }
            )

        merge_point = None
        if c.get("geometry_after_wkt"):
            g_after = wkt.loads(c["geometry_after_wkt"])
            merge_point = Point(g_after.coords[0])
            features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(merge_point),
                    "properties": {
                        "id": merge_node_id,
                        "feature_type": "node",
                        "node_type": "bridge_merge",
                        "node_id": merge_node_id,
                        "bridge_id": bridge_id,
                    },
                }
            )

        if split_point and merge_point and not split_point.equals(merge_point):
            LineString([split_point, merge_point])

        openings = c.get("openings", [])
        if not openings:
            openings = [
                {
                    "id": -int(bridge_id),
                    "width": None,
                    "height": None,
                    "geometry": c.get("geometry"),
                }
            ]

        for opening in openings:
            op_id = opening["id"]

            assert "geometry" in opening and opening["geometry"], (
                f"Bridge passage opening {op_id} is missing a geometry definition."
            )
            op_geom_raw = wkt.loads(opening["geometry"])
            if not isinstance(op_geom_raw, Point):
                op_geom_raw = op_geom_raw.centroid
            assert isinstance(op_geom_raw, Point), (
                f"Bridge passage opening {op_id} could not be resolved to a Point."
            )

            op_geom = op_geom_raw

            op_start_node = f"opening_{op_id}_start"
            op_end_node = f"opening_{op_id}_end"

            sections = c.get("sections", [])
            best_sec_id = None
            if sections:
                best_sec = next(
                    (s for s in sections if s.get("relation") == "direct"),
                    next(
                        (s for s in sections if s.get("relation") == "overlap"),
                        sections[0],
                    ),
                )
                best_sec_id = best_sec.get("id")

            features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(op_geom),
                    "properties": {
                        "id": op_start_node,
                        "feature_type": "node",
                        "node_type": "opening_start",
                        "node_id": op_start_node,
                        "bridge_id": bridge_id,
                        "opening_id": op_id,
                    },
                }
            )
            features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(op_geom),
                    "properties": {
                        "id": op_end_node,
                        "feature_type": "node",
                        "node_type": "opening_end",
                        "node_id": op_end_node,
                        "bridge_id": bridge_id,
                        "opening_id": op_id,
                    },
                }
            )

            if split_point:
                approach_geom = (
                    LineString([split_point, op_geom])
                    if not split_point.equals(op_geom)
                    else None
                )
                features.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(approach_geom) if approach_geom else None,
                        "properties": {
                            "id": f"bridge_approach_{bridge_id}_{op_id}",
                            "feature_type": "fairway_segment",
                            "segment_type": "bridge_approach",
                            "structure_type": "bridge",
                            "structure_id": bridge_id,
                            "bridge_id": bridge_id,
                            "opening_id": op_id,
                            "section_id": best_sec_id,
                            "source_node": split_node_id,
                            "target_node": op_start_node,
                            "length_m": geod.geometry_length(approach_geom)
                            if approach_geom
                            else 0.0,
                        },
                    }
                )

            passage_geom = LineString([op_geom, op_geom])

            # Serialize metadata and handle nominal length
            op_attrs = {}
            for k, v in opening.items():
                if k in ["id", "geometry"]:
                    continue
                if isinstance(v, (list, dict)):
                    op_attrs[k] = json.dumps(v)
                elif pd.isna(v):
                    op_attrs[k] = None
                else:
                    op_attrs[k] = v

            features.append(
                {
                    "type": "Feature",
                    "geometry": mapping(passage_geom),
                    "properties": {
                        **op_attrs,
                        "id": f"bridge_passage_{bridge_id}_{op_id}",
                        "feature_type": "fairway_segment",
                        "segment_type": "bridge_passage",
                        "structure_type": "bridge",
                        "structure_id": bridge_id,
                        "bridge_id": bridge_id,
                        "opening_id": op_id,
                        "section_id": best_sec_id,
                        "source_node": op_start_node,
                        "target_node": op_end_node,
                        "length_m": 2.0,  # Nominal length for simulation compatibility
                    },
                }
            )

            if merge_point:
                exit_geom = (
                    LineString([op_geom, merge_point])
                    if not op_geom.equals(merge_point)
                    else None
                )
                features.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(exit_geom) if exit_geom else None,
                        "properties": {
                            "id": f"bridge_exit_{bridge_id}_{op_id}",
                            "feature_type": "fairway_segment",
                            "segment_type": "bridge_exit",
                            "structure_type": "bridge",
                            "structure_id": bridge_id,
                            "bridge_id": bridge_id,
                            "opening_id": op_id,
                            "section_id": best_sec_id,
                            "source_node": op_end_node,
                            "target_node": merge_node_id,
                            "length_m": geod.geometry_length(exit_geom)
                            if exit_geom
                            else 0.0,
                        },
                    }
                )

    return features
