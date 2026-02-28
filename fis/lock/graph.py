import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, mapping
from pyproj import Geod
from fis.lock.utils import find_chamber_doors

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
        return gpd.GeoDataFrame(columns=["id", "node_type", "lock_id", "geometry"], crs=CRS)
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
        return gpd.GeoDataFrame(columns=["id", "segment_type", "lock_id", "geometry"], crs=CRS)
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
            attrs = {k: v for k, v in berth.items() if k not in _SKIP and not isinstance(v, (list, dict))}
            rows.append({**attrs, "lock_id": c["id"], "geometry": geom})
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
    _SKIP = {"geometry", "locks", "berths", "sections", "geometry_before_wkt", "geometry_after_wkt"}

    rows = []
    for c in complexes:
        if not c.get("geometry"):
            continue
        geom = wkt.loads(c["geometry"])
        attrs = {k: v for k, v in c.items() if k not in _SKIP and not isinstance(v, (list, dict))}
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
                attrs = {k: v for k, v in chamber.items() if k not in _SKIP and not isinstance(v, (list, dict))}
                rows.append({**attrs, "lock_id": c["id"], "lock_name": c.get("name"), "geometry": geom})
    if not rows:
        return gpd.GeoDataFrame(columns=["id", "name", "lock_id", "lock_name", "geometry"], crs=CRS)
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
                    attrs = {k: v for k, v in sc.items() if k not in _SKIP and not isinstance(v, (list, dict))}
                    rows.append({
                        **attrs,
                        "lock_id": c["id"],
                        "lock_name": c.get("name"),
                        "chamber_id": chamber["id"],
                        "chamber_name": chamber.get("name"),
                        "geometry": geom
                    })
    if not rows:
        return gpd.GeoDataFrame(columns=["id", "name", "lock_id", "chamber_id", "geometry"], crs=CRS)
    return gpd.GeoDataFrame(rows, geometry="geometry", crs=CRS)



def _geom_from_feature(feature):
    """Convert a GeoJSON feature geometry dict to a Shapely geometry."""
    from shapely.geometry import shape
    return shape(feature["geometry"])



def build_graph_features(complexes):
    """
    Flatten hierarchical complex objects into a list of GeoJSON features (Nodes and Edges).
    """
    features = []
    seen_nodes = set()
    
    for c in complexes:
        # Lock Feature
        geom = wkt.loads(c["geometry"]) if c.get("geometry") else None
        if geom:
             # Basic properties
             props = {k: v for k, v in c.items() if k not in ["geometry", "locks", "berths", "geometry_before_wkt", "geometry_after_wkt"]}
             props["feature_type"] = "lock"
             features.append({
                  "type": "Feature",
                  "geometry": mapping(geom),
                  "properties": {
                      **props,
                      "id": str(props.get("id")),
                      "feature_type": "lock"
                  }
              })
             
        # Nodes and Fairway Segments
        start_node = str(c.get("start_junction_id")) if c.get("start_junction_id") else None
        end_node = str(c.get("end_junction_id")) if c.get("end_junction_id") else None
        split_node_id = f"lock_{c['id']}_split"
        merge_node_id = f"lock_{c['id']}_merge"
        
        # Pre-calculate points
        split_point = None
        if c.get("geometry_before_wkt"):
            g_before = wkt.loads(c["geometry_before_wkt"])
            split_point = Point(g_before.coords[-1])

        merge_point = None
        if c.get("geometry_after_wkt"):
            g_after = wkt.loads(c["geometry_after_wkt"])
            merge_point = Point(g_after.coords[0])

        features.extend(_process_fairway_connections(c, seen_nodes, start_node, end_node, split_node_id, merge_node_id, split_point, merge_point))

        # Intersecting Fairway Sections
        for section in c.get("sections", []):
            if section.get("geometry"):
                    s_geom = wkt.loads(section["geometry"])
                    features.append({
                        "type": "Feature",
                        "geometry": mapping(s_geom),
                        "properties": {
                            "id": str(section.get("id")),
                            "feature_type": "fairway_section",
                            "name": section.get("name"),
                            "lock_id": c["id"],
                            "section_id": section.get("id"),
                            "fairway_id": section.get("fairway_id"),
                            "length": section.get("length"),
                            "length_m": geod.geometry_length(s_geom),
                            "relation": section.get("relation")
                        }
                    })

        # Chambers and Chamber Routes
        features.extend(_process_chambers(c, split_node_id, merge_node_id, split_point, merge_point))

        # Berths
        features.extend(_process_berths(c))

    return features

def _process_fairway_connections(c, seen_nodes, start_node, end_node, split_node_id, merge_node_id, split_point, merge_point):
    """
    Helper to process fairway connections (upstream/downstream) and key nodes.
    """
    features = []
    
    if c.get("geometry_before_wkt"):
        g_before_edges = wkt.loads(c["geometry_before_wkt"])
        
        # Split Node
        if split_point:
            features.append({
                "type": "Feature",
                "geometry": mapping(split_point),
                "properties": {
                    "id": split_node_id,
                    "feature_type": "node",
                    "node_type": "lock_split",
                    "node_id": split_node_id,
                    "lock_id": c["id"]
                }
            })
        
        # Before Segment
        features.append({
            "type": "Feature",
            "geometry": mapping(g_before_edges),
                "properties": {
                    "id": f"fairway_segment_{c['id']}_before",
                    "feature_type": "fairway_segment",
                    "segment_type": "before",
                    "lock_id": c["id"],
                    "fairway_id": c.get("fairway_id"),
                    "name": c.get("fairway_name"),
                    "section_id": c.get("sections", [{}])[0].get("id") if c.get("sections") else None,
                    "source_node": start_node,
                    "target_node": split_node_id,
                    "length_m": geod.geometry_length(g_before_edges)
                }
            })
        
        # Start Node (Junction)
        if start_node and start_node not in seen_nodes:
            start_point = Point(g_before_edges.coords[0])
            features.append({
                "type": "Feature",
                "geometry": mapping(start_point),
                "properties": {
                     "id": str(start_node),
                     "feature_type": "node",
                     "node_type": "junction",
                     "node_id": str(start_node),
                     "lock_id": c["id"]
                }
            })
            seen_nodes.add(start_node)

    if c.get("geometry_after_wkt"):
        g_after_edges = wkt.loads(c["geometry_after_wkt"])
        
        # Merge Node
        if merge_point:
            features.append({
                "type": "Feature",
                "geometry": mapping(merge_point),
                "properties": {
                    "id": merge_node_id,
                    "feature_type": "node",
                    "node_type": "lock_merge",
                    "node_id": merge_node_id,
                    "lock_id": c["id"]
                }
            })

        # After Segment
        features.append({
            "type": "Feature",
            "geometry": mapping(g_after_edges),
            "properties": {
                "id": f"fairway_segment_{c['id']}_after",
                "feature_type": "fairway_segment",
                "segment_type": "after",
                "lock_id": c["id"],
                "fairway_id": c.get("fairway_id"),
                "name": c.get("fairway_name"),
                "section_id": c.get("sections", [{}])[0].get("id") if c.get("sections") else None,
                "source_node": merge_node_id,
                "target_node": end_node,
                "length_m": geod.geometry_length(g_after_edges)
            }
        })
        
        # End Node (Junction)
        if end_node and end_node not in seen_nodes:
            end_point = Point(g_after_edges.coords[-1])
            features.append({
                "type": "Feature",
                "geometry": mapping(end_point),
                "properties": {
                     "id": str(end_node),
                     "feature_type": "node",
                     "node_type": "junction",
                     "node_id": str(end_node),
                     "lock_id": c["id"]
                }
            })
            seen_nodes.add(end_node)
            
    return features

def _process_berths(c):
    """
    Helper to process berths and generate related graph features.
    """
    features = []
    for berth in c.get("berths", []):
         if berth.get("geometry"):
             b_geom = wkt.loads(berth["geometry"])
             features.append({
                 "type": "Feature",
                 "geometry": mapping(b_geom),
                 "properties": {
                     "id": str(berth.get("id")),
                     "feature_type": "berth",
                     "name": berth.get("name"),
                     "lock_id": c["id"],
                     "berth_id": berth.get("id"),
                     "dist_m": berth.get("dist_m"),
                     "relation": berth.get("relation")
                 }
             })
    return features

def _process_chambers(c, split_node_id, merge_node_id, split_point, merge_point):
    """
    Helper to process chambers and generate related graph features.
    """
    features = []
    
    # Import locally if needed to avoid circular import issues, though we imported at top
    from shapely.geometry import LineString
    
    for l_obj in c.get("locks", []):
        for chamber in l_obj.get("chambers", []):
            chamber_id = chamber.get("id")
            chamber_node_start_id = f"chamber_{chamber_id}_start"
            chamber_node_end_id = f"chamber_{chamber_id}_end"
            
            c_geom = None
            if chamber.get("geometry") and pd.notna(chamber["geometry"]):
                c_geom = wkt.loads(chamber["geometry"]) if isinstance(chamber["geometry"], str) else chamber["geometry"]

            # Try to find doors
            door_start = None
            door_end = None
            if c_geom and split_point and merge_point:
                 door_start, door_end = find_chamber_doors(c_geom, split_point, merge_point)
            
            # Chamber Nodes
            if door_start and door_end:
                 features.extend(_build_chamber_route_features(
                     c, chamber_id, chamber_node_start_id, chamber_node_end_id,
                     door_start, door_end, split_point, merge_point,
                     split_node_id, merge_node_id
                 ))


            else:
                 # Fallback to centroid if doors not found or points missing
                 chamber_node_id = f"chamber_{chamber_id}"
                 centroid = c_geom.centroid if c_geom else None
                 if centroid:
                     features.append({
                         "type": "Feature",
                         "geometry": mapping(centroid),
                         "properties": {
                             "id": chamber_node_id,
                             "feature_type": "node",
                             "node_type": "chamber",
                             "node_id": chamber_node_id,
                             "lock_id": c["id"],
                             "chamber_id": chamber_id
                         }
                     })
                     
                 if chamber.get("route_geometry"):
                     features.append({
                         "type": "Feature",
                         "geometry": mapping(wkt.loads(chamber["route_geometry"])),
                         "properties": {
                             "feature_type": "fairway_segment",
                             "segment_type": "chamber_route", # Fallback type
                             "lock_id": c["id"],
                             "chamber_id": chamber_id,
                             "fairway_id": c.get("fairway_id"),
                             "source_node": split_node_id,
                             "target_node": merge_node_id, 
                             "intermediate_node": chamber_node_id,
                             "length_m": geod.geometry_length(wkt.loads(chamber["route_geometry"]))
                         }
                     })

            # Chamber Geometry Feature (Polygon)
            if c_geom:
                features.append({
                    "type": "Feature",
                    "geometry": mapping(c_geom),
                    "properties": {
                        "feature_type": "chamber",
                        "name": chamber.get("name"),
                        "lock_id": c["id"],
                        "chamber_id": chamber_id,
                        "length": chamber.get("length"),
                        "width": chamber.get("width")
                    }
                })
                
    return features

def _build_chamber_route_features(c, chamber_id, chamber_node_start_id, chamber_node_end_id, door_start, door_end, split_point, merge_point, split_node_id, merge_node_id):
    """
    Helper to extract routing lines from split nodes to doors and through chambers.
    """
    from shapely.geometry import LineString
    features = []
    
    # Start Node
    features.append({
        "type": "Feature",
        "geometry": mapping(door_start),
        "properties": {
            "id": chamber_node_start_id,
            "feature_type": "node",
            "node_type": "chamber_start",
            "node_id": chamber_node_start_id,
            "lock_id": c["id"],
            "chamber_id": chamber_id
        }
    })
    # End Node
    features.append({
        "type": "Feature",
        "geometry": mapping(door_end),
        "properties": {
            "id": chamber_node_end_id,
            "feature_type": "node",
            "node_type": "chamber_end",
            "node_id": chamber_node_end_id,
            "lock_id": c["id"],
            "chamber_id": chamber_id
        }
    })
    
    # Edges
    # Approach (Split -> Start)
    approach_line = LineString([split_point, door_start])
    features.append({
        "type": "Feature",
        "geometry": mapping(approach_line),
        "properties": {
            "id": f"fairway_segment_{c['id']}_{chamber_id}_approach",
            "feature_type": "fairway_segment",
            "segment_type": "chamber_approach",
            "lock_id": c["id"],
            "chamber_id": chamber_id,
            "fairway_id": c.get("fairway_id"),
            "name": c.get("fairway_name"),
            "section_id": c.get("sections", [{}])[0].get("id") if c.get("sections") else None,
            "source_node": split_node_id,
            "target_node": chamber_node_start_id,
            "length_m": geod.geometry_length(approach_line)
        }
    })
    
    # Chamber (Start -> End)
    chamber_line = LineString([door_start, door_end])
    features.append({
        "type": "Feature",
        "geometry": mapping(chamber_line),
        "properties": {
            "id": f"fairway_segment_{c['id']}_{chamber_id}_route",
            "feature_type": "fairway_segment",
            "segment_type": "chamber_route",
            "lock_id": c["id"],
            "chamber_id": chamber_id,
            "fairway_id": c.get("fairway_id"),
            "name": c.get("fairway_name"),
            "section_id": c.get("sections", [{}])[0].get("id") if c.get("sections") else None,
            "source_node": chamber_node_start_id,
            "target_node": chamber_node_end_id,
            "length_m": geod.geometry_length(chamber_line)
        }
    })
    
    # Exit (End -> Merge)
    exit_line = LineString([door_end, merge_point])
    features.append({
        "type": "Feature",
        "geometry": mapping(exit_line),
        "properties": {
            "id": f"fairway_segment_{c['id']}_{chamber_id}_exit",
            "feature_type": "fairway_segment",
            "segment_type": "chamber_exit",
            "lock_id": c["id"],
            "chamber_id": chamber_id,
            "fairway_id": c.get("fairway_id"),
            "name": c.get("fairway_name"),
            "section_id": c.get("sections", [{}])[0].get("id") if c.get("sections") else None,
            "source_node": chamber_node_end_id,
            "target_node": merge_node_id,
            "length_m": geod.geometry_length(exit_line)
        }
    })

    return features
