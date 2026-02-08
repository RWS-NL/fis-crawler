import pandas as pd
from shapely import wkt
from shapely.geometry import Point, mapping
from fis.utils import find_chamber_doors

def find_fairway_junctions(sections_gdf, fairway_id):
    """
    Identify start and end junctions for a given fairway based on sections.
    """
    start_junction = None
    end_junction = None
    
    if sections_gdf is None:
        return start_junction, end_junction
        
    fw_sections = sections_gdf[sections_gdf["FairwayId"] == int(fairway_id)]
    if fw_sections.empty:
        return start_junction, end_junction

    fw_sections = fw_sections.sort_values("RouteKmBegin")
    
    if pd.notna(fw_sections.iloc[0]["StartJunctionId"]):
        start_junction = int(fw_sections.iloc[0]["StartJunctionId"])
        
    if pd.notna(fw_sections.iloc[-1]["EndJunctionId"]):
        end_junction = int(fw_sections.iloc[-1]["EndJunctionId"])
                 
    return start_junction, end_junction

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
                 "properties": props
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
                try:
                    s_geom = wkt.loads(section["geometry"])
                    features.append({
                        "type": "Feature",
                        "geometry": mapping(s_geom),
                        "properties": {
                            "feature_type": "fairway_section",
                            "name": section.get("name"),
                            "lock_id": c["id"],
                            "section_id": section.get("id"),
                            "fairway_id": section.get("fairway_id"),
                            "length": section.get("length"),
                            "relation": section.get("relation")
                        }
                    })
                except Exception:
                    pass

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
                "feature_type": "fairway_segment",
                "segment_type": "before",
                "lock_id": c["id"],
                "fairway_id": c.get("fairway_id"),
                "source_node": start_node,
                "target_node": split_node_id
            }
        })
        
        # Start Node (Junction)
        if start_node and start_node not in seen_nodes:
            start_point = Point(g_before_edges.coords[0])
            features.append({
                "type": "Feature",
                "geometry": mapping(start_point),
                "properties": {
                     "feature_type": "node",
                     "node_type": "junction",
                     "node_id": start_node,
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
                "feature_type": "fairway_segment",
                "segment_type": "after",
                "lock_id": c["id"],
                "fairway_id": c.get("fairway_id"),
                "source_node": merge_node_id,
                "target_node": end_node
            }
        })
        
        # End Node (Junction)
        if end_node and end_node not in seen_nodes:
            end_point = Point(g_after_edges.coords[-1])
            features.append({
                "type": "Feature",
                "geometry": mapping(end_point),
                "properties": {
                     "feature_type": "node",
                     "node_type": "junction",
                     "node_id": end_node,
                     "lock_id": c["id"]
                }
            })
            seen_nodes.add(end_node)
            
    return features

# ... _process_berths stays the same ...

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
                try:
                    c_geom = wkt.loads(chamber["geometry"]) if isinstance(chamber["geometry"], str) else chamber["geometry"]
                except Exception:
                    pass

            # Try to find doors
            door_start = None
            door_end = None
            if c_geom and split_point and merge_point:
                 try:
                     door_start, door_end = find_chamber_doors(c_geom, split_point, merge_point)
                 except Exception:
                     pass
            
            # Chamber Nodes
            if door_start and door_end:
                 # Start Node
                 features.append({
                     "type": "Feature",
                     "geometry": mapping(door_start),
                     "properties": {
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
                         "feature_type": "node",
                         "node_type": "chamber_end",
                         "node_id": chamber_node_end_id,
                         "lock_id": c["id"],
                         "chamber_id": chamber_id
                     }
                 })
                 
                 # Edges
                 # 1. Approach (Split -> Start)
                 approach_line = LineString([split_point, door_start])
                 features.append({
                     "type": "Feature",
                     "geometry": mapping(approach_line),
                     "properties": {
                         "feature_type": "fairway_segment",
                         "segment_type": "chamber_approach",
                         "lock_id": c["id"],
                         "chamber_id": chamber_id,
                         "fairway_id": c.get("fairway_id"),
                         "source_node": split_node_id,
                         "target_node": chamber_node_start_id
                     }
                 })
                 
                 # 2. Chamber (Start -> End)
                 chamber_line = LineString([door_start, door_end])
                 features.append({
                     "type": "Feature",
                     "geometry": mapping(chamber_line),
                     "properties": {
                         "feature_type": "fairway_segment",
                         "segment_type": "chamber_route",
                         "lock_id": c["id"],
                         "chamber_id": chamber_id,
                         "fairway_id": c.get("fairway_id"),
                         "source_node": chamber_node_start_id,
                         "target_node": chamber_node_end_id
                     }
                 })
                 
                 # 3. Exit (End -> Merge)
                 exit_line = LineString([door_end, merge_point])
                 features.append({
                     "type": "Feature",
                     "geometry": mapping(exit_line),
                     "properties": {
                         "feature_type": "fairway_segment",
                         "segment_type": "chamber_exit",
                         "lock_id": c["id"],
                         "chamber_id": chamber_id,
                         "fairway_id": c.get("fairway_id"),
                         "source_node": chamber_node_end_id,
                         "target_node": merge_node_id
                     }
                 })

            else:
                 # Fallback to centroid if doors not found or points missing
                 chamber_node_id = f"chamber_{chamber_id}"
                 centroid = c_geom.centroid if c_geom else None
                 if centroid:
                     features.append({
                         "type": "Feature",
                         "geometry": mapping(centroid),
                         "properties": {
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
                             "intermediate_node": chamber_node_id
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
