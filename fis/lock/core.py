import pathlib
import pandas as pd
import geopandas as gpd
from shapely import wkt

from shapely.geometry import Point, LineString

def load_data(export_dir: pathlib.Path):
    """Load necessary parquet files."""
    def read_geo_or_parquet(stem):
        gpq = export_dir / f"{stem}.geoparquet"
        pq = export_dir / f"{stem}.parquet"
        if gpq.exists():
            return gpd.read_parquet(gpq)
        if pq.exists():
            df = pd.read_parquet(pq)
            if "Geometry" in df.columns and df["Geometry"].dtype == "object":
                df["geometry"] = df["Geometry"].apply(lambda x: wkt.loads(x) if x else None)
                return gpd.GeoDataFrame(df, geometry="geometry")
            return df
        return None

    locks = read_geo_or_parquet("lock")
    chambers = read_geo_or_parquet("chamber")
    isrs = read_geo_or_parquet("isrs")
    fairways = read_geo_or_parquet("fairway")
    berths = read_geo_or_parquet("berth")
    sections = read_geo_or_parquet("section")
    
    if locks is None or chambers is None:
        raise FileNotFoundError("Missing essential lock/chamber data.")

    return locks, chambers, isrs, fairways, berths, sections

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

def group_complexes(locks, chambers, isrs, ris_df, fairways, berths, sections):
    """
    Group locks into complexes and enrich with ISRS, RIS, Fairway, Berth, and Section data.
    """
    complexes = []
    
    # Convert locks to GeoDataFrame for spatial ops if needed
    if "Geometry" in locks.columns and locks["Geometry"].dtype == "object":
         locks["geometry"] = locks["Geometry"].apply(lambda x: wkt.loads(x) if x else None)
    locks_gdf = gpd.GeoDataFrame(locks, geometry="geometry")

    # Convert berths to GDF if needed
    berths_gdf = None
    if berths is not None:
        if "Geometry" in berths.columns and berths["Geometry"].dtype == "object":
             berths["geometry"] = berths["Geometry"].apply(lambda x: wkt.loads(x) if x else None)
        berths_gdf = gpd.GeoDataFrame(berths, geometry="geometry") if "geometry" in berths.columns else berths

    # Convert sections to GDF if needed
    sections_gdf = None
    if sections is not None:
        if "Geometry" in sections.columns and sections["Geometry"].dtype == "object":
             sections["geometry"] = sections["Geometry"].apply(lambda x: wkt.loads(x) if x else None)
        sections_gdf = gpd.GeoDataFrame(sections, geometry="geometry") if "geometry" in sections.columns else sections

    for idx, lock in locks_gdf.iterrows():
        # Get chambers for this lock (using confirmed ParentId key)
        lock_chambers = chambers[chambers["ParentId"] == lock["Id"]]
        
        # Resolve ISRS
        lock_isrs_code = None
        if pd.notna(lock.get("IsrsId")) and isrs is not None:
             isrs_row = isrs[isrs["Id"] == lock["IsrsId"]]
             if not isrs_row.empty:
                 lock_isrs_code = isrs_row.iloc[0]["Code"]

        # RIS Enrichment
        ris_info = {}
        if lock_isrs_code and ris_df is not None:
             match = ris_df[ris_df["isrs_code"] == lock_isrs_code]
             if not match.empty:
                 ris_info = {
                     "ris_name": match.iloc[0]["name"],
                     "ris_function": match.iloc[0]["function"]
                 }

        # Fairway Mapping
        fairway_data = {}
        fw_obj = None # Keep reference for processing
        if fairways is not None and pd.notna(lock.get("FairwayId")):
             fw_row = fairways[fairways["Id"] == lock["FairwayId"]]
             if not fw_row.empty:
                 fw_obj = fw_row.iloc[0]
                 fairway_data = {
                     "fairway_name": fw_obj["Name"],
                     "fairway_id": int(fw_obj["Id"])
                 }
                 # Calculate max chamber length for buffer
                 max_length = 0
                 if "Length" in lock_chambers.columns:
                     max_length = lock_chambers["Length"].max()
                 if pd.isna(max_length):
                     max_length = 0
                     
                 # Delegate complexity to helper function
                 # Buffer: half max length + 50m extra
                 buffer_dist = (max_length / 2) + 50
                 geom_data = process_fairway_geometry(fw_obj, lock, buffer_dist=buffer_dist)
                 fairway_data.update(geom_data)
                 
                 # Find junctions
                 if sections_gdf is not None:
                     s_junc, e_junc = find_fairway_junctions(sections_gdf, int(fw_obj["Id"]))
                     fairway_data["start_junction_id"] = s_junc
                     fairway_data["end_junction_id"] = e_junc

        # Chamber Route Generation (Virtual Fairways)
        # If we have valid split/merge points from the fairway, we can route through chambers
        chamber_routes = {}
        if "geometry_before_wkt" in fairway_data and "geometry_after_wkt" in fairway_data:
                     bwkt = fairway_data["geometry_before_wkt"]
                     awkt = fairway_data["geometry_after_wkt"]
                     if bwkt and awkt:
                         # Load geometries
                         g_before = wkt.loads(bwkt)
                         g_after = wkt.loads(awkt)
                         
                         # Identify connection points
                         # curve.coords is a list of tuples
                         split_point = Point(g_before.coords[-1])
                         merge_point = Point(g_after.coords[0])
                         
                         # We will calculate routes when iterating chambers below
                         chamber_routes["split_point"] = split_point
                         chamber_routes["merge_point"] = merge_point
        
        # Berth Identification
        berths_data = []
        if berths_gdf is not None:
             # We pass the WKTs from fairway_data if we want to parse them, 
             # or we rely on KM/FairwayId in the helper function.
             berths_data = find_nearby_berths(lock, berths_gdf, fairway_data.get("geometry_before_wkt"), fairway_data.get("geometry_after_wkt"))

        # Section Overlap Identification
        sections_data = []
        if sections_gdf is not None:
            # Define complex geometry: Union of lock + chambers
            # Start with lock geometry
            complex_geoms = [lock.geometry] if hasattr(lock, "geometry") and lock.geometry else []
            
            # Add chamber geometries
            if "Geometry" in lock_chambers.columns:
                 for _, c_row in lock_chambers.iterrows():
                      if pd.notna(c_row["Geometry"]):
                       if pd.notna(c_row["Geometry"]):
                            c_geom = wkt.loads(c_row["Geometry"])
                            complex_geoms.append(c_geom)
            
            if complex_geoms:
                 from shapely.ops import unary_union
                 complex_union = unary_union([g for g in complex_geoms if g])
                 if complex_union:
                      # Find intersecting sections
                      intersecting = sections_gdf[sections_gdf.intersects(complex_union)]
                      
                      for _, s_row in intersecting.iterrows():
                           sections_data.append({
                               "id": int(s_row["Id"]),
                               "name": s_row["Name"],
                               "fairway_id": int(s_row["FairwayId"]) if pd.notna(s_row.get("FairwayId")) else None,
                               "length": float(s_row["Length"]) if pd.notna(s_row.get("Length")) else None,
                               "geometry": s_row.geometry.wkt if hasattr(s_row, "geometry") and s_row.geometry else None,
                               "relation": "overlap"
                           })

        complex_obj = {
            "id": int(lock["Id"]),
            "name": lock["Name"],
            "isrs_code": lock_isrs_code,
            "geometry": lock.geometry.wkt if hasattr(lock, "geometry") and lock.geometry else None,
            **ris_info,
            **fairway_data,
            "berths": berths_data,
            "sections": sections_data,
            "locks": [  
                {
                     "id": int(lock["Id"]),
                     "name": lock["Name"],
                     "chambers": []
                }
            ]
        }
        
        # Add chambers
        for _, chamber in lock_chambers.iterrows():
             # Add Chamber Route (Virtual Fairway)
             route_wkt = None
             if "split_point" in chamber_routes and "merge_point" in chamber_routes:
                     if "Geometry" in chamber and pd.notna(chamber["Geometry"]):
                         ch_geom = wkt.loads(chamber["Geometry"])
                         centroid = ch_geom.centroid
                         route = LineString([chamber_routes["split_point"], centroid, chamber_routes["merge_point"]])
                         route_wkt = route.wkt

             c_obj = {
                 "id": int(chamber["Id"]),
                 "name": chamber["Name"],
                 "length": float(chamber["Length"]) if pd.notna(chamber["Length"]) else None,
                 "width": float(chamber["Width"]) if pd.notna(chamber["Width"]) else None,
                 "geometry": chamber["Geometry"] if "Geometry" in chamber and pd.notna(chamber["Geometry"]) else None,
                 "route_geometry": route_wkt
             }
             complex_obj["locks"][0]["chambers"].append(c_obj)
             
        complexes.append(complex_obj)
        
    return complexes

def split_fairway(fairway_geom, lock_km, fairway_start_km, fairway_end_km):
    """
    Split the fairway geometry at the lock's location based on KM mark.
    """
    from shapely.ops import substring
    from shapely.geometry import LineString
    
    if not fairway_geom or not isinstance(fairway_geom, LineString):
        return None, None
        
    total_len = fairway_geom.length
    section_len_km = abs(fairway_end_km - fairway_start_km)
    
    if section_len_km == 0:
        return None, None

    # Determine ratio
    if fairway_end_km > fairway_start_km:
        ratio = (lock_km - fairway_start_km) / section_len_km
    else:
        # Decreasing KM mapping
        ratio = (fairway_start_km - lock_km) / section_len_km

    dist_on_line = ratio * total_len
    dist_on_line = max(0.0, min(total_len, dist_on_line)) # Clamp

    before = substring(fairway_geom, 0, dist_on_line)
    after = substring(fairway_geom, dist_on_line, total_len)
    
    return before, after

def process_fairway_geometry(fw_row, lock_row, buffer_dist=0):
    """
    Calculate fairway segments and distance using metric projection (EPSG:28992).
    """
    from shapely.ops import substring
    import logging
    logger = logging.getLogger(__name__)
    
    fairway_data = {}
    
    # Extract geometries safely
    fw_geom = fw_row.geometry if hasattr(fw_row, "geometry") else None
    lock_geom = lock_row.geometry if hasattr(lock_row, "geometry") else None
    
    # KM-based Split (Fallback)
    if fw_geom and pd.notna(lock_row.get("RouteKmBegin")):
         geom_before, geom_after = split_fairway(
             fw_geom, 
             lock_row["RouteKmBegin"], 
             fw_row.get("RouteKmBegin", 0), 
             fw_row.get("RouteKmEnd", 0)
         )
         if geom_before:
             fairway_data["geometry_before_wkt"] = geom_before.wkt
             fairway_data["geometry_after_wkt"] = geom_after.wkt

    # Accurate Spatial Projection (EPSG:28992)
    if lock_geom and fw_geom:
        # Create GeoSeries for projection
        gs_lock = gpd.GeoSeries([lock_geom], crs="EPSG:4326")
        gs_fw = gpd.GeoSeries([fw_geom], crs="EPSG:4326")
        
        # Reproject to RD New (EPSG:28992) for meters
        gs_lock = gs_lock.to_crs("EPSG:28992")
        gs_fw = gs_fw.to_crs("EPSG:28992")
        
        lock_point_rd = gs_lock.iloc[0]
        fw_line_rd = gs_fw.iloc[0]
        
        if lock_point_rd.geom_type != 'Point':
            lock_point_rd = lock_point_rd.centroid

        # Project lock point to line (in meters)
        projected_dist = fw_line_rd.project(lock_point_rd)
        projected_point = fw_line_rd.interpolate(projected_dist)
        
        fairway_data["lock_to_fairway_distance_meters"] = lock_point_rd.distance(projected_point)
        
        # Split using spatial projection with buffer
        dist_split = max(0, projected_dist - buffer_dist)
        dist_merge = min(fw_line_rd.length, projected_dist + buffer_dist)
        
        before_spatial_rd = substring(fw_line_rd, 0, dist_split)
        after_spatial_rd = substring(fw_line_rd, dist_merge, fw_line_rd.length)
        
        # Project back to 4326 for WKT output
        before_spatial = gpd.GeoSeries([before_spatial_rd], crs="EPSG:28992").to_crs("EPSG:4326").iloc[0]
        after_spatial = gpd.GeoSeries([after_spatial_rd], crs="EPSG:28992").to_crs("EPSG:4326").iloc[0]

        fairway_data["geometry_before_wkt"] = before_spatial.wkt
        fairway_data["geometry_after_wkt"] = after_spatial.wkt

    return fairway_data

def find_nearby_berths(lock_row, berths_gdf, fairway_geom_before, fairway_geom_after, max_dist_m=2000):
    """
    Find berths associated with the lock's fairway and determine if they are before or after.
    Enforces a strict distance check (default 2km).
    """
    nearby = []
    if berths_gdf is None or "FairwayId" not in berths_gdf.columns:
        return nearby
        
    # Filter by FairwayId
    fw_id = lock_row.get("FairwayId")
    if pd.isna(fw_id):
        return nearby
        
    # Filter candidates
    candidates = berths_gdf[berths_gdf["FairwayId"] == fw_id].copy()
    
    if candidates.empty:
        return nearby
        
    lock_geom = lock_row.geometry if hasattr(lock_row, "geometry") else None
    lock_km = lock_row.get("RouteKmBegin")
    
    for _, berth in candidates.iterrows():
        is_nearby = False
        dist_m = None
        berth_km = berth.get("RouteKmBegin")
        
        # Calculate spatial distance if geometries exist
        if lock_geom and berth.geometry:
            from pyproj import Geod
            from shapely.geometry import Point
            
            # Ensure we are comparing Points for Geod.inv
            lg = lock_geom if isinstance(lock_geom, Point) else lock_geom.centroid
            bg = berth.geometry if isinstance(berth.geometry, Point) else berth.geometry.centroid
            
            if lg and bg:
                geod = Geod(ellps="WGS84")
                _, _, dist_m = geod.inv(lg.x, lg.y, bg.x, bg.y)
                
                if dist_m <= max_dist_m:
                    is_nearby = True

        if not is_nearby:
            continue

        # Determine relation (before/after)
        relation = "unknown"


        # Spatial Projection (Substrings)
        # We have fairway_geom_before and fairway_geom_after WKTs
        if fairway_geom_before and fairway_geom_after and berth.geometry:
             from shapely import wkt
             g_before = wkt.loads(fairway_geom_before)
             g_after = wkt.loads(fairway_geom_after)
             
             # Buffer slightly for robustness
             if g_before.distance(berth.geometry) < g_after.distance(berth.geometry):
                 relation = "before"
             else:
                 relation = "after"

        nearby.append({
            "id": int(berth["Id"]),
            "name": berth.get("Name"),
            "km": float(berth_km) if pd.notna(berth_km) else None,
            "dist_m": round(dist_m, 1) if dist_m is not None else None,
            "geometry": berth.geometry.wkt if hasattr(berth, "geometry") and berth.geometry else None,
            "relation": relation 
        })

    return nearby
