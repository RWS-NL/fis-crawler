
import json
import logging
import pathlib
import sys

import click
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, LineString
from shapely.ops import substring
from fis.ris_index import load_ris_index

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    
    if locks is None or chambers is None:
        raise FileNotFoundError("Missing essential lock/chamber data.")

    return locks, chambers, isrs, fairways, berths

def split_fairway(fairway_geom, lock_km, fairway_start_km, fairway_end_km):
    """
    Split the fairway geometry at the lock's location based on KM mark.
    """
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

def process_fairway_geometry(fw_row, lock_row):
    """
    Calculate fairway segments and distance using metric projection (EPSG:28992).
    """
    fairway_data = {}
    
    # Extract geometries safely
    fw_geom = fw_row.geometry if hasattr(fw_row, "geometry") else None
    lock_geom = lock_row.geometry if hasattr(lock_row, "geometry") else None
    
    # 1. Fallback / Initial Calculation: KM-based Split
    # We only try this if we have RouteKmBegin
    if fw_geom and pd.notna(lock_row.get("RouteKmBegin")):
         geom_before, geom_after = split_fairway(
             fw_geom, 
             lock_row["RouteKmBegin"], 
             fw_row.get("RouteKmBegin", 0), 
             fw_row.get("RouteKmEnd", 0)
         )
         # Populate early as fallback
         if geom_before:
             fairway_data["geometry_before_wkt"] = geom_before.wkt
             fairway_data["geometry_after_wkt"] = geom_after.wkt

    # 2. Refinement: Accurate Spatial Projection (EPSG:28992)
    # If we have both geometries, we can project to get precise metric distance/split
    if lock_geom and fw_geom:
        try:
            # Create GeoSeries for projection
            gs_lock = gpd.GeoSeries([lock_geom], crs="EPSG:4326")
            gs_fw = gpd.GeoSeries([fw_geom], crs="EPSG:4326")
            
            # Reproject to RD New (EPSG:28992) for meters
            gs_lock = gs_lock.to_crs("EPSG:28992")
            gs_fw = gs_fw.to_crs("EPSG:28992")
            
            lock_point_rd = gs_lock.iloc[0]
            fw_line_rd = gs_fw.iloc[0]
            
            # Ensure lock geometry is a point
            if lock_point_rd.geom_type != 'Point':
                lock_point_rd = lock_point_rd.centroid

            # Project lock point to line (in meters)
            projected_dist = fw_line_rd.project(lock_point_rd)
            projected_point = fw_line_rd.interpolate(projected_dist)
            
            fairway_data["lock_to_fairway_distance_meters"] = lock_point_rd.distance(projected_point)
            
            # Split using spatial projection (more accurate than KM)
            before_spatial_rd = substring(fw_line_rd, 0, projected_dist)
            after_spatial_rd = substring(fw_line_rd, projected_dist, fw_line_rd.length)
            
            # Project back to 4326 for WKT output
            before_spatial = gpd.GeoSeries([before_spatial_rd], crs="EPSG:28992").to_crs("EPSG:4326").iloc[0]
            after_spatial = gpd.GeoSeries([after_spatial_rd], crs="EPSG:28992").to_crs("EPSG:4326").iloc[0]

            fairway_data["geometry_before_wkt"] = before_spatial.wkt
            fairway_data["geometry_after_wkt"] = after_spatial.wkt
            
        except Exception as e:
            logger.warning(f"Projection/Splitting failed for lock {lock_row['Id']}: {e}")

    return fairway_data

def find_nearby_berths(lock_row, berths_gdf, fairway_geom_before, fairway_geom_after, max_dist_m=2000):
    """
    Find berths associated with the lock's fairway and determine if they are before or after.
    Enforces a strict distance check (default 2km).
    """
    nearby = []
    if berths_gdf is None or "FairwayId" not in berths_gdf.columns:
        return nearby
        
    # 1. Filter by FairwayId
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
        
        # 1. KM Check (Fast and robust for fairways)
        berth_km = berth.get("RouteKmBegin")
        if pd.notna(lock_km) and pd.notna(berth_km):
            # Calculate absolute KM distance
            km_diff = abs(lock_km - berth_km)
            # Convert to meters (approx)
            dist_m = km_diff * 1000
            if dist_m <= max_dist_m:
                is_nearby = True
                
        # 2. Spatial Check (Fallback if KM missing or verification needed)
        elif lock_geom and berth.geometry:
            try:
                # Project to EPSG:28992 for metric distance
                gs = gpd.GeoSeries([lock_geom, berth.geometry], crs="EPSG:4326")
                gs_rd = gs.to_crs("EPSG:28992")
                dist_m = gs_rd.iloc[0].distance(gs_rd.iloc[1])
                
                if dist_m <= max_dist_m:
                    is_nearby = True
            except Exception:
                pass # Fail silently on projection errors

        if is_nearby:
            nearby.append({
                "id": int(berth["Id"]),
                "name": berth.get("Name"),
                "km": float(berth_km) if pd.notna(berth_km) else None,
                "dist_m": round(dist_m, 1) if dist_m is not None else None,
                "geometry": berth.geometry.wkt if hasattr(berth, "geometry") and berth.geometry else None,
                "relation": "nearby" 
            })

    return nearby


def group_complexes(locks, chambers, isrs, ris_df, fairways, berths):
    """
    Group locks into complexes and enrich with ISRS, RIS, Fairway, and Berth data.
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
                 # Delegate complexity to helper function
                 geom_data = process_fairway_geometry(fw_obj, lock)
                 fairway_data.update(geom_data)
        
        # Berth Identification
        berths_data = []
        if berths_gdf is not None:
             # We pass the WKTs from fairway_data if we want to parse them, 
             # or we rely on KM/FairwayId in the helper function.
             berths_data = find_nearby_berths(lock, berths_gdf, fairway_data.get("geometry_before_wkt"), fairway_data.get("geometry_after_wkt"))

        complex_obj = {
            "id": int(lock["Id"]),
            "name": lock["Name"],
            "isrs_code": lock_isrs_code,
            "geometry": lock.geometry.wkt if hasattr(lock, "geometry") and lock.geometry else None,
            **ris_info,
            **fairway_data,
            "berths": berths_data,
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
             c_obj = {
                 "id": int(chamber["Id"]),
                 "name": chamber["Name"],
                 "length": float(chamber["Length"]) if pd.notna(chamber["Length"]) else None,
                 "width": float(chamber["Width"]) if pd.notna(chamber["Width"]) else None,
             }
             complex_obj["locks"][0]["chambers"].append(c_obj)
             
        complexes.append(complex_obj)
        
    return complexes

@click.command()
@click.option("--export-dir", default="fis-export", help="Directory containing input parquet files.")
def main(export_dir):
    data_dir = pathlib.Path(export_dir)
    try:
        locks, chambers, isrs, fairways, berths = load_data(data_dir)
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    # Load RIS Index
    ris_df = None
    try:
        ris_df = load_ris_index(data_dir / "RisIndexNL.xlsx")
        logger.info(f"Loaded {len(ris_df)} RIS Index entries")
    except Exception as e:
        logger.warning(f"Could not load RIS Index: {e}")

    # Create output directory
    output_dir = data_dir.parent / "lock-output"
    output_dir.mkdir(parents=True, exist_ok=True)

    result = group_complexes(locks, chambers, isrs, ris_df, fairways, berths)
    
    # 1. Standard JSON Output (Full Detail)
    output_json = output_dir / "lock_schematization.json"
    with open(output_json, "w") as f:
        json.dump(result, f, indent=2)
    logger.info(f"Saved JSON to {output_json}")

    # 2. Geospatial Outputs (GeoJSON / GeoParquet)
    if result:
        df = pd.DataFrame(result)
        
        # Enforce integer type for fairway_id (nullable)
        if "fairway_id" in df.columns:
            df["fairway_id"] = df["fairway_id"].astype("Int64")
        
        # Convert WKT geometry back to shapely
        if "geometry" in df.columns:
            df["geometry"] = df["geometry"].apply(lambda x: wkt.loads(x) if x else None)
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
            
            # Serialize nested 'locks' (chambers) for GIS formats that don't support objects
            # For GeoParquet it might be supported depending on the viewer, but string is safest for GeoJSON.
            # Actually Geopandas to_file (fiona) fails with dict/list properties.
            gdf_flat = gdf.copy()
            gdf_flat["locks"] = gdf_flat["locks"].apply(json.dumps)
            # Serialize berths as well if they are distinct objects
            if "berths" in gdf_flat.columns:
                 gdf_flat["berths"] = gdf_flat["berths"].apply(json.dumps)
            
            # Save GeoJSON
            output_geojson = output_dir / "lock_schematization.geojson"
            gdf_flat.to_file(output_geojson, driver="GeoJSON")
            logger.info(f"Saved GeoJSON to {output_geojson}")
            
            # Save GeoParquet (supports better types usually, but let's stick to flattened for consistency)
            output_geoparquet = output_dir / "lock_schematization.geoparquet"
            gdf_flat.to_parquet(output_geoparquet)
            logger.info(f"Saved GeoParquet to {output_geoparquet}")

if __name__ == "__main__":
    main()
