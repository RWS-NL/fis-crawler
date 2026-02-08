
import sys
import pathlib
sys.path.append(str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import geopandas as gpd
from shapely import wkt

def inspect_fairway_54726():
    print("Inspecting Fairway 54726...")
    
    output_dir = "output/lock-output"
    try:
        # Load the generated schematization (GeoJSON or Parquet)
        # Using GeoJSON as it is the final output format mentioned
        gdf = gpd.read_file(f"{output_dir}/lock_schematization.geojson")
    except Exception as e:
        print(f"Error loading schematization: {e}")
        return

    # Find segments related to fairway 54726
    # We might need to look up which lock is on this fairway first from the raw data
    # or just filter the output if fairway_id is present.
    
    # Let's verify if fairway_id is in the columns
    if "fairway_id" not in gdf.columns:
        print("Column 'fairway_id' not found in output.")
        print(gdf.columns)
        return

    # Filter features for this fairway
    features = gdf[gdf["fairway_id"] == 54726]
    
    if features.empty:
        print("No features found for fairway_id 54726.")
        # Try finding the lock based on raw data to see if it was skipped or ID mismatch
        raw_lock_path = "output/fis-export/lock.parquet"
        try:
            locks_df = pd.read_parquet(raw_lock_path)
            lock_on_fw = locks_df[locks_df["FairwayId"] == 54726]
            if not lock_on_fw.empty:
                print(f"Found Lock(s) on Fairway 54726 in raw data: {lock_on_fw['Id'].tolist()}")
                # Now search for these locks in the output
                for lock_id in lock_on_fw['Id']:
                    l_feats = gdf[gdf["lock_id"] == lock_id]
                    print(f"Features for Lock {lock_id}: {len(l_feats)}")
                    print(l_feats[["feature_type", "segment_type", "source_node", "target_node", "section_id", "name"]].to_string())
            else:
                print("No locks found on Fairway 54726 in raw data.")
        except Exception as e:
            print(f"Could not load raw lock data: {e}")
        return

    print(f"Found {len(features)} features for fairway 54726.")
    
    segments = features[features["feature_type"] == "fairway_segment"]
    print(f"Fairway Segments: {len(segments)}")
    
    if not segments.empty:
        cols = ["id", "feature_type", "segment_type", "source_node", "target_node", "section_id", "name"]
        # Only show cols that exist
        show_cols = [c for c in cols if c in segments.columns]
        print(segments[show_cols].to_string())
        
        # Check for missing values
        print("\nMissing Values:")
        print(segments[show_cols].isna().sum())

if __name__ == "__main__":
    inspect_fairway_54726()
