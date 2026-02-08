import json
import logging
import pathlib
import sys

import click
import geopandas as gpd

from fis.ris_index import load_ris_index
from fis.core import load_data, group_complexes
from fis.graph import build_graph_features

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@click.command()
@click.option("--export-dir", default="fis-export", help="Directory containing input parquet files.")
def main(export_dir):
    data_dir = pathlib.Path(export_dir)
    try:
        locks, chambers, isrs, fairways, berths, sections = load_data(data_dir)
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

    # Group Complexes
    result = group_complexes(locks, chambers, isrs, ris_df, fairways, berths, sections)
    
    # 1. Standard JSON Output (Full Detail)
    output_json = output_dir / "lock_schematization.json"
    with open(output_json, "w") as f:
        json.dump(result, f, indent=2)
    logger.info(f"Saved JSON to {output_json}")

    # 2. Geospatial Outputs (GeoJSON / GeoParquet)
    if result:
        # Generate flattened features for visualization / graph
        features = build_graph_features(result)
        gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
        
        # Enforce integer type for IDs (nullable)
        for col in ["fairway_id", "lock_id", "chamber_id", "berth_id"]:
            if col in gdf.columns:
                gdf[col] = gdf[col].astype("Int64")
        
        # Save GeoJSON
        output_geojson = output_dir / "lock_schematization.geojson"
        gdf.to_file(output_geojson, driver="GeoJSON")
        logger.info(f"Saved GeoJSON to {output_geojson}")
        
        # Save GeoParquet
        output_geoparquet = output_dir / "lock_schematization.geoparquet"
        gdf.to_parquet(output_geoparquet)
        logger.info(f"Saved GeoParquet to {output_geoparquet}")

if __name__ == "__main__":
    main()
