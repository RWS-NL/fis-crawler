import os
import pathlib
import shutil

import geopandas as gpd
import pandas as pd
from shapely import wkt

target_dir = pathlib.Path("tests/data/subset")
os.makedirs(target_dir, exist_ok=True)
source_dir = pathlib.Path("output/fis-export")

# 1. Target Lock IDs
lock_ids = [42863, 49032, 50750, 59464015]

# 2. Extract Locks
locks = pd.read_parquet(source_dir / "lock.parquet")
subset_locks = locks[locks["Id"].isin(lock_ids)]
subset_locks.to_parquet(target_dir / "lock.parquet")

# 3. Extract Chambers & Subchambers
chambers = pd.read_parquet(source_dir / "chamber.parquet")
subset_chambers = chambers[chambers["ParentId"].isin(lock_ids)]
subset_chambers.to_parquet(target_dir / "chamber.parquet")

subchambers = pd.read_parquet(source_dir / "subchamber.parquet")
subset_subchambers = subchambers[subchambers["ParentId"].isin(subset_chambers["Id"])]
subset_subchambers.to_parquet(target_dir / "subchamber.parquet")

# 4. Extract Sections & Fairways
fairway_ids = subset_locks["FairwayId"].unique().tolist()
fairway_ids.extend([59275756, 28354, 38542494, 30688892, 12821])

fairways = pd.read_parquet(source_dir / "fairway.parquet")
subset_fairways = fairways[fairways["Id"].isin(fairway_ids)]
subset_fairways.to_parquet(target_dir / "fairway.parquet")

sections = gpd.read_parquet(source_dir / "section.geoparquet")
subset_sections = sections[sections["FairwayId"].isin(fairway_ids)]
subset_sections.to_parquet(target_dir / "section.geoparquet")

# 5. Extract Bridges & Openings - Spatial lookup
bridges_gdf = gpd.read_parquet(source_dir / "bridge.geoparquet")
lock_geoms = [wkt.loads(g) for g in subset_locks["Geometry"]]
lock_union = gpd.GeoSeries(lock_geoms).union_all().buffer(0.01)  # ~1km buffer

subset_bridges = bridges_gdf[bridges_gdf.geometry.intersects(lock_union)]
subset_bridges.to_parquet(target_dir / "bridge.parquet")

openings = pd.read_parquet(source_dir / "opening.parquet")
subset_openings = openings[openings["ParentId"].isin(subset_bridges["Id"])]
subset_openings.to_parquet(target_dir / "opening.parquet")

# 6. Metadata
pd.DataFrame(columns=["Id", "ParentId"]).to_parquet(target_dir / "berth.parquet")
isrs = pd.read_parquet(source_dir / "isrs.parquet")
isrs.to_parquet(target_dir / "isrs.parquet")
ot = pd.read_parquet(source_dir / "operatingtimes.parquet")
ot.to_parquet(target_dir / "operatingtimes.parquet")
terminals = pd.read_parquet(source_dir / "terminal.parquet")
terminals.to_parquet(target_dir / "terminal.parquet")

shutil.copy(source_dir / "RisIndexNL.xlsx", target_dir / "RisIndexNL.xlsx")

print(f"Subset created in {target_dir}")
print(f"Bridges: {len(subset_bridges)}, Openings: {len(subset_openings)}")
