import pandas as pd
from shapely import wkt
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).parent.parent))

from fis.lock.utils import find_chamber_doors
from fis.lock.core import process_fairway_geometry
from shapely.geometry import Point


def inspect_lock_6951():
    print("Inspecting Lock 6951...")

    # Load raw data to get geometry inputs
    output_dir = "output/fis-export"
    try:
        locks_df = pd.read_parquet(f"{output_dir}/lock.parquet")
        chambers_df = pd.read_parquet(f"{output_dir}/chamber.parquet")
        fairways_df = pd.read_parquet(f"{output_dir}/fairway.parquet")
    except Exception as e:
        print(f"Error loading parquet files: {e}")
        return

    lock_row = locks_df[locks_df["Id"] == 6951]
    if lock_row.empty:
        print("Lock 6951 not found.")
        return
    lock_row = lock_row.iloc[0]

    # Find chamber for this lock
    chamber_rows = chambers_df[chambers_df["ParentId"] == 6951]

    print(f"Lock Name: {lock_row['Name']}")
    print(f"Chambers found: {len(chamber_rows)}")

    # We need to simulate the split/merge point calculation
    fw_row = fairways_df[fairways_df["Id"] == lock_row["FairwayId"]]
    if fw_row.empty:
        print(f"Fairway {lock_row['FairwayId']} not found.")
        return
    fw_row = fw_row.iloc[0]

    class RowObj:
        def __init__(self, data):
            self.__dict__.update(data)
            if data["Geometry"]:
                self.geometry = wkt.loads(data["Geometry"])
            else:
                self.geometry = None

        def get(self, key, default=None):
            return self.__dict__.get(key, default)

        def __getitem__(self, key):
            return self.__dict__[key]

    l_obj = RowObj(lock_row)
    f_obj = RowObj(fw_row)

    # Mock max length for buffer
    max_len = (
        chamber_rows["Length"].max()
        if "Length" in chamber_rows and not chamber_rows["Length"].isnull().all()
        else 0
    )
    buffer_dist = (max_len / 2) + 50
    print(f"Max Length: {max_len}, Buffer: {buffer_dist}")

    data = process_fairway_geometry(f_obj, l_obj, buffer_dist=buffer_dist)

    if "geometry_before_wkt" not in data or "geometry_after_wkt" not in data:
        print("Could not calculate split/merge points.")
        return

    g_before = wkt.loads(data["geometry_before_wkt"])
    g_after = wkt.loads(data["geometry_after_wkt"])
    split_point = Point(g_before.coords[-1])
    merge_point = Point(g_after.coords[0])

    print(f"Split Point: {split_point.wkt}")
    print(f"Merge Point: {merge_point.wkt}")

    for _, chamber in chamber_rows.iterrows():
        print(f"\nChamber {chamber['Id']} ({chamber['Name']})")
        c_geom_wkt = chamber["Geometry"]
        if not c_geom_wkt:
            continue
        c_geom = wkt.loads(c_geom_wkt)
        print(f"Chamber Geometry Type: {c_geom.geom_type}")

        # Test current logic
        start, end = find_chamber_doors(c_geom, split_point, merge_point)

        print(f"Current Start Door: {start.wkt}")
        print(f"Current End Door: {end.wkt}")


if __name__ == "__main__":
    inspect_lock_6951()
