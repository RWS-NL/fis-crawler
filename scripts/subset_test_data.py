#!/usr/bin/env python
import pathlib
import geopandas as gpd
import pandas as pd
from shapely.geometry import box


def subset_test_data():
    bbox = [4.14, 51.66, 4.41, 51.72]  # Slighly expanded to ensure full coverage
    bpoly = box(*bbox)

    input_dirs = {
        "fis-export": pathlib.Path("output/fis-export"),
        "disk-export": pathlib.Path("output/disk-export"),
    }
    output_base = pathlib.Path("tests/data")
    output_base.mkdir(parents=True, exist_ok=True)

    for name, input_dir in input_dirs.items():
        if not input_dir.exists():
            print(f"Skipping {name}: {input_dir} not found")
            continue

        output_dir = output_base / name
        output_dir.mkdir(parents=True, exist_ok=True)

        for f in input_dir.glob("*"):
            if f.suffix not in [".geoparquet", ".parquet"]:
                continue

            stem = f.stem
            print(f"Subsetting {name}/{f.name}...")

            try:
                if f.suffix == ".geoparquet":
                    df = gpd.read_parquet(f)
                else:
                    df = pd.read_parquet(f)
            except Exception as e:
                print(f"  Error reading {f}: {e}")
                continue

            # Identify geometry column
            geom_col = None
            if isinstance(df, gpd.GeoDataFrame):
                geom_col = df.geometry.name
            elif "geometry" in df.columns:
                geom_col = "geometry"
            elif "Geometry" in df.columns:
                geom_col = "Geometry"

            if geom_col:
                # Filter by bbox
                if df[geom_col].dtype == "object":
                    # Might be WKT
                    from shapely import wkt

                    temp_gs = gpd.GeoSeries(
                        df[geom_col].apply(
                            lambda x: wkt.loads(x) if isinstance(x, str) else x
                        ),
                        crs="EPSG:4326",
                    )
                    mask = temp_gs.intersects(bpoly)
                else:
                    mask = df[geom_col].intersects(bpoly)
                subset = df[mask].copy()
            else:
                # For non-spatial tables like operatingtimes, keep all or filter by ID if possible
                # For now, let's just keep everything as it's small.
                subset = df

            print(f"  Result: {len(subset)} rows")
            if f.suffix == ".geoparquet":
                subset.to_parquet(output_dir / f"{stem}.geoparquet")
            else:
                subset.to_parquet(output_dir / f"{stem}.parquet")


if __name__ == "__main__":
    subset_test_data()
