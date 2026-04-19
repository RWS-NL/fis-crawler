#!/usr/bin/env python
import pathlib
import geopandas as gpd
import pandas as pd
from shapely.geometry import box, Point

# Row limit for non-spatial tables that cannot be spatially filtered
NON_SPATIAL_ROW_LIMIT = 2000
# Number of header rows in RIS Index xlsx (metadata row + column header row)
XLSX_HEADER_ROWS = 2


def subset_test_data():
    bboxes = [
        [4.14, 51.66, 4.41, 51.72],  # Hollandsch Diep / Dordrecht area
        [5.75, 51.82, 5.90, 51.90],  # Weurt (near Nijmegen)
    ]
    bpoly = box(*bboxes[0]).union(box(*bboxes[1]))

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

        # Pass 1: spatial (.geoparquet) files — clip to bbox, collect VinCodes
        spatial_vin_codes: set = set()
        for f in sorted(input_dir.glob("*.geoparquet")):
            print(f"Subsetting {name}/{f.name}...")
            try:
                df = gpd.read_parquet(f)
            except Exception as e:
                print(f"  Error reading {f}: {e}")
                continue

            # Clip trims large polygons (e.g. vtssector) to the bbox, reducing
            # both row count and polygon complexity in the output.
            try:
                subset = df.clip(bpoly)
            except Exception:
                subset = df[df.geometry.intersects(bpoly)].copy()

            if "VinCode" in subset.columns:
                spatial_vin_codes.update(subset["VinCode"].dropna().tolist())

            print(f"  Result: {len(subset)} rows")
            subset.to_parquet(output_dir / f"{f.stem}.geoparquet")

        # Pass 2: non-spatial .parquet files (skip those superseded by .geoparquet)
        for f in sorted(input_dir.glob("*.parquet")):
            if (input_dir / f"{f.stem}.geoparquet").exists():
                print(f"Skipping {name}/{f.name}: superseded by {f.stem}.geoparquet")
                continue

            print(f"Subsetting {name}/{f.name}...")
            try:
                df = pd.read_parquet(f)
            except Exception as e:
                print(f"  Error reading {f}: {e}")
                continue

            # Filter by VinCode when spatial context is available, otherwise
            # take a head sample to avoid bloating test data.
            if "VinCode" in df.columns and spatial_vin_codes:
                subset = df[df["VinCode"].isin(spatial_vin_codes)].copy()
            else:
                subset = df.head(NON_SPATIAL_ROW_LIMIT).copy()

            print(f"  Result: {len(subset)} rows")
            subset.to_parquet(output_dir / f"{f.stem}.parquet")

        # Pass 3: xlsx files — preserve header structure, spatially filter data rows
        for f in sorted(input_dir.glob("*.xlsx")):
            print(f"Subsetting {name}/{f.name}...")
            try:
                sheets = pd.read_excel(f, sheet_name=None, header=None)
            except Exception as e:
                print(f"  Error reading {f}: {e}")
                continue

            out_path = output_dir / f.name
            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                for sheet_name, sheet_df in sheets.items():
                    header_rows = sheet_df.iloc[:XLSX_HEADER_ROWS]
                    data = sheet_df.iloc[XLSX_HEADER_ROWS:].copy()

                    # Detect lat/lon columns by their header cell value, which
                    # may contain embedded newlines (e.g. "Lat\n(WGS 84 ...)").
                    col_headers = header_rows.iloc[-1]
                    lat_cols = [
                        c for c, v in col_headers.items()
                        if isinstance(v, str) and v.startswith("Lat")
                    ]
                    lon_cols = [
                        c for c, v in col_headers.items()
                        if isinstance(v, str) and v.startswith("Lon")
                    ]

                    if lat_cols and lon_cols:
                        lat_col, lon_col = lat_cols[0], lon_cols[0]
                        lat = pd.to_numeric(data[lat_col], errors="coerce")
                        lon = pd.to_numeric(data[lon_col], errors="coerce")
                        valid = lat.notna() & lon.notna()
                        mask = valid & pd.Series(
                            [
                                bpoly.contains(Point(lo, la)) if ok else False
                                for ok, la, lo in zip(valid, lat, lon)
                            ],
                            index=data.index,
                        )
                        data = data[mask]

                    subset = pd.concat([header_rows, data], ignore_index=True)
                    print(f"  Sheet '{sheet_name}': {len(data)} data rows")
                    subset.to_excel(writer, sheet_name=sheet_name, header=False, index=False)


if __name__ == "__main__":
    subset_test_data()
