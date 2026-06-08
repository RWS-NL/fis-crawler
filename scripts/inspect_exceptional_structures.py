#!/usr/bin/env python
import pathlib
import geopandas as gpd
import pandas as pd


def main():
    print("Inspecting exceptionalnavigationalstructure dataset...")

    file_path = pathlib.Path(
        "output/fis-export/exceptionalnavigationalstructure.geoparquet"
    )
    if not file_path.exists():
        file_path = pathlib.Path(
            "output/fis-export/exceptionalnavigationalstructure.parquet"
        )

    if not file_path.exists():
        print(f"Error: {file_path} not found. Has crawl been run?")
        return

    print(f"Loading data from: {file_path}")
    if file_path.suffix == ".geoparquet":
        df = gpd.read_parquet(file_path)
    else:
        df = pd.read_parquet(file_path)

    print(f"Total rows found: {len(df)}")
    print("\nColumns and Data Types:")
    for col in df.columns:
        print(f" - {col}: {df[col].dtype}")

    # Check structure types
    type_col = None
    for col in df.columns:
        if col.lower() in ["structuretype", "structure_type", "type"]:
            type_col = col
            break

    if type_col:
        print(f"\nUnique values in type column '{type_col}':")
        print(df[type_col].value_counts())
    else:
        print(
            "\nCould not find a clear structure type column. Checking unique values in columns with 'type' in their name:"
        )
        for col in df.columns:
            if "type" in col.lower():
                print(f"Unique values in '{col}': {df[col].unique()[:5]}")

    # Display first few rows
    display_cols = [
        c
        for c in ["Id", "Name", "StructureType", "Geometry", "geometry"]
        if c in df.columns
    ]
    if not display_cols:
        display_cols = list(df.columns[:5])

    print("\nFirst 10 sample structures:")
    print(df[display_cols].head(10).to_string())


if __name__ == "__main__":
    main()
