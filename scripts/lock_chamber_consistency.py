import pandas as pd
import geopandas as gpd
import os
import argparse
import logging
import sqlite3
from shapely.geometry import Point, LineString
from fis import utils

# Set up logger
logger = logging.getLogger("lock_consistency")


def load_bivas_locks(db_path, branch_set_id=337):
    """Load locks from BIVAS SQLite, joined with arc geometries."""
    conn = sqlite3.connect(db_path)
    try:
        # 1. Load nodes for geometry building
        nodes_df = pd.read_sql_query(
            "SELECT ID as NodeID, XCoordinate, YCoordinate FROM nodes WHERE BranchSetId = ?",
            conn,
            params=(branch_set_id,),
        )

        # 2. Load locks and join with arcs
        query = """
        SELECT 
            l.ArcID as id,
            a.Name as name,
            l.LockLength__m as bivas_length,
            l.LockWidth__m as bivas_width,
            a.FromNodeID,
            a.ToNodeID
        FROM locks l
        JOIN arcs a ON l.ArcID = a.ID AND l.BranchSetId = a.BranchSetId
        WHERE l.BranchSetId = ?
        """
        locks_df = pd.read_sql_query(query, conn, params=(branch_set_id,))

        if locks_df.empty:
            return gpd.GeoDataFrame(
                columns=["id", "name", "bivas_length", "bivas_width"],
                geometry=[],
                crs="EPSG:28992",
            )

        # 3. Create geometries
        merged = locks_df.merge(nodes_df, left_on="FromNodeID", right_on="NodeID")
        merged = merged.rename(
            columns={"XCoordinate": "X_from", "YCoordinate": "Y_from"}
        )
        merged = merged.merge(nodes_df, left_on="ToNodeID", right_on="NodeID")
        merged = merged.rename(columns={"XCoordinate": "X_to", "YCoordinate": "Y_to"})

        lines = [
            LineString(
                [Point(row["X_from"], row["Y_from"]), Point(row["X_to"], row["Y_to"])]
            )
            for _, row in merged.iterrows()
        ]

        # Build GDF in RD (since BIVAS is RD)
        gdf = gpd.GeoDataFrame(
            merged[["id", "name", "bivas_length", "bivas_width"]],
            geometry=lines,
            crs="EPSG:28992",
        )
        return gdf
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Check lock chamber consistency across FIS, EURIS, and BIVAS."
    )
    parser.add_argument(
        "--fis-chambers", default="output/fis-export/chamber.geoparquet"
    )
    parser.add_argument(
        "--euris-chambers",
        default="output/euris-export/LockChamber_NL_20260224.geojson",
    )
    parser.add_argument("--bivas-db", default="reference/Bivas.5.10.1.sqlite")
    parser.add_argument("--branch-set-id", type=int, default=337)
    parser.add_argument("--output-dir", default="output/bivas-validation")

    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    # 1. Load Datasets
    print("Loading FIS chambers...")
    fis = gpd.read_parquet(args.fis_chambers)
    # Normalize attributes using schema to get dim_* names
    schema = utils.load_schema()
    fis_norm = utils.normalize_attributes(fis, "chambers", schema)

    print("Loading EURIS chambers...")
    import glob

    euris_search_path = os.path.join(
        os.path.dirname(args.euris_chambers), "LockChamber_NL_*.geojson"
    )
    euris_files = glob.glob(euris_search_path)
    if not euris_files:
        if os.path.exists(args.euris_chambers):
            euris_files = [args.euris_chambers]
        else:
            raise FileNotFoundError(
                f"No EURIS lock chamber files found matching: {euris_search_path}"
            )

    # Pick the newest file
    euris_file = max(euris_files, key=os.path.getmtime)
    euris = gpd.read_file(euris_file)
    # EURIS fields mapping in schema
    euris_norm = utils.normalize_attributes(euris, "chambers", schema)

    print("Loading BIVAS locks...")
    bivas_rd = load_bivas_locks(args.bivas_db, args.branch_set_id)

    # 2. Re-project everything to RD (BIVAS is already RD)
    print("Reprojecting...")
    # Standardize FIS
    if fis_norm.crs is None:
        fis_norm.set_crs(epsg=4326, inplace=True)
    fis_rd = fis_norm.to_crs(epsg=28992)

    # Standardize EURIS
    if euris_norm.crs is None:
        euris_norm.set_crs(epsg=4326, inplace=True)
    euris_rd = euris_norm.to_crs(epsg=28992)

    # 3. Spatial Match: FIS to EURIS
    euris_buffered = euris_rd.copy()
    euris_buffered.geometry = euris_rd.buffer(20)  # 20m buffer for matching

    print("Spatial join FIS -> EURIS...")
    fis_euris = gpd.sjoin(fis_rd, euris_buffered, how="left", rsuffix="euris")
    print(f"Col count after FIS-EURIS: {len(fis_euris.columns)}")

    # 4. Spatial Match: (FIS+EURIS) to BIVAS
    print("Spatial join (FIS+EURIS) -> BIVAS...")
    fis_euris_buffered = fis_euris.copy()
    fis_euris_buffered.geometry = fis_euris_buffered.buffer(100)

    # Drop colliding names from BIVAS before join
    bivas_to_join = bivas_rd.rename(
        columns={"id": "bivas_id_orig", "name": "bivas_name_orig"}
    )
    matches = gpd.sjoin(fis_euris_buffered, bivas_to_join, how="left", rsuffix="bivas")

    # 5. Dimension Comparison
    # After sjoin, if columns collided (like dim_usable_length), FIS ones might be suffixed with _left
    def get_fis_col(base_name):
        if f"{base_name}_left" in matches.columns:
            return f"{base_name}_left"
        return base_name

    fis_usable_len_col = get_fis_col("dim_usable_length")
    fis_gate_wid_col = get_fis_col("dim_gate_width")

    # FIS to EURIS differences
    if "dim_usable_length_euris" in matches.columns:
        matches["diff_length_fis_euris"] = (
            matches[fis_usable_len_col] - matches["dim_usable_length_euris"]
        )
        matches["diff_width_fis_euris"] = (
            matches[fis_gate_wid_col] - matches["dim_gate_width_euris"]
        )

    # FIS to BIVAS differences
    if "bivas_length" in matches.columns:
        matches["diff_length_fis_bivas"] = (
            matches[fis_usable_len_col] - matches["bivas_length"]
        )
        matches["diff_width_fis_bivas"] = (
            matches[fis_gate_wid_col] - matches["bivas_width"]
        )

    # 6. Flag Significant Discrepancies
    # Tolerance: 2m for length, 0.5m for width
    matches["flag_length"] = False
    if "diff_length_fis_euris" in matches.columns:
        matches["flag_length"] |= matches["diff_length_fis_euris"].abs() > 2.0
    if "diff_length_fis_bivas" in matches.columns:
        matches["flag_length"] |= matches["diff_length_fis_bivas"].abs() > 2.0

    matches["flag_width"] = False
    if "diff_width_fis_euris" in matches.columns:
        matches["flag_width"] |= matches["diff_width_fis_euris"].abs() > 0.5
    if "diff_width_fis_bivas" in matches.columns:
        matches["flag_width"] |= matches["diff_width_fis_bivas"].abs() > 0.5

    # 7. Output Result
    out_path = os.path.join(args.output_dir, "lock_chamber_consistency.geoparquet")
    # Clean up before export
    id_col = get_fis_col("id")
    name_col = get_fis_col("name")

    results = matches.drop_duplicates(subset=id_col).copy()

    # Select subset of interesting columns to avoid duplicates and bloat
    export_cols = [
        id_col,
        name_col,
        fis_usable_len_col,
        fis_gate_wid_col,
        get_fis_col("dim_structural_length"),
        get_fis_col("dim_structural_width"),
        "id_euris",
        "name_euris",
        "dim_usable_length_euris",
        "dim_gate_width_euris",
        "bivas_id_orig",
        "bivas_name_orig",
        "bivas_length",
        "bivas_width",
        "diff_length_fis_euris",
        "diff_length_fis_bivas",
        "diff_width_fis_euris",
        "diff_width_fis_bivas",
        "flag_length",
        "flag_width",
        "geometry",
    ]
    # Filter for columns that actually exist
    final_cols = [c for c in export_cols if c in results.columns]
    results = results[final_cols].copy()

    # Rename for clean output
    results = results.rename(
        columns={
            id_col: "id",
            name_col: "name",
            fis_usable_len_col: "dim_usable_length",
            fis_gate_wid_col: "dim_gate_width",
            get_fis_col("dim_structural_length"): "dim_structural_length",
            get_fis_col("dim_structural_width"): "dim_structural_width",
            "bivas_id_orig": "id_bivas",
            "bivas_name_orig": "name_bivas",
        }
    )

    # Convert to 4326 for portability
    results = results.to_crs(epsg=4326)
    results.to_parquet(out_path)
    print(f"Results saved to {out_path}")

    # 8. Summary Report
    flagged = results[results["flag_length"] | results["flag_width"]]
    total_results = len(results)
    flagged_count = len(flagged)
    flagged_pct = (flagged_count / total_results) if total_results > 0 else 0.0

    report = rf"""# Lock Chamber Consistency Report

## Overall Statistics
- Total Processed Chambers: {total_results}
- Chambers with Dimension Discrepancies: {flagged_count} ({flagged_pct:.1%})

## Key Discrepancies (FIS vs BIVAS/EURIS)
*Note: Comparisons use FIS `dim_usable_length` (SchutLengte) and `dim_gate_width`.*

### Top 10 Length Discrepancies
{flagged[flagged["flag_length"]].sort_values("diff_length_fis_euris", key=lambda x: x.abs() if hasattr(x, "abs") else x, ascending=False).head(10)[["id", "name", "dim_usable_length", "dim_usable_length_euris", "bivas_length"]].to_markdown(index=False) if not flagged.empty else "No significant discrepancies found."}

### Top 10 Width Discrepancies
{flagged[flagged["flag_width"]].sort_values("diff_width_fis_euris", key=lambda x: x.abs() if hasattr(x, "abs") else x, ascending=False).head(10)[["id", "name", "dim_gate_width", "dim_gate_width_euris", "bivas_width"]].to_markdown(index=False) if not flagged.empty else "No significant discrepancies found."}
"""
    report_path = os.path.join(args.output_dir, "lock_chamber_consistency_report.md")
    with open(report_path, "w") as f:
        f.write(report)
    print(f"Report saved to {report_path}")


if __name__ == "__main__":
    main()
