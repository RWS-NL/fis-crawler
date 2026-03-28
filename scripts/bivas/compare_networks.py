import sqlite3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
import os
import argparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)


def load_bivas_network(db_path, branch_set_id=337):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"BIVAS database not found: {db_path}")

    conn = sqlite3.connect(db_path)

    # Load nodes
    nodes_df = pd.read_sql_query(
        f"SELECT ID as NodeID, XCoordinate, YCoordinate FROM nodes WHERE BranchSetId = {branch_set_id}",
        conn,
    )
    nodes_gdf = gpd.GeoDataFrame(
        nodes_df,
        geometry=[
            Point(x, y) for x, y in zip(nodes_df.XCoordinate, nodes_df.YCoordinate)
        ],
        crs="EPSG:28992",
    )

    # Load arcs (strictly Dutch network)
    # Using MaximumWidth__m and MaximumDepth__m for vessel constraint comparison
    arcs_df = pd.read_sql_query(
        f"SELECT ID, FromNodeID, ToNodeID, Name, Length__m, Width__m, MaximumDepth__m, MaximumWidth__m "
        f"FROM arcs WHERE BranchSetId = {branch_set_id} AND CountryCode = 'NL'",
        conn,
    )

    # Merge geometries to form LineStrings
    merged = arcs_df.merge(nodes_df, left_on="FromNodeID", right_on="NodeID")
    merged = merged.rename(columns={"XCoordinate": "X_from", "YCoordinate": "Y_from"})

    merged = merged.merge(nodes_df, left_on="ToNodeID", right_on="NodeID")
    merged = merged.rename(columns={"XCoordinate": "X_to", "YCoordinate": "Y_to"})

    lines = []
    for _, row in merged.iterrows():
        p_from = Point(row["X_from"], row["Y_from"])
        p_to = Point(row["X_to"], row["Y_to"])
        lines.append(LineString([p_from, p_to]))

    arcs_gdf = gpd.GeoDataFrame(arcs_df, geometry=lines, crs="EPSG:28992")

    conn.close()
    return nodes_gdf, arcs_gdf


def main():
    parser = argparse.ArgumentParser(
        description="Compare Enriched FIS network with BIVAS model."
    )
    parser.add_argument(
        "--bivas-db",
        default="tests/data/bivas/Bivas.5.10.1.sqlite",
        help="Path to BIVAS SQLite database",
    )
    parser.add_argument(
        "--branch-set-id", type=int, default=337, help="BIVAS Branch Set ID"
    )
    parser.add_argument(
        "--fis-edges",
        default="output/fis-enriched/edges.geoparquet",
        help="Path to enriched FIS edges",
    )
    parser.add_argument(
        "--output-dir", default="output/bivas-validation", help="Output directory"
    )

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print(
        f"Loading BIVAS network from {args.bivas_db} (BranchSet: {args.branch_set_id})..."
    )
    try:
        bivas_nodes, bivas_arcs = load_bivas_network(args.bivas_db, args.branch_set_id)
    except FileNotFoundError as e:
        logger.error(e)
        return

    print(f"Loading ENRICHED FIS network from {args.fis_edges}...")
    if not os.path.exists(args.fis_edges):
        logger.error(f"FIS edges file not found: {args.fis_edges}")
        return

    fis_edges = gpd.read_parquet(args.fis_edges)

    # Filter for Dutch network: VinCode is present
    if "VinCode" in fis_edges.columns:
        fis_edges = fis_edges[fis_edges.VinCode.notna()].copy()
    else:
        logger.warning(
            "VinCode column missing from FIS edges, skipping Dutch network filter."
        )

    # Robust CRS handling
    if fis_edges.crs is None:
        logger.warning("FIS edges missing CRS, assuming EPSG:4326")
        fis_edges.set_crs(epsg=4326, inplace=True)

    if fis_edges.crs.to_epsg() != 28992:
        print("Reprojecting FIS edges to RD New (EPSG:28992)...")
        fis_edges = fis_edges.to_crs(epsg=28992)

    # Compare counts
    bivas_arc_cnt = len(bivas_arcs)
    bivas_len = bivas_arcs["Length__m"].sum() / 1000.0  # km

    fis_edge_cnt = len(fis_edges)
    fis_len = fis_edges.geometry.length.sum() / 1000.0  # km

    # Matching Logic: Spatial join using 50m buffer
    bivas_arcs_buffered = bivas_arcs.copy()
    bivas_arcs_buffered.geometry = bivas_arcs.buffer(50)

    # Intersect
    joined = gpd.sjoin(
        fis_edges, bivas_arcs_buffered, how="inner", predicate="intersects"
    )

    # Identify unique matches
    matched_fis_ids = joined["Id"].unique() if "Id" in joined.columns else []
    matched_bivas_ids = joined["ID"].unique() if "ID" in joined.columns else []

    matched_fis_pct = (
        (len(matched_fis_ids) / fis_edge_cnt * 100) if fis_edge_cnt > 0 else 0
    )
    matched_bivas_pct = (
        (len(matched_bivas_ids) / bivas_arc_cnt * 100) if bivas_arc_cnt > 0 else 0
    )

    # Attribute Comparison for joined records
    required_cols = ["dim_width", "MaximumWidth__m", "dim_depth", "MaximumDepth__m"]
    missing_cols = [col for col in required_cols if col not in joined.columns]

    if missing_cols:
        print(
            f"Warning: Skipping attribute comparison because columns are missing: {', '.join(missing_cols)}"
        )
        width_mae = depth_mae = width_bias = depth_bias = 0.0
        comp = pd.DataFrame()
    else:
        comp = joined.dropna(subset=required_cols).copy()
        if not comp.empty:
            comp["width_diff"] = comp["dim_width"] - comp["MaximumWidth__m"]
            comp["depth_diff"] = comp["dim_depth"] - comp["MaximumDepth__m"]

            width_mae = comp["width_diff"].abs().mean()
            depth_mae = comp["depth_diff"].abs().mean()

            width_bias = comp["width_diff"].mean()
            depth_bias = comp["depth_diff"].mean()
        else:
            width_mae = depth_mae = width_bias = depth_bias = 0.0

    # Generate match/unmatch GDFs
    fis_matched = (
        fis_edges[fis_edges["Id"].isin(matched_fis_ids)]
        if "Id" in fis_edges.columns
        else fis_edges
    )
    bivas_matched = (
        bivas_arcs[bivas_arcs["ID"].isin(matched_bivas_ids)]
        if "ID" in bivas_arcs.columns
        else bivas_arcs
    )

    print(f"Exporting match results to {args.output_dir}...")
    fis_matched.to_parquet(os.path.join(args.output_dir, "fis_matched.geoparquet"))
    bivas_matched.to_parquet(os.path.join(args.output_dir, "bivas_matched.geoparquet"))

    # Save a GeoParquet of attribute deltas for reporting
    if not comp.empty:
        comp_cols = [
            "Id",
            "ID",
            "Name_left",
            "Name_right",
            "dim_width",
            "MaximumWidth__m",
            "width_diff",
            "dim_depth",
            "MaximumDepth__m",
            "depth_diff",
            "geometry",
        ]
        available_comp_cols = [c for c in comp_cols if c in comp.columns]
        comp[available_comp_cols].to_parquet(
            os.path.join(args.output_dir, "attribute_comparison.geoparquet")
        )

    report = f"""# FIS (Enriched) vs BIVAS Network Comparison

## 1. Network Statistics (NL Focus)

| Metric | FIS Enriched | BIVAS (NL) |
| :--- | :---: | :---: |
| Count | {fis_edge_cnt} | {bivas_arc_cnt} |
| Total Length (km) | {fis_len:.1f} | {bivas_len:.1f} |

## 2. Spatial Matching
Using a 50m spatial buffer:
- **FIS matched with BIVAS:** {matched_fis_pct:.1f}% ({len(matched_fis_ids)} segments)
- **BIVAS matched with FIS:** {matched_bivas_pct:.1f}% ({len(matched_bivas_ids)} arcs)

## 3. Attribute Accuracy (Matched Segments)

Analysis of physical dimensions for overlapping network segments (Vessel Constraints).

| Property | Mean Absolute Error (MAE) | Mean Bias |
| :--- | :---: | :---: |
| **Width (m)** | {width_mae:.2f} m | {width_bias:.2f} m |
| **Depth (m)** | {depth_mae:.2f} m | {depth_bias:.2f} m |

*Note: Comparison performed on {len(comp)} segments where both datasets provide dimensions.*

## 4. Observations
- FIS Enrichment provides `dim_width` and `dim_depth` derived from `maximumdimensions.parquet`.
- BIVAS `MaximumWidth__m` and `MaximumDepth__m` are used for comparison as they represent vessel constraints.
- Differences often occur in complex junctions where BIVAS topological arcs cross multiple FIS segments with varying dimensions.
"""

    report_path = os.path.join(args.output_dir, "comparison_report.md")
    with open(report_path, "w") as f:
        f.write(report)

    print(f"Report written to {report_path}")


if __name__ == "__main__":
    main()
