import sqlite3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
import os


def load_bivas_network(db_path, branch_set_id=337):
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
    os.makedirs("output/bivas-validation", exist_ok=True)

    print("Loading BIVAS network (NL only)...")
    bivas_nodes, bivas_arcs = load_bivas_network("tests/data/bivas/Bivas.5.10.1.sqlite")

    print("Loading ENRICHED FIS network (NL only)...")
    # Load enriched FIS data
    fis_edges = gpd.read_parquet("output/fis-enriched/edges.geoparquet")

    # Filter for Dutch network: VinCode is present
    fis_edges = fis_edges[fis_edges.VinCode.notna()].copy()

    # Reproject to RD New
    if fis_edges.crs != "EPSG:28992":
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
    matched_fis_ids = joined["Id"].unique()
    matched_bivas_ids = joined["ID"].unique()

    matched_fis_pct = (len(matched_fis_ids) / fis_edge_cnt) * 100
    matched_bivas_pct = (len(matched_bivas_ids) / bivas_arc_cnt) * 100

    # Attribute Comparison for joined records
    # Columns: dim_width (FIS) vs MaximumWidth__m (BIVAS), dim_depth (FIS) vs MaximumDepth__m (BIVAS)
    comp = joined.dropna(
        subset=["dim_width", "MaximumWidth__m", "dim_depth", "MaximumDepth__m"]
    ).copy()

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
    fis_matched = fis_edges[fis_edges["Id"].isin(matched_fis_ids)]

    bivas_matched = bivas_arcs[bivas_arcs["ID"].isin(matched_bivas_ids)]

    print("Exporting match results to output/bivas-validation/...")
    fis_matched.to_parquet("output/bivas-validation/fis_matched.geoparquet")
    bivas_matched.to_parquet("output/bivas-validation/bivas_matched.geoparquet")

    # Save a GeoParquet of attribute deltas for reporting
    if not comp.empty:
        comp[
            [
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
        ].to_parquet("output/bivas-validation/attribute_comparison.geoparquet")

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
- FIS Enrichment now provides `dim_width` and `dim_depth` derived from `maximumdimensions.parquet`.
- BIVAS `MaximumWidth__m` and `MaximumDepth__m` are used for comparison as they represent vessel constraints.
- Differences often occur in complex junctions where BIVAS topological arcs cross multiple FIS segments with varying dimensions.
"""

    with open("output/bivas-validation/comparison_report.md", "w") as f:
        f.write(report)

    print("Report written to output/bivas-validation/comparison_report.md")


if __name__ == "__main__":
    main()
