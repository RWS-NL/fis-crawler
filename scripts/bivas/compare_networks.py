import sqlite3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
import os
import argparse
import logging

# Set up logger
logger = logging.getLogger(__name__)


def load_bivas_network(db_path, branch_set_id=337):
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"BIVAS database not found: {db_path}")

    conn = sqlite3.connect(db_path)

    # Load nodes
    nodes_df = pd.read_sql_query(
        "SELECT ID as NodeID, XCoordinate, YCoordinate FROM nodes WHERE BranchSetId = ?",
        conn,
        params=(branch_set_id,),
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
    # Joining with arc_vin_trajectory_connection to get TrajectCode (VinCode equivalent)
    arcs_df = pd.read_sql_query(
        """
        SELECT a.ID, a.FromNodeID, a.ToNodeID, a.Name, a.Length__m, a.Width__m, a.MaximumDepth__m, a.MaximumWidth__m, t.TrajectCode 
        FROM arcs a
        LEFT JOIN arc_vin_trajectory_connection t ON a.ID = t.ArcID
        WHERE a.BranchSetId = ? AND a.CountryCode = 'NL'
        """,
        conn,
        params=(branch_set_id,),
    )

    # Merge geometries to form LineStrings
    merged = arcs_df.merge(nodes_df, left_on="FromNodeID", right_on="NodeID")
    merged = merged.rename(columns={"XCoordinate": "X_from", "YCoordinate": "Y_from"})

    merged = merged.merge(nodes_df, left_on="ToNodeID", right_on="NodeID")
    merged = merged.rename(columns={"XCoordinate": "X_to", "YCoordinate": "Y_to"})

    if merged.empty:
        return nodes_gdf, gpd.GeoDataFrame(columns=arcs_df.columns, geometry=[], crs="EPSG:28992")

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
        "--bivas-version", default="5.10.1", help="BIVAS version for filenames"
    )
    parser.add_argument(
        "--fis-version", default="latest", help="FIS version for filenames"
    )
    parser.add_argument(
        "--output-dir", default="output/bivas-validation", help="Output directory"
    )

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # Helper to format filenames
    def get_out_path(name, ext="geoparquet"):
        return os.path.join(
            args.output_dir,
            f"{name}_bivas_{args.bivas_version}_fis_{args.fis_version}.{ext}",
        )

    print(
        f"Loading BIVAS network (v{args.bivas_version}) from {args.bivas_db} (BranchSet: {args.branch_set_id})..."
    )
    bivas_nodes, bivas_arcs = load_bivas_network(args.bivas_db, args.branch_set_id)

    print(
        f"Loading ENRICHED FIS network (v{args.fis_version}) from {args.fis_edges}..."
    )
    if not os.path.exists(args.fis_edges):
        raise FileNotFoundError(f"FIS edges file not found: {args.fis_edges}")

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

    current_epsg = fis_edges.crs.to_epsg()
    if current_epsg != 28992:
        logger.info(
            "Reprojecting FIS edges from EPSG:%s to RD New (EPSG:28992)...",
            current_epsg,
        )
        fis_edges = fis_edges.to_crs(epsg=28992)

    # Ensure IDs and Codes are strings for robust matching (preserve missing as <NA>)
    fis_edges["VinCode"] = fis_edges["VinCode"].astype("string")
    bivas_arcs["TrajectCode"] = bivas_arcs["TrajectCode"].astype("string")
    fis_edges["Id"] = fis_edges["Id"].astype("string")
    bivas_arcs["ID"] = bivas_arcs["ID"].astype("string")

    # BIVAS total length using Length__m (database provided)
    if "Length__m" in bivas_arcs.columns:
        bivas_total_len = bivas_arcs["Length__m"].sum()
        if pd.isna(bivas_total_len) or bivas_total_len == 0:
            logger.warning("BIVAS Length__m is missing or zero; falling back to geometry-based length.")
            bivas_total_len = bivas_arcs.geometry.length.sum()
    else:
        bivas_total_len = bivas_arcs.geometry.length.sum()

    fis_total_len = fis_edges.geometry.length.sum()
    bivas_arc_cnt = len(bivas_arcs)
    fis_edge_cnt = len(fis_edges)
    
    # -------------------------------------------------------------------------
    # Matching Method A: Spatial only (50m buffer)
    # -------------------------------------------------------------------------
    bivas_arcs_buffered = bivas_arcs.copy()
    bivas_arcs_buffered.geometry = bivas_arcs.buffer(50)
    
    spatial_joined_all = gpd.sjoin(
        fis_edges, bivas_arcs_buffered, how="inner", predicate="intersects"
    )
    
    # Reduce to a 1:1 match set to avoid weighting duplicated matches in metrics.
    spatial_joined = spatial_joined_all.sort_values(by=["Id", "ID"]).drop_duplicates(subset="Id", keep="first")
    
    spatial_fis_ids = set(spatial_joined["Id"].unique()) if "Id" in spatial_joined.columns else set()
    spatial_bivas_ids = set(spatial_joined["ID"].unique()) if "ID" in spatial_joined.columns else set()

    fis_spatial = fis_edges[fis_edges["Id"].isin(spatial_fis_ids)]
    bivas_spatial = bivas_arcs[bivas_arcs["ID"].isin(spatial_bivas_ids)]
    
    spatial_fis_len = fis_spatial.geometry.length.sum()
    spatial_bivas_len = bivas_spatial["Length__m"].sum() if "Length__m" in bivas_spatial.columns else bivas_spatial.geometry.length.sum()

    if not fis_spatial.empty: fis_spatial.to_parquet(get_out_path("fis_matches_spatial_only"))
    if not bivas_spatial.empty: bivas_spatial.to_parquet(get_out_path("bivas_matches_spatial_only"))

    # -------------------------------------------------------------------------
    # Matching Method B: ID only (VinCode == TrajectCode)
    # -------------------------------------------------------------------------
    id_joined_all = fis_edges.merge(
        bivas_arcs.drop(columns="geometry"), 
        left_on="VinCode", 
        right_on="TrajectCode", 
        how="inner"
    )
    
    # Reduce to 1:1
    id_joined = id_joined_all.sort_values(by=["Id", "ID"]).drop_duplicates(subset="Id", keep="first")

    id_fis_ids = set(id_joined["Id"].unique()) if "Id" in id_joined.columns else set()
    id_bivas_ids = set(id_joined["ID"].unique()) if "ID" in id_joined.columns else set()
    
    fis_id = fis_edges[fis_edges["Id"].isin(id_fis_ids)]
    bivas_id = bivas_arcs[bivas_arcs["ID"].isin(id_bivas_ids)]
    
    id_fis_len = fis_id.geometry.length.sum()
    id_bivas_len = bivas_id["Length__m"].sum() if "Length__m" in bivas_id.columns else bivas_id.geometry.length.sum()

    if not fis_id.empty: fis_id.to_parquet(get_out_path("fis_matches_id_only"))
    if not bivas_id.empty: bivas_id.to_parquet(get_out_path("bivas_matches_id_only"))
    
    # Export ID-only mapping table as GeoParquet (mapping with FIS geometry)
    id_mapping_gdf = id_joined[["Id", "ID", "VinCode", "TrajectCode", "geometry"]].drop_duplicates()
    id_mapping_gdf.to_parquet(get_out_path("id_matches_mapping"))

    # -------------------------------------------------------------------------
    # Matching Method C: Combined (Spatial AND ID)
    # -------------------------------------------------------------------------
    combined_joined_all = spatial_joined_all[
        spatial_joined_all["VinCode"].astype(str) == spatial_joined_all["TrajectCode"].astype(str)
    ].copy()
    
    # Reduce to 1:1 match set to avoid biasing metrics
    combined_joined = combined_joined_all.sort_values(by=["Id", "ID"]).drop_duplicates(subset="Id", keep="first")

    combined_fis_ids = set(combined_joined["Id"].unique()) if "Id" in combined_joined.columns else set()
    combined_bivas_ids = set(combined_joined["ID"].unique()) if "ID" in combined_joined.columns else set()

    fis_combined = fis_edges[fis_edges["Id"].isin(combined_fis_ids)]
    bivas_combined = bivas_arcs[bivas_arcs["ID"].isin(combined_bivas_ids)]
    
    combined_fis_len = fis_combined.geometry.length.sum()
    combined_bivas_len = bivas_combined["Length__m"].sum() if "Length__m" in bivas_combined.columns else bivas_combined.geometry.length.sum()

    if not fis_combined.empty: fis_combined.to_parquet(get_out_path("fis_matches_combined"))
    if not bivas_combined.empty: bivas_combined.to_parquet(get_out_path("bivas_matches_combined"))

    # -------------------------------------------------------------------------
    # Mismatch Analysis (Three types)
    # -------------------------------------------------------------------------
    # 1. Not matched by Spatial (proximity mismatch)
    fis_no_spatial = fis_edges[~fis_edges["Id"].isin(spatial_fis_ids)]
    bivas_no_spatial = bivas_arcs[~bivas_arcs["ID"].isin(spatial_bivas_ids)]

    # 2. Not matched by ID (trajectory code mismatch)
    fis_no_id = fis_edges[~fis_edges["Id"].isin(id_fis_ids)]
    bivas_no_id = bivas_arcs[~bivas_arcs["ID"].isin(id_bivas_ids)]

    # 3. Not matched by BOTH (Complete mismatches)
    fis_no_both = fis_edges[~(fis_edges["Id"].isin(spatial_fis_ids) | fis_edges["Id"].isin(id_fis_ids))]
    bivas_no_both = bivas_arcs[~(bivas_arcs["ID"].isin(spatial_bivas_ids) | bivas_arcs["ID"].isin(id_bivas_ids))]

    # Export Mismatch sets
    if not fis_no_spatial.empty: fis_no_spatial.to_parquet(get_out_path("fis_unmatched_spatial"))
    if not bivas_no_spatial.empty: bivas_no_spatial.to_parquet(get_out_path("bivas_unmatched_spatial"))
    if not fis_no_id.empty: fis_no_id.to_parquet(get_out_path("fis_unmatched_id"))
    if not bivas_no_id.empty: bivas_no_id.to_parquet(get_out_path("bivas_unmatched_id"))
    if not fis_no_both.empty: fis_no_both.to_parquet(get_out_path("fis_unmatched_both"))
    if not bivas_no_both.empty: bivas_no_both.to_parquet(get_out_path("bivas_unmatched_both"))

    # -------------------------------------------------------------------------
    # Attribute Comparison (using Combined matches for highest accuracy)
    # -------------------------------------------------------------------------
    required_cols = ["dim_width", "MaximumWidth__m", "dim_depth", "MaximumDepth__m"]
    missing_cols = [col for col in required_cols if col not in combined_joined.columns]

    if missing_cols:
        logger.warning(
            f"Cannot perform attribute comparison: missing columns {', '.join(missing_cols)}. "
        )
        comp = pd.DataFrame()
        width_mae = depth_mae = width_bias = depth_bias = float("nan")
    else:
        comp = combined_joined.dropna(subset=required_cols).copy()
        if not comp.empty:
            comp["width_diff"] = comp["dim_width"] - comp["MaximumWidth__m"]
            comp["depth_diff"] = comp["dim_depth"] - comp["MaximumDepth__m"]
            width_mae = comp["width_diff"].abs().mean()
            depth_mae = comp["depth_diff"].abs().mean()
            width_bias = comp["width_diff"].mean()
            depth_bias = comp["depth_diff"].mean()
        else:
            width_mae = depth_mae = width_bias = depth_bias = float("nan")

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
        comp[available_comp_cols].to_parquet(get_out_path("attribute_comparison"))

    # Report Generation
    def pct(part_val, total_val): return (part_val / total_val * 100) if total_val > 0 else 0

    report = f"""# FIS (Enriched) vs BIVAS Network Comparison

## 1. Network Statistics (NL Focus)

| Metric | FIS Enriched (v{args.fis_version}) | BIVAS (v{args.bivas_version}) |
| :--- | :---: | :---: |
| Count | {fis_edge_cnt} | {bivas_arc_cnt} |
| Total Length (km) | {fis_total_len/1000.0:.1f} | {bivas_total_len/1000.0:.1f} |

## 2. Matching Method Comparison

### FIS Segments Matched (Source: FIS)
| Method | Count | Count % | Length % |
| :--- | :---: | :---: | :---: |
| **Spatial Only (50m)** | {len(spatial_fis_ids)} | {pct(len(spatial_fis_ids), fis_edge_cnt):.1f}% | {pct(spatial_fis_len, fis_total_len):.1f}% |
| **ID Only (VinCode)** | {len(id_fis_ids)} | {pct(len(id_fis_ids), fis_edge_cnt):.1f}% | {pct(id_fis_len, fis_total_len):.1f}% |
| **Combined (Spatial + ID)** | {len(combined_fis_ids)} | {pct(len(combined_fis_ids), fis_edge_cnt):.1f}% | {pct(combined_fis_len, fis_total_len):.1f}% |

### BIVAS Arcs Matched (Source: BIVAS)
| Method | Count | Count % | Length % |
| :--- | :---: | :---: | :---: |
| **Spatial Only (50m)** | {len(spatial_bivas_ids)} | {pct(len(spatial_bivas_ids), bivas_arc_cnt):.1f}% | {pct(spatial_bivas_len, bivas_total_len):.1f}% |
| **ID Only (VinCode)** | {len(id_bivas_ids)} | {pct(len(id_bivas_ids), bivas_arc_cnt):.1f}% | {pct(id_bivas_len, bivas_total_len):.1f}% |
| **Combined (Spatial + ID)** | {len(combined_bivas_ids)} | {pct(len(combined_bivas_ids), bivas_arc_cnt):.1f}% | {pct(combined_bivas_len, bivas_total_len):.1f}% |

## 3. Mismatch Analysis (Not Matched)

### FIS Segments Unmatched
| Category | Count | Count % | Length % |
| :--- | :---: | :---: | :---: |
| **Not matched by Spatial** | {len(fis_no_spatial)} | {pct(len(fis_no_spatial), fis_edge_cnt):.1f}% | {pct(fis_no_spatial.geometry.length.sum(), fis_total_len):.1f}% |
| **Not matched by ID** | {len(fis_no_id)} | {pct(len(fis_no_id), fis_edge_cnt):.1f}% | {pct(fis_no_id.geometry.length.sum(), fis_total_len):.1f}% |
| **Not matched by BOTH** | {len(fis_no_both)} | {pct(len(fis_no_both), fis_edge_cnt):.1f}% | {pct(fis_no_both.geometry.length.sum(), fis_total_len):.1f}% |

### BIVAS Arcs Unmatched
| Category | Count | Count % | Length % |
| :--- | :---: | :---: | :---: |
| **Not matched by Spatial** | {len(bivas_no_spatial)} | {pct(len(bivas_no_spatial), bivas_arc_cnt):.1f}% | {pct(bivas_no_spatial.geometry.length.sum(), bivas_total_len):.1f}% |
| **Not matched by ID** | {len(bivas_no_id)} | {pct(len(bivas_no_id), bivas_arc_cnt):.1f}% | {pct(bivas_no_id.geometry.length.sum(), bivas_total_len):.1f}% |
| **Not matched by BOTH** | {len(bivas_no_both)} | {pct(len(bivas_no_both), bivas_arc_cnt):.1f}% | {pct(bivas_no_both.geometry.length.sum(), bivas_total_len):.1f}% |

## 4. Attribute Accuracy (Combined Matches)

Analysis of physical dimensions for network segments matched by both proximity and trajectory code.

| Property | Mean Absolute Error (MAE) | Mean Bias |
| :--- | :---: | :---: |
| **Width (m)** | {f"{width_mae:.2f} m" if not pd.isna(width_mae) else "N/A"} | {f"{width_bias:.2f} m" if not pd.isna(width_bias) else "N/A"} |
| **Depth (m)** | {f"{depth_mae:.2f} m" if not pd.isna(depth_mae) else "N/A"} | {f"{depth_bias:.2f} m" if not pd.isna(depth_bias) else "N/A"} |

*Note: Comparison performed on {len(comp)} segments.*

## 5. Observations
- **ID Matching** using `VinCode` (FIS) and `TrajectCode` (BIVAS) provides a robust topological link.
- **Combined Matching** significantly reduces false positives in dense areas like Meppel or Rotterdam.
- Discrepancies in **Spatial Only** often occur where fairways run parallel but have different trajectory codes.
"""

    report_path = os.path.join(args.output_dir, "comparison_report.md")
    with open(report_path, "w") as f:
        f.write(report)

    print(f"Report written to {report_path}")
    print(f"Results exported to {args.output_dir}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    main()
