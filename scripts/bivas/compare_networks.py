import pandas as pd
import geopandas as gpd
import os
import argparse
import logging
from fis.graph.bivas import (
    load_bivas_network,
    normalize_code,
    has_km_overlap,
    get_consistent_length,
)

# Set up logger
logger = logging.getLogger(__name__)


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

    def get_out_path(name, ext="geoparquet"):
        return os.path.join(
            args.output_dir,
            f"{name}_bivas_{args.bivas_version}_fis_{args.fis_version}.{ext}",
        )

    print(f"Loading BIVAS network (v{args.bivas_version}) from {args.bivas_db}...")
    bivas_nodes, bivas_arcs = load_bivas_network(args.bivas_db, args.branch_set_id)

    print(f"Loading ENRICHED FIS network from {args.fis_edges}...")
    fis_edges = gpd.read_parquet(args.fis_edges)

    # Reproject FIS edges
    if fis_edges.crs is None:
        fis_edges.set_crs(epsg=4326, inplace=True)
    if fis_edges.crs.to_epsg() != 28992:
        fis_edges = fis_edges.to_crs(epsg=28992)

    # Type safety
    fis_edges["Id"] = fis_edges["Id"].astype("string")
    bivas_arcs["ID"] = bivas_arcs["ID"].astype("string")

    # Normalize Codes for robust matching
    fis_match_col = "route_code" if "route_code" in fis_edges.columns else "vincode"
    fis_edges["match_code_norm"] = (
        fis_edges[fis_match_col].apply(normalize_code).astype("string")
    )
    bivas_arcs["TrajectCode_norm"] = (
        bivas_arcs["TrajectCode"].apply(normalize_code).astype("string")
    )

    # Calculate Route Extents
    fis_max_km = (
        fis_edges.groupby("match_code_norm")[["RouteKmBegin", "RouteKmEnd"]]
        .max()
        .max(axis=1)
    )
    bivas_max_km = (
        bivas_arcs.groupby("TrajectCode_norm")[["StartKilometer", "EndKilometer"]]
        .max()
        .max(axis=1)
    )
    route_max_km = pd.concat([fis_max_km, bivas_max_km], axis=1).max(axis=1).to_dict()

    # Metrics prep
    fis_total_len = fis_edges.geometry.length.sum()
    bivas_total_len = get_consistent_length(bivas_arcs)
    fis_edge_cnt, bivas_arc_cnt = len(fis_edges), len(bivas_arcs)

    # Helper for overlap with inversion support
    def check_overlap(row):
        code = row.get("match_code_norm") or row.get("TrajectCode_norm")
        max_km = route_max_km.get(code)
        return has_km_overlap(row, route_max_km=max_km)

    # -------------------------------------------------------------------------
    # Matching Method A: Spatial Only
    # -------------------------------------------------------------------------
    bivas_arcs_buffered = bivas_arcs.copy()
    bivas_arcs_buffered.geometry = bivas_arcs.buffer(50)
    spatial_joined_all = gpd.sjoin(
        fis_edges, bivas_arcs_buffered, how="inner", predicate="intersects"
    )

    spatial_fis_ids = (
        set(spatial_joined_all["Id"].unique())
        if not spatial_joined_all.empty
        else set()
    )
    spatial_bivas_ids = (
        set(spatial_joined_all["ID"].unique())
        if not spatial_joined_all.empty
        else set()
    )

    # -------------------------------------------------------------------------
    # Matching Method B: ID + KM (Logical Precision)
    # -------------------------------------------------------------------------
    id_merged = fis_edges.merge(
        bivas_arcs.drop(columns="geometry"),
        left_on="match_code_norm",
        right_on="TrajectCode_norm",
        how="inner",
    )
    id_merged = id_merged[id_merged["match_code_norm"].notna()]

    id_merged["is_f"] = id_merged.apply(
        lambda r: has_km_overlap(r, route_max_km=None), axis=1
    )
    id_merged["is_b"] = id_merged.apply(
        check_overlap, axis=1
    )  # check_overlap includes inversion

    # Track Inverted Routes
    route_stats = id_merged.groupby("match_code_norm").agg(
        f=("is_f", "sum"), b=("is_b", "sum")
    )
    inverted_routes = route_stats[route_stats["b"] > route_stats["f"]].index.tolist()

    id_joined_all = id_merged[id_merged["is_f"] | id_merged["is_b"]].copy()
    id_fis_ids = set(id_joined_all["Id"].unique()) if not id_joined_all.empty else set()
    id_bivas_ids = (
        set(id_joined_all["ID"].unique()) if not id_joined_all.empty else set()
    )

    # -------------------------------------------------------------------------
    # Matching Method C: Combined
    # -------------------------------------------------------------------------
    combined_joined_all = spatial_joined_all[
        (spatial_joined_all["match_code_norm"].notna())
        & (
            spatial_joined_all["match_code_norm"]
            == spatial_joined_all["TrajectCode_norm"]
        )
    ].copy()
    combined_joined_all = combined_joined_all[
        combined_joined_all.apply(check_overlap, axis=1)
    ]

    combined_fis_ids = (
        set(combined_joined_all["Id"].unique())
        if not combined_joined_all.empty
        else set()
    )
    combined_bivas_ids = (
        set(combined_joined_all["ID"].unique())
        if not combined_joined_all.empty
        else set()
    )

    # -------------------------------------------------------------------------
    # Attribute Comparison (Deduplicated for 1:1 accuracy stats)
    # -------------------------------------------------------------------------
    comp_joined = (
        combined_joined_all.sort_values(by=["Id", "ID"])
        .drop_duplicates(subset="Id", keep="first")
        .copy()
    )
    required_cols = ["dim_width", "MaximumWidth__m", "dim_depth", "MaximumDepth__m"]
    missing_cols = [col for col in required_cols if col not in comp_joined.columns]

    width_mae = depth_mae = width_bias = depth_bias = float("nan")
    if not missing_cols:
        comp = comp_joined.dropna(subset=required_cols).copy()
        if not comp.empty:
            comp["width_diff"] = comp["dim_width"] - comp["MaximumWidth__m"]
            comp["depth_diff"] = comp["dim_depth"] - comp["MaximumDepth__m"]
            width_mae, depth_mae = (
                comp["width_diff"].abs().mean(),
                comp["depth_diff"].abs().mean(),
            )
            width_bias, depth_bias = (
                comp["width_diff"].mean(),
                comp["depth_diff"].mean(),
            )
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
            available = [c for c in comp_cols if c in comp.columns]
            comp[available].to_parquet(get_out_path("attribute_comparison"))

    # Exports
    def export_gdf(gdf, name):
        if not gdf.empty:
            gdf.to_parquet(get_out_path(name))

    export_gdf(
        fis_edges[fis_edges["Id"].isin(spatial_fis_ids)], "fis_matches_spatial_only"
    )
    export_gdf(
        bivas_arcs[bivas_arcs["ID"].isin(spatial_bivas_ids)],
        "bivas_matches_spatial_only",
    )
    export_gdf(fis_edges[fis_edges["Id"].isin(id_fis_ids)], "fis_matches_id_km")
    export_gdf(bivas_arcs[bivas_arcs["ID"].isin(id_bivas_ids)], "bivas_matches_id_km")
    export_gdf(
        fis_edges[fis_edges["Id"].isin(combined_fis_ids)], "fis_matches_combined"
    )
    export_gdf(
        bivas_arcs[bivas_arcs["ID"].isin(combined_bivas_ids)], "bivas_matches_combined"
    )

    # Unmatched sets
    fis_no_spatial = fis_edges[~fis_edges["Id"].isin(spatial_fis_ids)]
    fis_no_id = fis_edges[~fis_edges["Id"].isin(id_fis_ids)]
    bivas_no_spatial = bivas_arcs[~bivas_arcs["ID"].isin(spatial_bivas_ids)]
    bivas_no_id = bivas_arcs[~bivas_arcs["ID"].isin(id_bivas_ids)]

    export_gdf(fis_no_spatial, "fis_unmatched_spatial")
    export_gdf(fis_no_id, "fis_unmatched_id")
    export_gdf(bivas_no_spatial, "bivas_unmatched_spatial")
    export_gdf(bivas_no_id, "bivas_unmatched_id")

    fis_no_both = fis_edges[
        ~(fis_edges["Id"].isin(spatial_fis_ids) | fis_edges["Id"].isin(id_fis_ids))
    ]
    bivas_no_both = bivas_arcs[
        ~(
            bivas_arcs["ID"].isin(spatial_bivas_ids)
            | bivas_arcs["ID"].isin(id_bivas_ids)
        )
    ]
    export_gdf(fis_no_both, "fis_unmatched_both")
    export_gdf(bivas_no_both, "bivas_unmatched_both")

    # Report Generation
    def pct(part_val, total_val):
        return (part_val / total_val * 100) if total_val > 0 else 0

    report = f"""# FIS (Enriched) vs BIVAS Network Comparison

## 1. Network Statistics (NL Focus)
| Metric | FIS Enriched | BIVAS |
| :--- | :---: | :---: |
| Count | {fis_edge_cnt} | {bivas_arc_cnt} |
| Total Length (km) | {fis_total_len / 1000.0:.1f} | {bivas_total_len / 1000.0:.1f} |

## 2. Inverted Kilometrage Analysis
Identified **{len(inverted_routes)}** routes where BIVAS and FIS use opposite kilometer directions.

**Routes with Inverted KM (BIVAS vs FIS):**
{", ".join(sorted(inverted_routes)) if inverted_routes else "None identified."}

### Data Mapping Reference
| BIVAS Property | FIS Property | Description |
| :--- | :--- | :--- |
| **TrajectCode** | `{fis_match_col}` | Base route code (normalized). |
| **Start/EndKilometer** | `RouteKmBegin/End` | Geographic position along trajectory. |

## 3. Matching Method Comparison (FIS Perspective)
| Method | Count | Count % | Length % |
| :--- | :---: | :---: | :---: |
| **Spatial Only** | {len(spatial_fis_ids)} | {pct(len(spatial_fis_ids), fis_edge_cnt):.1f}% | {pct(fis_edges[fis_edges["Id"].isin(spatial_fis_ids)].geometry.length.sum(), fis_total_len):.1f}% |
| **ID + KM Overlap** | {len(id_fis_ids)} | {pct(len(id_fis_ids), fis_edge_cnt):.1f}% | {pct(fis_edges[fis_edges["Id"].isin(id_fis_ids)].geometry.length.sum(), fis_total_len):.1f}% |
| **Combined** | {len(combined_fis_ids)} | {pct(len(combined_fis_ids), fis_edge_cnt):.1f}% | {pct(fis_edges[fis_edges["Id"].isin(combined_fis_ids)].geometry.length.sum(), fis_total_len):.1f}% |

## 4. Mismatch Analysis (Unmatched Segments)

### FIS Segments Unmatched
| Category | Count | Count % | Length % |
| :--- | :---: | :---: | :---: |
| **Not matched by Spatial** | {len(fis_no_spatial)} | {pct(len(fis_no_spatial), fis_edge_cnt):.1f}% | {pct(fis_no_spatial.geometry.length.sum(), fis_total_len):.1f}% |
| **Not matched by ID+KM** | {len(fis_no_id)} | {pct(len(fis_no_id), fis_edge_cnt):.1f}% | {pct(fis_no_id.geometry.length.sum(), fis_total_len):.1f}% |
| **Not matched by BOTH** | {len(fis_no_both)} | {pct(len(fis_no_both), fis_edge_cnt):.1f}% | {pct(fis_no_both.geometry.length.sum(), fis_total_len):.1f}% |

### BIVAS Arcs Unmatched
| Category | Count | Count % | Length % |
| :--- | :---: | :---: | :---: |
| **Not matched by Spatial** | {len(bivas_no_spatial)} | {pct(len(bivas_no_spatial), bivas_arc_cnt):.1f}% | {pct(get_consistent_length(bivas_no_spatial), bivas_total_len):.1f}% |
| **Not matched by ID+KM** | {len(bivas_no_id)} | {pct(len(bivas_no_id), bivas_arc_cnt):.1f}% | {pct(get_consistent_length(bivas_no_id), bivas_total_len):.1f}% |
| **Not matched by BOTH** | {len(bivas_no_both)} | {pct(len(bivas_no_both), bivas_arc_cnt):.1f}% | {pct(get_consistent_length(bivas_no_both), bivas_total_len):.1f}% |

## 5. Attribute Accuracy (Combined Matches)
| Property | MAE | Bias |
| :--- | :---: | :---: |
| **Width (m)** | {f"{width_mae:.2f} m" if not pd.isna(width_mae) else "N/A"} | {f"{width_bias:.2f} m" if not pd.isna(width_bias) else "N/A"} |
| **Depth (m)** | {f"{depth_mae:.2f} m" if not pd.isna(depth_mae) else "N/A"} | {f"{depth_bias:.2f} m" if not pd.isna(depth_bias) else "N/A"} |

## 6. Observations
- **Logical Mapping**: One-to-many matches are correctly handled by using independent ID sets for each perspective.
- **Metric Consistency**: BIVAS length metrics consistently use `Length__m` with geometric fallback.
- **Resource Management**: SQLite connections are now properly closed.
"""
    with open(os.path.join(args.output_dir, "comparison_report.md"), "w") as f:
        f.write(report)
    print(f"Report written to {os.path.join(args.output_dir, 'comparison_report.md')}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    main()
