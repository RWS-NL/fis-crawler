import sqlite3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString


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
        f"SELECT ID, FromNodeID, ToNodeID, Name, Length__m, Width__m, MaximumDepth__m "
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
    print("Loading BIVAS network (NL only)...")
    bivas_nodes, bivas_arcs = load_bivas_network("reference/Bivas.db")

    print("Loading FIS network (NL only)...")
    # Load FIS data
    fis_sections = gpd.read_parquet("output/fis-export/section.geoparquet")

    # Filter for Dutch network: VinCode is present, ForeignCode is absent
    fis_sections = fis_sections[fis_sections.VinCode.notna()].copy()

    # Set CRS if none, usually FIS is EPSG:4326, but let's check and reproject
    if fis_sections.crs is None:
        fis_sections.set_crs(epsg=4326, inplace=True)
    fis_sections_rd = fis_sections.to_crs(epsg=28992)

    # Compare counts
    bivas_arc_cnt = len(bivas_arcs)
    bivas_len = bivas_arcs["Length__m"].sum() / 1000.0  # km

    fis_arc_cnt = len(fis_sections_rd)
    fis_len = fis_sections_rd.geometry.length.sum() / 1000.0  # km

    # Matching Logic: Spatial join using 50m buffer
    bivas_arcs_buffered = bivas_arcs.copy()
    bivas_arcs_buffered.geometry = bivas_arcs.buffer(50)

    # Intersect
    joined = gpd.sjoin(
        fis_sections_rd, bivas_arcs_buffered, how="inner", predicate="intersects"
    )
    matched_fis_ids = joined["Id"].unique()
    matched_bivas_ids = joined["ID"].unique()

    matched_fis_pct = (len(matched_fis_ids) / fis_arc_cnt) * 100
    matched_bivas_pct = (len(matched_bivas_ids) / bivas_arc_cnt) * 100

    # Generate match/unmatch GDFs
    fis_matched = fis_sections_rd[fis_sections_rd["Id"].isin(matched_fis_ids)]
    fis_unmatched = fis_sections_rd[~fis_sections_rd["Id"].isin(matched_fis_ids)]

    bivas_matched = bivas_arcs[bivas_arcs["ID"].isin(matched_bivas_ids)]
    bivas_unmatched = bivas_arcs[~bivas_arcs["ID"].isin(matched_bivas_ids)]

    print("Exporting match results to output/bivas-validation/...")
    fis_matched.to_parquet("output/bivas-validation/fis_matched.geoparquet")
    fis_unmatched.to_parquet("output/bivas-validation/fis_unmatched.geoparquet")
    bivas_matched.to_parquet("output/bivas-validation/bivas_matched.geoparquet")
    bivas_unmatched.to_parquet("output/bivas-validation/bivas_unmatched.geoparquet")

    # Export to GeoJSON for compatibility
    fis_matched.to_file("output/bivas-validation/fis_matched.geojson", driver="GeoJSON")
    fis_unmatched.to_file(
        "output/bivas-validation/fis_unmatched.geojson", driver="GeoJSON"
    )
    bivas_matched.to_file(
        "output/bivas-validation/bivas_matched.geojson", driver="GeoJSON"
    )
    bivas_unmatched.to_file(
        "output/bivas-validation/bivas_unmatched.geojson", driver="GeoJSON"
    )

    # Calculate Total Statistics
    total_bivas_arc_cnt = 9631  # From database query
    total_bivas_len = 26446.2  # From database query
    total_fis_arc_cnt = 5107  # Total rows in parquet
    total_fis_len = 31276.3  # Total RD length

    report = f"""# FIS vs BIVAS Network Comparison

## 1. Network Schematization Approach

- **FIS Network (vaarweginformatie.nl):** The network uses detailed geographically correct geometries. Nodes are exact spatial points (`sectionjunction`), and edges (`section`) contain realistic, highly accurate LineStrings capturing the exact curves and path of the waterways. It maps directly to VIN codes.
- **BIVAS Network:** This is a macroscopic assignment model. Nodes are given with specific coordinates (RD New, EPSG:28992), but edges (`arcs`) act topologically as straight lines between the nodes. The length in BIVAS is an attribute (`Length__m`), not strictly bound to the geographic shape length.

## 2. Terminology Mapping

| FIS (vaarweginformatie) Term | BIVAS Term | Note |
| --- | --- | --- |
| `Section` (edge) | `arcs` / `segment` | BIVAS uses 'arcs' for topological link. 'segment' describes logical sets. |
| `SectionJunction` (node) | `nodes` | Nodes connect the arcs. |
| `StartJunctionId` | `FromNodeID` | Source node. |
| `EndJunctionId` | `ToNodeID` | Target node. |
| `Length` | `Length__m` | FIS length vs BIVAS logical length. |
| `VinCode` | `Code` (in segment) | Found via `arc_vin_trajectory_connection` mapping. |

## 3. Network Statistics (Total)

- **Total BIVAS Network:**
  - Arc count: {total_bivas_arc_cnt}
  - Total network length: {total_bivas_len:.1f} km (based on `Length__m` attribute)
- **Total FIS Network:**
  - Edge (Section) count: {total_fis_arc_cnt}
  - Total network geographic length: {total_fis_len:.1f} km

## 4. Granularity & Matching Analysis (Focus: Netherlands)

To ensure a meaningful comparison of Dutch waterway representations, we filter both networks to the Netherlands (BIVAS `CountryCode='NL'` and FIS entries with a valid `VinCode`).

### NL Quantitative Statistics

- **BIVAS Network (NL only):**
  - Arc count: {bivas_arc_cnt}
  - Network length: {bivas_len:.1f} km
- **FIS Network (NL only):**
  - Edge (Section) count: {fis_arc_cnt}
  - Network geographic length: {fis_len:.1f} km

### Spatial Matching (NL only)
Using a 50m spatial buffer around the straight-line topological BIVAS arcs:
- **FIS NL Edges matched with BIVAS:** {matched_fis_pct:.1f}% of FIS NL network intersects with a BIVAS arc.
- **BIVAS NL Arcs matched with FIS:** {matched_bivas_pct:.1f}% of BIVAS NL network intersects with a FIS section.

### Qualitative Differences
- BIVAS network represents a higher-level abstraction. Multiple consecutive FIS sections often map to a single topological BIVAS arc.
- The 1-to-N relationship leads to geometric mismatch, as the straight topological line of a BIVAS arc does not follow the physical curves of the waterway stored in the FIS `geometry` column.
- The FIS Dutch network is significantly more extensive (totaling ~12,838 km compared to BIVAS's ~6,931 km) as it includes many secondary and regional waterways not present in the BIVAS model.

"""

    with open("output/bivas-validation/comparison_report.md", "w") as f:
        f.write(report)

    print("Report written to output/bivas-validation/comparison_report.md")


if __name__ == "__main__":
    main()
