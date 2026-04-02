import sqlite3
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
import os
import logging
import re

logger = logging.getLogger(__name__)


def load_bivas_network(db_path, branch_set_id=337):
    """Load BIVAS network from SQLite database."""
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"BIVAS database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        # Load nodes
        nodes_df = pd.read_sql_query(
            "SELECT ID as NodeID, XCoordinate, YCoordinate FROM nodes WHERE BranchSetId = ?",
            conn,
            params=(branch_set_id,),
        )

        # Load arcs (strictly Dutch network)
        # Including Start/End Kilometer for precision matching
        arcs_df = pd.read_sql_query(
            """
            SELECT a.ID, a.FromNodeID, a.ToNodeID, a.Name, a.Length__m, a.Width__m, a.MaximumDepth__m, a.MaximumWidth__m, 
                   t.TrajectCode, t.StartKilometer, t.EndKilometer 
            FROM arcs a
            LEFT JOIN arc_vin_trajectory_connection t ON a.ID = t.ArcID
            WHERE a.BranchSetId = ? AND a.CountryCode = 'NL'
            """,
            conn,
            params=(branch_set_id,),
        )

        # Merge geometries
        merged = arcs_df.merge(nodes_df, left_on="FromNodeID", right_on="NodeID")
        merged = merged.rename(
            columns={"XCoordinate": "X_from", "YCoordinate": "Y_from"}
        )
        merged = merged.merge(nodes_df, left_on="ToNodeID", right_on="NodeID")
        merged = merged.rename(columns={"XCoordinate": "X_to", "YCoordinate": "Y_to"})

        if merged.empty:
            return gpd.GeoDataFrame(), gpd.GeoDataFrame(
                columns=arcs_df.columns, geometry=[], crs="EPSG:28992"
            )

        lines = [
            LineString(
                [Point(row["X_from"], row["Y_from"]), Point(row["X_to"], row["Y_to"])]
            )
            for _, row in merged.iterrows()
        ]
        arcs_gdf = gpd.GeoDataFrame(arcs_df, geometry=lines, crs="EPSG:28992")

        nodes_gdf = gpd.GeoDataFrame(
            nodes_df,
            geometry=[
                Point(x, y) for x, y in zip(nodes_df.XCoordinate, nodes_df.YCoordinate)
            ],
            crs="EPSG:28992",
        )
        return nodes_gdf, arcs_gdf
    finally:
        conn.close()


def normalize_code(val):
    """
    Normalize Route Code by stripping .0, leading zeros, and alphanumeric suffixes.
    Example: '001b' -> '1', '041.0' -> '41'.
    """
    if pd.isna(val) or val == "" or str(val).lower() in ["nan", "none", "<na>"]:
        return pd.NA
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    match = re.match(r"^0*(\d+)", s)
    return match.group(1) if match else s.lstrip("0")


def has_km_overlap(
    row,
    fis_begin_col="RouteKmBegin",
    fis_end_col="RouteKmEnd",
    bivas_begin_col="StartKilometer",
    bivas_end_col="EndKilometer",
    route_max_km=None,
):
    """
    Check if FIS edge km-range overlaps with BIVAS arc km-range.
    Supports inverse kilometrage if route_max_km is provided.
    """
    req = [fis_begin_col, fis_end_col, bivas_begin_col, bivas_end_col]
    if any(pd.isna(row.get(c)) for c in req):
        return False

    f_begin = min(row[fis_begin_col], row[fis_end_col])
    f_end = max(row[fis_begin_col], row[fis_end_col])
    b_begin = min(row[bivas_begin_col], row[bivas_end_col])
    b_end = max(row[bivas_begin_col], row[bivas_end_col])

    # 1. Standard overlap check
    if not (f_end < b_begin or b_end < f_begin):
        return True

    # 2. Inverse overlap check
    if route_max_km is not None:
        inv_b_begin = route_max_km - b_end
        inv_b_end = route_max_km - b_begin
        if not (f_end < inv_b_begin or inv_b_end < f_begin):
            return True

    return False


def get_consistent_length(gdf):
    """Bases length on Length__m with geometry fallback, ensuring consistency across metrics."""
    if gdf.empty:
        return 0.0
    if "Length__m" in gdf.columns:
        total = gdf["Length__m"].sum(min_count=1)
        if pd.isna(total) or total == 0:
            return gdf.geometry.length.sum()
        return total
    return gdf.geometry.length.sum()
