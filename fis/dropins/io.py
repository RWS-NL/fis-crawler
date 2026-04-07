import logging
import pathlib
import pickle
from typing import List, Dict, Tuple

import pandas as pd
import geopandas as gpd

from fis.lock.core import load_data as lock_load_data, group_complexes as group_locks
from fis.bridge.core import group_bridge_complexes as group_bridges
from fis import utils
from fis.dropins.terminals import build_terminals_gdf
from fis.dropins.berths import build_berths_gdf

logger = logging.getLogger(__name__)


def load_dropins_with_spatial_matching(
    export_dir: pathlib.Path, disk_dir: pathlib.Path, bbox=None
) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict], pd.DataFrame, pd.DataFrame]:
    """
    Loads structure and network data for FIS/DISK, using spatial matching
    (within buffers) to link physical structures to fairway sections.

    This loading strategy is required for FIS as it does not provide explicit
    section-to-structure foreign keys in its raw exports.

    Mapping Strategy:
    1. Units: Dimensions are in meters.
    2. Grouping: Locks and bridges are grouped via specialized spatial core modules.
    3. Fairway Linking: Spatial intersection between DISK geometries and FIS records.
    """
    data = lock_load_data(export_dir, disk_dir)

    def read_geo_or_parquet(stem):
        gpq = export_dir / f"{stem}.geoparquet"
        pq = export_dir / f"{stem}.parquet"
        if not gpq.exists() and not pq.exists():
            raise FileNotFoundError(
                f"Missing essential data: neither {gpq} nor {pq} exist."
            )
        if gpq.exists():
            df = gpd.read_parquet(gpq)
        else:
            df = pd.read_parquet(pq)

        # Normalize geometry column: handle uppercase "Geometry" WKT and ensure GeoDataFrame.
        if "geometry" not in df.columns and "Geometry" in df.columns:
            if isinstance(df["Geometry"].iloc[0], str):
                df["geometry"] = gpd.GeoSeries.from_wkt(df["Geometry"])
            else:
                df["geometry"] = df["Geometry"]
        elif "geometry" in df.columns:
            if isinstance(df["geometry"].iloc[0], str):
                df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])

        # Ensure we return a GeoDataFrame when a geometry column is present.
        if "geometry" in df.columns and not isinstance(df, gpd.GeoDataFrame):
            df = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

        return df

    data["terminals"] = read_geo_or_parquet("terminal")
    data["berths"] = read_geo_or_parquet("berth")

    if bbox:
        import shapely.geometry

        bbox_poly = shapely.geometry.box(*bbox)

        def filter_df(df, name):
            if df.empty or "geometry" not in df.columns:
                return df
            if df["geometry"].dtype == "object":
                is_str = df["geometry"].apply(lambda x: isinstance(x, str))
                if is_str.any():
                    df = df.copy()
                    df.loc[is_str, "geometry"] = gpd.GeoSeries.from_wkt(
                        df.loc[is_str, "geometry"]
                    )
            mask = gpd.GeoSeries(df["geometry"], crs="EPSG:4326").intersects(bbox_poly)
            return df[mask].copy()

        data["locks"] = filter_df(data["locks"], "locks")
        data["bridges"] = filter_df(data["bridges"], "bridges")
        data["sections"] = filter_df(data["sections"], "sections")
        data["terminals"] = filter_df(data["terminals"], "terminals")
        data["berths"] = filter_df(data["berths"], "berths")

    logger.info("Grouping Locks...")
    lock_complexes = group_locks(data)

    logger.info("Grouping Bridges...")
    bridge_complexes = group_bridges(data)

    logger.info("Preparing Terminals...")
    terminals_list = []
    for _, row in data["terminals"].iterrows():
        term_dict = row.to_dict()
        if "Id" in term_dict:
            term_dict["id"] = term_dict.pop("Id")
        if "geometry" in term_dict and hasattr(term_dict["geometry"], "wkt"):
            term_dict["geometry"] = term_dict["geometry"].wkt
        terminals_list.append(term_dict)

    logger.info("Preparing Berths...")
    berths_list = []
    for _, row in data["berths"].iterrows():
        berth_dict = row.to_dict()
        if "Id" in berth_dict:
            berth_dict["id"] = berth_dict.pop("Id")
        if "geometry" in berth_dict and hasattr(berth_dict["geometry"], "wkt"):
            berth_dict["geometry"] = berth_dict["geometry"].wkt
        berths_list.append(berth_dict)

    return (
        lock_complexes,
        bridge_complexes,
        terminals_list,
        berths_list,
        data["sections"],
        data["openings"],
    )


def export_graph(
    all_features: List[Dict],
    lock_complexes: List[Dict],
    bridge_complexes: List[Dict],
    terminals: List[Dict],
    berths: List[Dict],
    output_dir: pathlib.Path,
):
    """Exports the generated graph features to GeoJSON/GeoParquet."""
    import networkx as nx

    logger.info("Exporting drop-ins network graph and components...")
    nodes_rows, edges_rows = _separate_features(all_features)

    if not nodes_rows or not edges_rows:
        raise ValueError("Cannot export graph: Nodes or Edges list is empty.")

    nodes_gdf = gpd.GeoDataFrame(nodes_rows, geometry="geometry", crs="EPSG:4326")
    edges_gdf = gpd.GeoDataFrame(edges_rows, geometry="geometry", crs="EPSG:4326")

    output_dir.mkdir(parents=True, exist_ok=True)

    G = nx.MultiGraph()
    _populate_graph(G, nodes_gdf, edges_gdf)

    with open(output_dir / "graph.pickle", "wb") as f:
        pickle.dump(G, f)

    logger.info(
        "Generated graph with %d nodes and %d edges",
        G.number_of_nodes(),
        G.number_of_edges(),
    )
    _export_dataframes(
        lock_complexes,
        bridge_complexes,
        terminals,
        berths,
        nodes_gdf,
        edges_gdf,
        output_dir,
    )


def _separate_features(all_features: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    from shapely.geometry import shape

    nodes_rows, edges_rows = [], []
    seen_nodes = set()
    for f in all_features:
        props = f["properties"]
        geom = shape(f["geometry"]) if f["geometry"] else None
        if not geom:
            continue
        ftype = props.get("feature_type")
        if ftype == "node":
            if props["id"] not in seen_nodes:
                seen_nodes.add(props["id"])
                nodes_rows.append(props | {"geometry": geom})
        elif ftype == "fairway_segment":
            edges_rows.append(props | {"geometry": geom})
    return nodes_rows, edges_rows


def _populate_graph(G, nodes_gdf: gpd.GeoDataFrame, edges_gdf: gpd.GeoDataFrame):
    for _, row in nodes_gdf.iterrows():
        node_attr = {k: v for k, v in row.items() if k != "geometry"}
        node_attr["geometry_wkt"] = row.geometry.wkt
        G.add_node(row["id"], **node_attr)
    for _, row in edges_gdf.iterrows():
        if pd.isna(row.get("source_node")) or pd.isna(row.get("target_node")):
            continue
        edge_attr = {
            k: v
            for k, v in row.items()
            if k not in ["source_node", "target_node", "geometry"]
        }
        edge_attr["geometry_wkt"] = row.geometry.wkt
        G.add_edge(row["source_node"], row["target_node"], **edge_attr)


def _export_dataframes(
    lock_complexes,
    bridge_complexes,
    terminals,
    berths,
    nodes_gdf,
    edges_gdf,
    output_dir,
):
    from fis.lock.graph import (
        build_locks_gdf,
        build_chambers_gdf,
        build_subchambers_gdf,
        build_berths_gdf as build_lock_berths_gdf,
    )
    from fis.bridge.graph import build_bridges_gdf, build_openings_gdf

    gdfs = {
        "nodes": nodes_gdf,
        "edges": edges_gdf,
        "locks": build_locks_gdf(lock_complexes),
        "chambers": build_chambers_gdf(lock_complexes),
        "subchambers": build_subchambers_gdf(lock_complexes),
        "lock_berths": build_lock_berths_gdf(lock_complexes),
        "bridges": build_bridges_gdf(bridge_complexes),
        "openings": build_openings_gdf(bridge_complexes),
        "terminals": build_terminals_gdf(terminals),
        "berths": build_berths_gdf(berths),
    }

    schema = utils.load_schema()
    id_cols = schema.get("identifiers", {}).get("columns", [])

    for name, gdf in gdfs.items():
        if gdf is not None and not gdf.empty:
            gdf = gdf.copy()
            for col in id_cols:
                if col in gdf.columns:
                    gdf[col] = gdf[col].apply(utils.stringify_id)

            gdf.to_parquet(output_dir / f"{name}.geoparquet")
            gdf.to_file(output_dir / f"{name}.geojson", driver="GeoJSON")
            logger.info("Exported %s with %d rows", name, len(gdf))
