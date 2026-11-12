"""EURIS graph building logic."""

import logging
import pathlib
import pickle
import re

import geopandas as gpd
import networkx as nx
import pandas as pd
import pyproj
import shapely
from shapely.ops import substring
import json
from tqdm.auto import tqdm

logger = logging.getLogger(__name__)


def _latest_paths_by_country(
    paths: list[pathlib.Path], prefix: str
) -> list[pathlib.Path]:
    """Keep only the newest dated GeoJSON snapshot per country code."""
    pattern = re.compile(
        rf"^{re.escape(prefix)}_(?P<cc>[A-Z]+)_(?P<date>\d+)\.geojson$"
    )
    latest_by_cc: dict[str, tuple[str, pathlib.Path]] = {}
    unmatched: list[pathlib.Path] = []
    for path in sorted(paths):
        match = pattern.match(path.name)
        if not match:
            unmatched.append(path)
            continue
        cc = match.group("cc")
        date = match.group("date")
        if cc not in latest_by_cc or date > latest_by_cc[cc][0]:
            latest_by_cc[cc] = (date, path)
    if len(latest_by_cc) + len(unmatched) < len(paths):
        logger.info(
            "Filtered %d %s files to %d latest per country",
            len(paths),
            prefix,
            len(latest_by_cc) + len(unmatched),
        )
    return [p for _, p in latest_by_cc.values()] + unmatched


def _drop_mutual_common_border_duplicates(
    section_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Drop one side of mutual EURIS-EURIS common border section pairs.

    Pairs where country A's ``code_cb`` points to country B's ``code`` and vice
    versa represent the same physical border section supplied by both countries.
    Dutch (``NL``) sections are excluded here because NL is skipped during FIS
    integration.
    """
    if "code_cb" not in section_gdf.columns or "code" not in section_gdf.columns:
        return section_gdf

    cb_map = (
        section_gdf.dropna(subset=["code", "code_cb"])
        .set_index("code")["code_cb"]
        .to_dict()
    )
    to_drop: set[str] = set()
    for code, code_cb in cb_map.items():
        if str(code).startswith("NL") or str(code_cb).startswith("NL"):
            continue
        if cb_map.get(code_cb) == code:
            _, drop = sorted((str(code), str(code_cb)))
            to_drop.add(drop)

    if to_drop:
        logger.info(
            "Dropping %d mutual common-border duplicate section(s): %s",
            len(to_drop),
            sorted(to_drop),
        )
        section_gdf = section_gdf[~section_gdf["code"].isin(to_drop)]
    return section_gdf


def concat_nodes(data_dir: pathlib.Path) -> gpd.GeoDataFrame:
    """Concatenate all EURIS node files into a single GeoDataFrame.

    Args:
        data_dir: Directory containing Node_*.geojson files.

    Returns:
        Concatenated GeoDataFrame with deduplicated nodes.
    """
    node_paths = _latest_paths_by_country(
        list(data_dir.glob("Node_*.geojson")), prefix="Node"
    )
    logger.info("Found %d node files", len(node_paths))

    if not node_paths:
        raise FileNotFoundError(f"No Node_*.geojson files found in {data_dir}")

    node_gdfs = []
    for node_path in tqdm(node_paths, desc="Reading node files"):
        gdf = gpd.read_file(node_path)
        gdf["path"] = node_path.name
        node_gdfs.append(gdf)

    node_gdf = pd.concat(node_gdfs)

    # Deduplicate
    uniq_columns = set(node_gdf.columns) - {"path"}
    n_duplicated = node_gdf.duplicated(subset=uniq_columns).sum()
    node_gdf = node_gdf.drop_duplicates(subset=uniq_columns)
    logger.info("Removed %d duplicated nodes, kept %d", n_duplicated, len(node_gdf))

    # Add node_id
    node_path_re = re.compile(r"Node_(?P<countrycode>[A-Z]+)_\d+.geojson")
    node_gdf["countrycode_locode"] = node_gdf["locode"].apply(lambda x: x[:2])
    node_gdf["countrycode_path"] = node_gdf["path"].apply(
        lambda x: (
            node_path_re.match(x).group("countrycode")
            if node_path_re.match(x)
            else None
        )
    )
    node_gdf["countrycode"] = node_gdf["countrycode_locode"]
    node_gdf["node_id"] = node_gdf.apply(
        lambda row: f"{row['countrycode']}_{row['objectcode']}", axis=1
    )

    return node_gdf


def concat_sections(data_dir: pathlib.Path) -> gpd.GeoDataFrame:
    """Concatenate all EURIS section files into a single GeoDataFrame.

    Args:
        data_dir: Directory containing FairwaySection_*.geojson files.

    Returns:
        Concatenated GeoDataFrame with deduplicated sections.
    """
    section_paths = _latest_paths_by_country(
        list(data_dir.glob("FairwaySection_*.geojson")), prefix="FairwaySection"
    )
    logger.info("Found %d section files", len(section_paths))

    if not section_paths:
        raise FileNotFoundError(
            f"No FairwaySection_*.geojson files found in {data_dir}"
        )

    section_gdfs = []
    for section_path in tqdm(section_paths, desc="Reading section files"):
        gdf = gpd.read_file(section_path)
        gdf["path"] = section_path.name
        section_gdfs.append(gdf)

    section_gdf = pd.concat(section_gdfs)

    # Deduplicate
    uniq_columns = set(section_gdf.columns) - {"path"}
    n_duplicated = section_gdf.duplicated(subset=uniq_columns).sum()
    section_gdf = section_gdf.drop_duplicates(subset=uniq_columns)
    logger.info(
        "Removed %d duplicated sections, kept %d", n_duplicated, len(section_gdf)
    )

    return section_gdf


def _split_edge_at_node(graph: nx.Graph, section_code: str, split_node: str) -> None:
    """Split a EURIS section edge at an intermediate junction node (e.g. missing T-junction)."""
    if split_node not in graph:
        return
    split_pt = graph.nodes[split_node].get("geometry")
    if split_pt is None:
        return

    match = next(
        (
            (u, v, d)
            for u, v, d in graph.edges(data=True)
            if d.get("code") == section_code
        ),
        None,
    )
    if match is None:
        return

    u, v, attrs = match
    if split_node in (u, v):
        return

    geom = attrs.get("geometry")
    if geom is not None and geom.length > 0:
        u_pt = graph.nodes[u].get("geometry")
        start_pt = shapely.Point(geom.coords[0])
        end_pt = shapely.Point(geom.coords[-1])
        if u_pt is not None and u_pt.distance(end_pt) < u_pt.distance(start_pt):
            u, v = v, u
        frac = geom.project(split_pt, normalized=True)
        geom_1 = substring(geom, 0.0, frac, normalized=True)
        geom_2 = substring(geom, frac, 1.0, normalized=True)
    else:
        geom_1 = geom
        geom_2 = geom

    attrs_1 = {**attrs, "geometry": geom_1}
    attrs_2 = {**attrs, "geometry": geom_2}
    graph.remove_edge(u, v)
    graph.add_edge(u, split_node, **attrs_1)
    graph.add_edge(split_node, v, **attrs_2)
    logger.info(
        "Split EURIS section %s (%s -- %s) at %s", section_code, u, v, split_node
    )


def build_euris_graph(
    node_gdf: gpd.GeoDataFrame,
    section_gdf: gpd.GeoDataFrame,
    edge_splits: list[dict[str, str]] | None = None,
) -> nx.Graph:
    """Build EURIS network graph from nodes and sections.

    Args:
        node_gdf: Node GeoDataFrame with node_id column.
        section_gdf: Section GeoDataFrame with code column.
        edge_splits: Optional list of edge split overrides with
            ``section_code`` and ``split_node`` keys. Defaults to
            ``[euris.edge_splits]`` in ``config/schema.toml``.

    Returns:
        NetworkX graph with nodes and edges.
    """
    logger.info("Building node-section administration...")
    section_gdf = _drop_mutual_common_border_duplicates(section_gdf)
    node_section = section_gdf[["code"]].merge(
        node_gdf[["sectionref", "node_id"]], left_on="code", right_on="sectionref"
    )[["sectionref", "node_id"]]

    left_df = node_section.groupby("sectionref").first()
    right_df = node_section.groupby("sectionref").last()

    edge_df = pd.merge(
        left_df, right_df, left_index=True, right_index=True, suffixes=["_from", "_to"]
    )
    edge_df = edge_df.rename(columns={"node_id_from": "source", "node_id_to": "target"})

    section_gdf = section_gdf.merge(
        edge_df.reset_index(), left_on="code", right_on="sectionref"
    )

    logger.info("Building graph from %d sections...", len(section_gdf))
    graph = nx.from_pandas_edgelist(
        section_gdf, source="source", target="target", edge_attr=True
    )

    # Update node info
    logger.info("Updating node information for %d nodes...", len(node_gdf))
    for _, row in tqdm(node_gdf.iterrows(), total=len(node_gdf), desc="Updating nodes"):
        n = row["node_id"]
        if n not in graph.nodes:
            continue
        node = graph.nodes[n]
        euris_nodes = node.get("euris_nodes", [])
        euris_nodes.append(row.to_dict())
        node["euris_nodes"] = euris_nodes
        node.update(row.to_dict())

    if edge_splits is None:
        from fis.utils import load_schema

        edge_splits = load_schema().get("euris", {}).get("edge_splits", [])

    for split in edge_splits:
        _split_edge_at_node(graph, split["section_code"], split["split_node"])

    # Connect borders
    logger.info("Connecting border nodes...")
    border_node_gdf = node_gdf[~node_gdf["borderpoint"].isna()]
    border_locode_connections = pd.merge(
        border_node_gdf[["node_id", "borderpoint"]],
        node_gdf[["node_id", "locode"]],
        left_on="borderpoint",
        right_on="locode",
    )
    border_locode_connections = border_locode_connections.rename(
        columns={"node_id_x": "source", "node_id_y": "target"}
    )
    border_locode_connections = border_locode_connections[
        border_locode_connections["source"].isin(graph.nodes)
        & border_locode_connections["target"].isin(graph.nodes)
    ]
    logger.info("Found %d border connections", len(border_locode_connections))

    def geometry_for_border(row):
        source_geometry = graph.nodes[row["source"]]["geometry"]
        target_geometry = graph.nodes[row["target"]]["geometry"]
        return shapely.LineString([source_geometry, target_geometry])

    def border_edge_code(row):
        a, b = sorted((f"EURIS_{row['source']}", f"EURIS_{row['target']}"))
        return f"virtual_{a}_{b}"

    if len(border_locode_connections) > 0:
        border_locode_connections["geometry"] = border_locode_connections.apply(
            geometry_for_border, axis=1
        )
        border_locode_connections["code"] = border_locode_connections.apply(
            border_edge_code, axis=1
        )
        border_locode_connections["segment_type"] = "border_connection"
        border_graph = nx.from_pandas_edgelist(
            border_locode_connections, source="source", target="target", edge_attr=True
        )
        graph.add_edges_from(
            (e[0], e[1], attrs) for e, attrs in border_graph.edges.items()
        )
        for e in graph.edges:
            graph.edges[e]["is_border"] = e in border_graph.edges

    # Compute subgraphs
    logger.info("Computing subgraphs...")
    for i, component in enumerate(nx.connected_components(graph)):
        subgraph = graph.subgraph(component)
        for edge in subgraph.edges.values():
            edge["subgraph"] = i
        for node in subgraph.nodes.values():
            node["subgraph"] = i

    # Add length
    logger.info("Computing edge lengths...")
    geod = pyproj.Geod(ellps="WGS84")
    for edge in tqdm(
        graph.edges.values(), total=graph.number_of_edges(), desc="Edge lengths"
    ):
        edge["length_m"] = geod.geometry_length(edge["geometry"])

    logger.info(
        "Built EURIS graph: %d nodes, %d edges, %d components",
        graph.number_of_nodes(),
        graph.number_of_edges(),
        nx.number_connected_components(graph),
    )

    return graph


def export_euris_graph(
    graph: nx.Graph,
    output_dir: pathlib.Path,
) -> None:
    """Export EURIS graph to various formats.

    Args:
        graph: NetworkX graph to export.
        output_dir: Output directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Pickle
    with open(output_dir / "graph.pickle", "wb") as f:
        pickle.dump(graph, f)

    # GeoJSON and GeoParquet edges
    edge_df = pd.DataFrame(
        data=graph.edges.values(), index=graph.edges.keys()
    ).reset_index(names=["source", "target"])
    edge_gdf = gpd.GeoDataFrame(edge_df, crs="EPSG:4326")
    edge_gdf.to_file(output_dir / "edges.geojson")
    edge_gdf.to_parquet(output_dir / "edges.geoparquet")

    # GeoJSON and GeoParquet nodes
    # For geoparquet, list/dict types are difficult, but we handle euris_nodes
    node_df = []
    for node_id, attrs in graph.nodes(data=True):
        row = {"n": node_id}
        for k, v in attrs.items():
            if k == "euris_nodes" or isinstance(v, (list, dict)):
                continue
            row[k] = v
        node_df.append(row)

    node_gdf = gpd.GeoDataFrame(node_df, crs="EPSG:4326")
    node_gdf.to_file(output_dir / "nodes.geojson")
    node_gdf.to_parquet(output_dir / "nodes.geoparquet")

    # Summary

    summary = {
        "num_nodes": graph.number_of_nodes(),
        "num_edges": graph.number_of_edges(),
        "num_connected_components": nx.number_connected_components(graph),
    }
    with open(output_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    logger.info("Exported EURIS graph to %s", output_dir)
