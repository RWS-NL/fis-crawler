"""Network integration between FIS and EURIS graphs."""

import logging
import pathlib
import pickle
from typing import Dict, List, Tuple

import geopandas as gpd
import networkx as nx

logger = logging.getLogger(__name__)


def load_euris_graph(path: pathlib.Path) -> nx.Graph:
    """Load EURIS graph from pickle file.

    Args:
        path: Path to the EURIS graph pickle file.

    Returns:
        Loaded networkx graph.
    """
    logger.info("Loading EURIS graph from %s", path)
    with open(path, "rb") as f:
        graph = pickle.load(f)
    logger.info("Loaded EURIS graph: %d nodes, %d edges", graph.number_of_nodes(), graph.number_of_edges())
    return graph


def load_border_nodes(export_dir: pathlib.Path) -> gpd.GeoDataFrame:
    """Load FIS common border nodes.

    Args:
        export_dir: Path to FIS export directory.

    Returns:
        GeoDataFrame with border node information.
    """
    path = export_dir / "commonbordernode.geoparquet"
    logger.info("Loading border nodes from %s", path)
    border_nodes = gpd.read_parquet(path)
    logger.info("Loaded %d border nodes", len(border_nodes))
    return border_nodes


def find_border_connections(
    fis_graph: nx.Graph,
    euris_graph: nx.Graph,
    border_nodes: gpd.GeoDataFrame,
) -> List[Tuple[int, str, str]]:
    """Find connections between FIS and EURIS networks via border nodes.

    Uses ISRS location codes to match FIS JunctionIds with EURIS nodes.

    Args:
        fis_graph: FIS networkx graph (Dutch network).
        euris_graph: EURIS networkx graph (international network).
        border_nodes: FIS common border nodes with LocationCode and JunctionId.

    Returns:
        List of tuples: (fis_junction_id, euris_node_id, location_code)
    """
    connections = []

    # Build lookup of EURIS nodes by locode
    euris_locode_to_node = {}
    for node_id in euris_graph.nodes():
        node_data = euris_graph.nodes[node_id]
        # EURIS nodes may have locode in euris_nodes list
        if "locode" in node_data:
            euris_locode_to_node[node_data["locode"]] = node_id
        # Also check borderpoint field
        if "borderpoint" in node_data and node_data["borderpoint"]:
            bp = node_data["borderpoint"]
            # borderpoint can be a country code or a full locode
            if len(bp) > 2:  # Full locode
                euris_locode_to_node[bp] = node_id

    logger.info("Built EURIS locode lookup with %d entries", len(euris_locode_to_node))

    # Match FIS border nodes to EURIS
    for _, row in border_nodes.iterrows():
        fis_junction_id = int(row["JunctionId"])
        location_code = row["LocationCode"]

        # Check if FIS junction exists in FIS graph
        if fis_junction_id not in fis_graph.nodes():
            logger.debug("FIS junction %d not in graph", fis_junction_id)
            continue

        # Try to find matching EURIS node
        if location_code in euris_locode_to_node:
            euris_node = euris_locode_to_node[location_code]
            connections.append((fis_junction_id, euris_node, location_code))
            logger.debug(
                "Found connection: FIS %d <-> EURIS %s via %s",
                fis_junction_id,
                euris_node,
                location_code,
            )

    logger.info("Found %d potential border connections", len(connections))
    return connections


def merge_graphs(
    fis_graph: nx.Graph,
    euris_graph: nx.Graph,
    connections: List[Tuple[int, str, str]],
) -> nx.Graph:
    """Merge FIS and EURIS graphs into a combined network.

    Args:
        fis_graph: FIS networkx graph.
        euris_graph: EURIS networkx graph.
        connections: List of (fis_node, euris_node, locode) tuples.

    Returns:
        Combined networkx graph.
    """
    # Create combined graph with prefixed node IDs to avoid collisions
    combined = nx.Graph()

    # Add FIS nodes with prefix
    logger.info("Adding FIS nodes to combined graph")
    for node_id, attrs in fis_graph.nodes(data=True):
        combined.add_node(f"FIS_{node_id}", source="FIS", **attrs)

    # Add FIS edges
    for u, v, attrs in fis_graph.edges(data=True):
        combined.add_edge(f"FIS_{u}", f"FIS_{v}", source="FIS", **attrs)

    # Add EURIS nodes (already have country prefix like NL_J3524)
    logger.info("Adding EURIS nodes to combined graph")
    for node_id, attrs in euris_graph.nodes(data=True):
        combined.add_node(f"EURIS_{node_id}", source="EURIS", **attrs)

    # Add EURIS edges
    for u, v, attrs in euris_graph.edges(data=True):
        combined.add_edge(f"EURIS_{u}", f"EURIS_{v}", source="EURIS", **attrs)

    # Add border connections
    logger.info("Adding %d border connections", len(connections))
    for fis_node, euris_node, locode in connections:
        combined.add_edge(
            f"FIS_{fis_node}",
            f"EURIS_{euris_node}",
            source="BORDER",
            location_code=locode,
        )

    logger.info(
        "Combined graph: %d nodes, %d edges, %d components",
        combined.number_of_nodes(),
        combined.number_of_edges(),
        nx.number_connected_components(combined),
    )

    return combined
