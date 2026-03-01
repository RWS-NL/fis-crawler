"""Network integration between FIS and EURIS graphs."""

import logging
import pathlib
import pickle
from typing import Dict, List

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
    logger.info(
        "Loaded EURIS graph: %d nodes, %d edges",
        graph.number_of_nodes(),
        graph.number_of_edges(),
    )
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


def find_geometric_border_connections(
    fis_graph: nx.Graph,
    euris_graph: nx.Graph,
    distance_threshold: float = 100.0,
) -> List[Dict]:
    """Find border connections using geometric proximity of EURIS-NL nodes.

    Strategy:
    1. Find EURIS edges crossing into Netherlands (Non-NL <-> NL).
    2. The NL-side node in EURIS is treated as a "bridgehead".
    3. Match this bridgehead geometrically to the nearest FIS node.
    4. If distance < threshold, create a link between the Foreign EURIS node and the FIS node.

    Args:
        fis_graph: FIS network.
        euris_graph: EURIS network.
        distance_threshold: Max distance in meters for a valid match.

    Returns:
        List of connection dicts:
        {
            'foreign_node': str, (EURIS node ID)
            'foreign_cc': str, (Country code)
            'bridgehead_node': str, (EURIS-NL node ID)
            'fis_node': int, (FIS node ID)
            'distance': float, (Meters)
            'type': 'geometric'
        }
    """
    from shapely.geometry import Point

    # 1. Prepare FIS nodes spatial index
    fis_points = []
    fis_ids = []
    for n, d in fis_graph.nodes(data=True):
        # Extract geometry (could be x,y or shapely obj)
        p = None
        if "Geometry" in d and isinstance(d["Geometry"], Point):
            p = d["Geometry"]
        elif "geometry" in d and isinstance(d["geometry"], Point):
            p = d["geometry"]
        elif "x" in d and "y" in d:
            p = Point(d["x"], d["y"])

        if p:
            fis_points.append(p)
            fis_ids.append(n)

    if not fis_points:
        logger.warning("No valid geometry found in FIS graph nodes")
        return []

    # Create GDF in projected CRS for distance calculation (UTM 31N covers NL)
    fis_gdf = gpd.GeoDataFrame(
        {"node_id": fis_ids, "geometry": fis_points}, crs="EPSG:4326"
    ).to_crs("EPSG:32631")

    # 2. Find bridgehead nodes in EURIS
    border_edges = []
    bridgeheads = set()

    for u, v, d in euris_graph.edges(data=True):
        u_cc = euris_graph.nodes[u].get("countrycode")
        v_cc = euris_graph.nodes[v].get("countrycode")

        # Check for Non-NL <-> NL crossing
        # We only care about edges entering the NL network
        if u_cc == "NL" and v_cc and v_cc != "NL":
            border_edges.append((v, u, v_cc))  # Foreign -> Bridgehead
            bridgeheads.add(u)
        elif v_cc == "NL" and u_cc and u_cc != "NL":
            border_edges.append((u, v, u_cc))  # Foreign -> Bridgehead
            bridgeheads.add(v)

    logger.info(
        "Found %d EURIS cross-border edges with %d unique NL bridgeheads",
        len(border_edges),
        len(bridgeheads),
    )

    # 3. Match bridgeheads to FIS nodes
    matches = {}  # bridgehead_id -> {fis_node, distance}

    for bh in bridgeheads:
        d = euris_graph.nodes[bh]
        p = None
        if "geometry" in d and isinstance(d["geometry"], Point):
            p = d["geometry"]
        elif "x" in d and "y" in d:
            p = Point(d["x"], d["y"])

        if p:
            # Project point and find nearest FIS node
            bh_gdf = gpd.GeoDataFrame({"geometry": [p]}, crs="EPSG:4326").to_crs(
                "EPSG:32631"
            )

            # Simple distance to all (fast enough for <10k nodes)
            dists = fis_gdf.distance(bh_gdf.iloc[0].geometry)
            min_dist = dists.min()
            nearest_idx = dists.idxmin()

            if min_dist < distance_threshold:
                matches[bh] = {
                    "fis_node": fis_gdf.iloc[nearest_idx].node_id,
                    "distance": min_dist,
                }
                logger.debug(
                    "Matched %s -> FIS:%s (%.1fm)",
                    bh,
                    matches[bh]["fis_node"],
                    min_dist,
                )

    # 4. Create connections
    connections = []
    for foreign, bridgehead, cc in border_edges:
        if bridgehead in matches:
            m = matches[bridgehead]

            # Extract original edge attributes from EURIS
            edge_attrs = {}
            if euris_graph.has_edge(foreign, bridgehead):
                edge_attrs = euris_graph.edges[foreign, bridgehead].copy()
            elif euris_graph.has_edge(bridgehead, foreign):
                edge_attrs = euris_graph.edges[bridgehead, foreign].copy()

            connections.append(
                {
                    "foreign_node": foreign,
                    "foreign_cc": cc,
                    "bridgehead_node": bridgehead,
                    "fis_node": m["fis_node"],
                    "distance": m["distance"],
                    "type": "geometric",
                    "edge_attrs": edge_attrs,  # Pass original attributes
                }
            )

    logger.info("Established %d geometric border connections", len(connections))
    return connections


def merge_graphs(
    fis_graph: nx.Graph,
    euris_graph: nx.Graph,
    connections: List[Dict],
) -> nx.Graph:
    """Merge FIS and EURIS graphs into a combined network.

    Args:
        fis_graph: FIS networkx graph.
        euris_graph: EURIS networkx graph.
        connections: List of connection dicts (from find_geometric_border_connections).

    Returns:
        Combined networkx graph.
    """
    # Create combined graph with prefixed node IDs to avoid collisions
    combined = nx.Graph()

    # Add FIS nodes
    # Lobith correction: Remove foreign nodes extending into Germany
    # These are better represented by EURIS
    prune_node_ids = {22637860, 22638030}

    logger.info("Adding FIS nodes to combined graph")
    for node_id, attrs in fis_graph.nodes(data=True):
        if node_id in prune_node_ids:
            logger.info("Pruning FIS node %s - Lobith correction", node_id)
            continue
        combined.add_node(f"FIS_{node_id}", data_source="FIS", **attrs)

    # Add FIS edges
    # Lobith correction: Remove edge 22638449 (redundant border crossing)
    # Also remove any edges connected to pruned nodes
    prune_edge_ids = {22638449}

    for u, v, attrs in fis_graph.edges(data=True):
        if attrs.get("Id") in prune_edge_ids:
            logger.info(
                "Pruning FIS edge %s (Id: %s) - Lobith correction",
                (u, v),
                attrs.get("Id"),
            )
            continue

        # Skip edges connected to pruned nodes
        if u in prune_node_ids or v in prune_node_ids:
            continue

        combined.add_edge(f"FIS_{u}", f"FIS_{v}", data_source="FIS", **attrs)

    # Add EURIS nodes (already have country prefix like NL_J3524)
    logger.info("Adding EURIS nodes to combined graph (excluding NL)")
    for node_id, attrs in euris_graph.nodes(data=True):
        # Skip Dutch nodes in EURIS as FIS provides the authoritative network
        if attrs.get("countrycode") == "NL":
            continue
        combined.add_node(f"EURIS_{node_id}", data_source="EURIS", **attrs)

    # Add EURIS edges
    logger.info("Adding EURIS edges to combined graph (excluding NL)")
    for u, v, attrs in euris_graph.edges(data=True):
        # Skip edges where either node is Dutch
        u_cc = euris_graph.nodes[u].get("countrycode")
        v_cc = euris_graph.nodes[v].get("countrycode")

        if u_cc == "NL" or v_cc == "NL":
            continue

        combined.add_edge(f"EURIS_{u}", f"EURIS_{v}", data_source="EURIS", **attrs)

    # Add new border connections
    logger.info("Adding %d border connections", len(connections))
    for conn in connections:
        # Link: FIS_<fis_node> <--> EURIS_<foreign_node>
        # Skipping the bridgehead node effectively stitches the networks
        u = f"FIS_{conn['fis_node']}"
        v = f"EURIS_{conn['foreign_node']}"

        # Base attributes for border connection
        edge_attrs = conn.get("edge_attrs", {}).copy()

        # Override/Set metadata
        edge_attrs.update(
            {
                "data_source": "BORDER",
                "bridgehead": conn["bridgehead_node"],
                "distance_gap": conn["distance"],
                "connection_type": conn["type"],
            }
        )

        combined.add_edge(u, v, **edge_attrs)

    logger.info(
        "Combined graph: %d nodes, %d edges, %d components",
        combined.number_of_nodes(),
        combined.number_of_edges(),
        nx.number_connected_components(combined),
    )

    return combined
