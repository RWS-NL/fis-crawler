"""Network integration between FIS and EURIS graphs."""

import logging
import pathlib
import pickle
from typing import Dict, List

import geopandas as gpd
import networkx as nx
from shapely.geometry import LineString, Point

logger = logging.getLogger(__name__)

# Belgian EURIS sections that physically overlap with the authoritative FIS network
# without a `cntrycode_cb == "NL"` tag: code -> (outer_euris_node, inner_euris_node)
_REDUNDANT_BE_BORDER_SECTIONS: dict[str, tuple[str, str]] = {
    "BE0173200000": ("BE_F7792", "BE_D7511"),  # Antwerpen / Schelde
    "BE0532800000": ("BE_F6151", "BE_F5919"),  # Kanaal Gent-Terneuzen
    "BE0210100000": ("BE_W3531", "BE_W3523"),  # Petit-Lanaye
}


def _extract_node_point(attrs: Dict) -> Point | None:
    """Return a node's Point geometry from `geometry`, `Geometry`, or `(x, y)`."""
    if "geometry" in attrs and isinstance(attrs["geometry"], Point):
        return attrs["geometry"]
    if "Geometry" in attrs and isinstance(attrs["Geometry"], Point):
        return attrs["Geometry"]
    if "x" in attrs and "y" in attrs:
        return Point(attrs["x"], attrs["y"])
    return None


def _build_fis_nodes_gdf(fis_graph: nx.Graph) -> gpd.GeoDataFrame | None:
    """Build a UTM 31N GeoDataFrame of FIS nodes with valid Point geometries."""
    fis_points = []
    fis_ids = []
    for n, d in fis_graph.nodes(data=True):
        p = _extract_node_point(d)
        if p is not None:
            fis_points.append(p)
            fis_ids.append(n)
    if not fis_points:
        return None
    return gpd.GeoDataFrame(
        {"node_id": fis_ids, "geometry": fis_points}, crs="EPSG:4326"
    ).to_crs("EPSG:32631")


def _nearest_fis_node(fis_gdf: gpd.GeoDataFrame, point: Point) -> tuple[object, float]:
    """Return `(fis_node_id, distance_m)` for the nearest FIS node to an EPSG:4326 Point."""
    from ..lock.utils import project_geometry

    pt_rd = project_geometry(point, "EPSG:4326", "EPSG:32631")
    dists = fis_gdf.distance(pt_rd)
    nearest_idx = dists.idxmin()
    return fis_gdf.iloc[nearest_idx].node_id, float(dists.min())


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

    # 1. Prepare FIS nodes spatial index
    fis_gdf = _build_fis_nodes_gdf(fis_graph)
    if fis_gdf is None:
        logger.warning("No valid geometry found in FIS graph nodes")
        return []

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
        p = _extract_node_point(euris_graph.nodes[bh])
        if p is not None:
            fis_node_id, min_dist = _nearest_fis_node(fis_gdf, p)
            if min_dist < distance_threshold:
                matches[bh] = {
                    "fis_node": fis_node_id,
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
        node_attrs = {"data_source": "FIS", **attrs}
        combined.add_node(f"FIS_{node_id}", **node_attrs)

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

        edge_attrs = {"data_source": "FIS", **attrs}
        combined.add_edge(f"FIS_{u}", f"FIS_{v}", **edge_attrs)

    # Identify redundant BE-NL overlap sections present in EURIS so we can drop
    # the duplicate BE edge + outer node and snap the inner BE node directly to FIS.
    skip_euris_nodes: set[str] = set()
    retarget_outer_to_inner: dict[str, str] = {}
    for code, (outer, inner) in _REDUNDANT_BE_BORDER_SECTIONS.items():
        if euris_graph.has_edge(outer, inner):
            skip_euris_nodes.add(outer)
            retarget_outer_to_inner[outer] = inner

    # Add EURIS nodes (already have country prefix like NL_J3524)
    logger.info("Adding EURIS nodes to combined graph (excluding NL)")
    for node_id, attrs in euris_graph.nodes(data=True):
        # Skip Dutch nodes in EURIS as FIS provides the authoritative network
        if attrs.get("countrycode") == "NL" or node_id in skip_euris_nodes:
            continue
        node_attrs = {"data_source": "EURIS", **attrs}
        combined.add_node(f"EURIS_{node_id}", **node_attrs)

    # Add EURIS edges
    logger.info("Adding EURIS edges to combined graph (excluding NL)")
    for u, v, attrs in euris_graph.edges(data=True):
        # Skip edges where either node is Dutch or skipped outer border node
        u_cc = euris_graph.nodes[u].get("countrycode")
        v_cc = euris_graph.nodes[v].get("countrycode")

        if u_cc == "NL" or v_cc == "NL":
            continue
        if u in skip_euris_nodes or v in skip_euris_nodes:
            continue
        if attrs.get("cntrycode_cb") == "NL":
            logger.info(
                "Skipping common-border EURIS edge %s (covered by FIS)",
                attrs.get("code"),
            )
            continue
        if attrs.get("code") in _REDUNDANT_BE_BORDER_SECTIONS:
            logger.info(
                "Skipping overlapping BE border edge %s (covered by FIS)",
                attrs.get("code"),
            )
            continue

        edge_attrs = {"data_source": "EURIS", **attrs}
        combined.add_edge(f"EURIS_{u}", f"EURIS_{v}", **edge_attrs)

    # Add new border connections
    fis_nodes_gdf = _build_fis_nodes_gdf(fis_graph) if retarget_outer_to_inner else None
    logger.info("Adding %d border connections", len(connections))
    for conn in connections:
        foreign_node = conn["foreign_node"]
        fis_node = conn["fis_node"]
        distance = conn["distance"]
        edge_attrs = conn.get("edge_attrs", {}).copy()

        if foreign_node in retarget_outer_to_inner:
            inner_node = retarget_outer_to_inner[foreign_node]
            inner_pt = _extract_node_point(euris_graph.nodes[inner_node])
            if fis_nodes_gdf is not None and inner_pt is not None:
                fis_node, distance = _nearest_fis_node(fis_nodes_gdf, inner_pt)
                fis_pt = _extract_node_point(fis_graph.nodes[fis_node])
                if fis_pt is not None:
                    edge_attrs["geometry"] = LineString([fis_pt, inner_pt])
                    edge_attrs["length_m"] = distance
            logger.info(
                "Retargeted border connection %s -> %s (FIS:%s, %.1fm)",
                foreign_node,
                inner_node,
                fis_node,
                distance,
            )
            foreign_node = inner_node

        # Link: FIS_<fis_node> <--> EURIS_<foreign_node>
        # Skipping the bridgehead node effectively stitches the networks
        u = f"FIS_{fis_node}"
        v = f"EURIS_{foreign_node}"

        # Override/Set metadata
        a, b = sorted((u, v))
        edge_attrs.update(
            {
                "data_source": "BORDER",
                "bridgehead": conn["bridgehead_node"],
                "distance_gap": distance,
                "connection_type": conn["type"],
            }
        )
        existing_code = edge_attrs.get("code")
        if (
            not existing_code
            or (isinstance(existing_code, float) and existing_code != existing_code)
            or str(existing_code).strip().lower() in ("", "nan", "none")
            or str(existing_code).startswith("virtual_")
        ):
            edge_attrs["code"] = f"virtual_{a}_{b}"
        if not edge_attrs.get("segment_type"):
            edge_attrs["segment_type"] = "border_connection"

        combined.add_edge(u, v, **edge_attrs)

    logger.info(
        "Combined graph: %d nodes, %d edges, %d components",
        combined.number_of_nodes(),
        combined.number_of_edges(),
        nx.number_connected_components(combined),
    )

    return combined
