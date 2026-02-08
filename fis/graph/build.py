"""Core graph building logic for FIS fairway network."""

import logging
from typing import Tuple

import geopandas as gpd
import networkx as nx
import pandas as pd

logger = logging.getLogger(__name__)


def filter_sections(sections: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Filter sections to those with valid junction IDs.

    Args:
        sections: All sections from FIS data.

    Returns:
        Filtered sections with valid StartJunctionId and EndJunctionId.
    """
    valid = sections[
        sections["StartJunctionId"].notna() & sections["EndJunctionId"].notna()
    ].copy()

    # Convert junction IDs to int for consistency
    valid["StartJunctionId"] = valid["StartJunctionId"].astype(int)
    valid["EndJunctionId"] = valid["EndJunctionId"].astype(int)

    logger.info(
        "Filtered sections: %d -> %d (removed %d without junction IDs)",
        len(sections),
        len(valid),
        len(sections) - len(valid),
    )

    return valid


def filter_junctions(
    junctions: gpd.GeoDataFrame, sections: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Filter junctions to only those referenced by sections.

    Args:
        junctions: All junctions from FIS data.
        sections: Filtered sections with valid junction IDs.

    Returns:
        Junctions that are referenced by at least one section.
    """
    referenced_ids = set(sections["StartJunctionId"]) | set(sections["EndJunctionId"])

    valid = junctions[junctions["Id"].isin(referenced_ids)].copy()

    logger.info(
        "Filtered junctions: %d -> %d (keeping only referenced)",
        len(junctions),
        len(valid),
    )

    return valid


def build_graph(
    sections: gpd.GeoDataFrame, junctions: gpd.GeoDataFrame
) -> Tuple[nx.Graph, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Build networkx graph from sections (edges) and junctions (nodes).

    Args:
        sections: Sections GeoDataFrame with StartJunctionId and EndJunctionId.
        junctions: Junctions GeoDataFrame with Id and geometry.

    Returns:
        Tuple of (graph, filtered_sections, filtered_junctions)
    """
    # Filter to valid sections and referenced junctions
    filtered_sections = filter_sections(sections)
    filtered_junctions = filter_junctions(junctions, filtered_sections)

    # Prepare edge data for networkx
    edge_data = filtered_sections.copy()
    edge_data = edge_data.rename(
        columns={"StartJunctionId": "source", "EndJunctionId": "target"}
    )

    # Convert geometry to WKT for edge attributes (networkx can't serialize shapely)
    edge_data["geometry_wkt"] = edge_data["geometry"].apply(lambda g: g.wkt)

    # Build graph from edge list
    logger.info("Building graph from %d edges", len(edge_data))
    graph = nx.from_pandas_edgelist(
        edge_data, source="source", target="target", edge_attr=True
    )

    # Add node attributes from junctions
    logger.info("Adding node attributes from %d junctions", len(filtered_junctions))
    junction_dict = filtered_junctions.set_index("Id").to_dict("index")

    for node_id in graph.nodes():
        if node_id in junction_dict:
            attrs = junction_dict[node_id]
            # Convert geometry to WKT
            if "geometry" in attrs:
                attrs["geometry_wkt"] = attrs["geometry"].wkt
                attrs["x"] = attrs["geometry"].x
                attrs["y"] = attrs["geometry"].y
                del attrs["geometry"]
            graph.nodes[node_id].update(attrs)

    # Log graph statistics
    logger.info(
        "Graph built: %d nodes, %d edges, %d connected components",
        graph.number_of_nodes(),
        graph.number_of_edges(),
        nx.number_connected_components(graph),
    )

    return graph, filtered_sections, filtered_junctions
