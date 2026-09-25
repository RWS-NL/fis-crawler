"""Schema harmonization for FIS/EURIS graphs."""

import logging
import pathlib
import tomllib
from typing import Dict, Any

import networkx as nx

from fis.utils import stringify_id

logger = logging.getLogger(__name__)


def load_schema(
    config_path: pathlib.Path = pathlib.Path("config/schema.toml"),
) -> Dict[str, Any]:
    """Load schema configuration from TOML file.

    Args:
        config_path: Path to schema.toml.

    Returns:
        Dictionary with schema configuration.
    """
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def apply_schema_mapping(graph: nx.Graph, schema: Dict[str, Any]) -> nx.Graph:
    """Apply attribute mappings to graph element.

    Args:
        graph: NetworkX graph to harmonize.
        schema: Schema configuration dict.

    Returns:
        Graph with renamed attributes.
    """
    mappings = schema.get("attributes", {})
    node_map = mappings.get("nodes", {})
    edge_map = mappings.get("edges", {})
    identifiers = set(schema.get("identifiers", {}).get("columns", []))

    # 1. Harmonize Nodes
    logger.info("Harmonizing node attributes")
    for _, attrs in graph.nodes(data=True):
        keys = list(attrs.keys())
        for k in keys:
            if k in node_map:
                new_key = node_map[k]
                val = attrs.pop(k)
                if new_key in identifiers:
                    val = stringify_id(val)
                if new_key not in attrs or not attrs[new_key]:
                    attrs[new_key] = val

    # 2. Harmonize Edges
    logger.info("Harmonizing edge attributes")
    for _, _, attrs in graph.edges(data=True):
        keys = list(attrs.keys())
        for k in keys:
            if k in edge_map:
                new_key = edge_map[k]
                val = attrs.pop(k)
                if new_key in identifiers:
                    val = stringify_id(val)
                if new_key not in attrs or not attrs[new_key]:
                    attrs[new_key] = val
        # Drop redundant/vague length columns to enforce length_m consistency
        for key_to_drop in ["Length", "length", "length_km"]:
            if key_to_drop in attrs:
                attrs.pop(key_to_drop)

    return graph
