"""Schema harmonization for FIS/EURIS graphs."""

import logging
import pathlib
import tomllib
from typing import Dict, Any

import networkx as nx

logger = logging.getLogger(__name__)


def load_schema(config_path: pathlib.Path = pathlib.Path("config/schema.toml")) -> Dict[str, Any]:
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
    
    # 1. Harmonize Nodes
    logger.info("Harmonizing node attributes")
    for _, attrs in graph.nodes(data=True):
        # Rename keys in place
        # Create list of keys to avoid runtime error during iteration
        keys = list(attrs.keys())
        for k in keys:
            if k in node_map:
                new_key = node_map[k]
                # Only rename if new key doesn't exist or we want to overwrite
                # Schema mapping implies strict rename
                attrs[new_key] = attrs.pop(k)
                
    # 2. Harmonize Edges
    logger.info("Harmonizing edge attributes")
    for _, _, attrs in graph.edges(data=True):
        keys = list(attrs.keys())
        for k in keys:
            if k in edge_map:
                new_key = edge_map[k]
                attrs[new_key] = attrs.pop(k)
                
    return graph
