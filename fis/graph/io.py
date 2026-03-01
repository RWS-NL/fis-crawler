"""Data loading and export functions for FIS graph."""

import json
import logging
import pathlib
import pickle
from typing import Tuple

import geopandas as gpd
import networkx as nx

logger = logging.getLogger(__name__)


def load_fis_data(
    export_dir: pathlib.Path,
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Load sections and junctions from geoparquet files.

    Args:
        export_dir: Path to the fis-export directory containing geoparquet files.

    Returns:
        Tuple of (sections_gdf, junctions_gdf)
    """
    export_dir = pathlib.Path(export_dir)

    logger.info("Loading sections from %s", export_dir / "section.geoparquet")
    sections = gpd.read_parquet(export_dir / "section.geoparquet")

    logger.info("Loading junctions from %s", export_dir / "sectionjunction.geoparquet")
    junctions = gpd.read_parquet(export_dir / "sectionjunction.geoparquet")

    logger.info("Loaded %d sections and %d junctions", len(sections), len(junctions))

    return sections, junctions


def export_graph(
    graph: nx.Graph,
    sections: gpd.GeoDataFrame,
    junctions: gpd.GeoDataFrame,
    output_dir: pathlib.Path,
) -> None:
    """Export graph to pickle and nodes/edges to geoparquet/geojson.

    Args:
        graph: The networkx graph to export.
        sections: The filtered sections GeoDataFrame (edges).
        junctions: The filtered junctions GeoDataFrame (nodes).
        output_dir: Output directory for exports.
    """
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Export graph as pickle
    pickle_path = output_dir / "graph.pickle"
    logger.info("Exporting graph to %s", pickle_path)
    with open(pickle_path, "wb") as f:
        pickle.dump(graph, f)

    # Export edges (sections)
    edges_parquet = output_dir / "edges.geoparquet"
    edges_geojson = output_dir / "edges.geojson"
    logger.info("Exporting %d edges to %s", len(sections), edges_parquet)
    sections.to_parquet(edges_parquet)
    sections.to_file(edges_geojson, driver="GeoJSON")

    # Export nodes (junctions)
    nodes_parquet = output_dir / "nodes.geoparquet"
    nodes_geojson = output_dir / "nodes.geojson"
    logger.info("Exporting %d nodes to %s", len(junctions), nodes_parquet)
    junctions.to_parquet(nodes_parquet)
    junctions.to_file(nodes_geojson, driver="GeoJSON")

    # Export summary
    summary = {
        "num_nodes": graph.number_of_nodes(),
        "num_edges": graph.number_of_edges(),
        "num_connected_components": nx.number_connected_components(graph),
        "largest_component_size": len(max(nx.connected_components(graph), key=len)),
    }
    summary_path = output_dir / "summary.json"
    logger.info("Exporting summary to %s", summary_path)
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    logger.info("Export complete: %s", output_dir)
