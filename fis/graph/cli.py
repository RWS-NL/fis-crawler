"""Command-line interface for FIS graph building."""

import logging
import pathlib

import click

from .build import build_graph
from .io import export_graph, load_fis_data
from .integrate import load_euris_graph, load_border_nodes, find_geometric_border_connections, merge_graphs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """FIS Fairway Network Graph tools."""
    pass


@cli.command()
@click.option(
    "--export-dir",
    type=click.Path(exists=True, path_type=pathlib.Path),
    default="output/fis-export",
    help="Path to FIS export directory.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=pathlib.Path),
    default="output/fis-graph",
    help="Output directory for FIS graph.",
)
def fis(export_dir: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Build basic FIS graph (nodes/edges only)."""
    logger.info("Building FIS graph")
    sections, junctions = load_fis_data(export_dir)
    graph, filtered_sections, filtered_junctions = build_graph(sections, junctions)
    export_graph(graph, filtered_sections, filtered_junctions, output_dir)
    logger.info("FIS graph exported to %s", output_dir)


@cli.command()
@click.option(
    "--euris-export",
    type=click.Path(exists=True, path_type=pathlib.Path),
    default="output/euris-export",
    help="Path to EURIS export directory with Node_*.geojson and FairwaySection_*.geojson files.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=pathlib.Path),
    default="output/euris-graph",
    help="Output directory for EURIS graph.",
)
def euris(euris_export: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Build EURIS graph from crawled GeoJSON files."""
    from .euris import concat_nodes, concat_sections, build_euris_graph, export_euris_graph
    
    logger.info("Building EURIS graph from %s", euris_export)
    
    node_gdf = concat_nodes(euris_export)
    section_gdf = concat_sections(euris_export)
    graph = build_euris_graph(node_gdf, section_gdf)
    export_euris_graph(graph, output_dir)
    
    logger.info("EURIS graph exported to %s", output_dir)


@cli.command()
@click.option("--fis-graph", type=click.Path(exists=True, path_type=pathlib.Path), default="output/fis-graph")
@click.option("--fis-export", type=click.Path(exists=True, path_type=pathlib.Path), default="output/fis-export")
@click.option("--output-dir", type=click.Path(path_type=pathlib.Path), default="output/fis-enriched")
def enrich_fis(fis_graph: pathlib.Path, fis_export: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Enrich FIS graph with maximumdimensions and navigability (CEMT class)."""
    import pickle
    import json
    import networkx as nx
    import geopandas as gpd
    from shapely import wkt
    from .enrich import load_fis_enrichment_data, build_fis_section_enrichment, enrich_fis_graph
    
    logger.info("Enriching FIS graph")
    
    # Load base graph
    with open(fis_graph / "graph.pickle", "rb") as f:
        graph = pickle.load(f)
    logger.info("Loaded graph with %d nodes, %d edges", graph.number_of_nodes(), graph.number_of_edges())
    
    # Load enrichment data and apply
    datasets = load_fis_enrichment_data(fis_export)
    enrichment = build_fis_section_enrichment(datasets)
    graph = enrich_fis_graph(graph, datasets['section'], enrichment)
    
    # Export
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / "graph.pickle", "wb") as f:
        pickle.dump(graph, f)
    
    # Export edges with enrichment as GeoJSON
    edge_data = []
    for u, v, attrs in graph.edges(data=True):
        row = {"source": u, "target": v, **attrs}
        if "geometry" in row and hasattr(row["geometry"], "wkt"):
            pass  # Keep geometry
        elif "geometry_wkt" in row:
            row["geometry"] = wkt.loads(row.pop("geometry_wkt"))
        edge_data.append(row)
    
    if edge_data:
        edges_gdf = gpd.GeoDataFrame(edge_data, crs="EPSG:4326")
        edges_gdf.to_file(output_dir / "edges.geojson", driver="GeoJSON")
        edges_gdf.to_parquet(output_dir / "edges.geoparquet")
        logger.info("Exported %d enriched edges", len(edges_gdf))
    
    # Summary with enrichment stats
    enriched_edges = sum(1 for _, _, d in graph.edges(data=True) if 'cemt_class' in d)
    summary = {
        "num_nodes": graph.number_of_nodes(),
        "num_edges": graph.number_of_edges(),
        "num_connected_components": nx.number_connected_components(graph),
        "edges_with_cemt": enriched_edges,
    }
    with open(output_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    logger.info("FIS enriched graph exported to %s", output_dir)


@cli.command()
@click.option("--euris-dir", type=click.Path(exists=True, path_type=pathlib.Path), default="output/euris-graph")
@click.option("--euris-export", type=click.Path(exists=True, path_type=pathlib.Path), default="output/euris-export")
@click.option("--output-dir", type=click.Path(path_type=pathlib.Path), default="output/euris-enriched")
def enrich_euris(euris_dir: pathlib.Path, euris_export: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Enrich EURIS graph with SailingSpeed attributes."""
    import pickle
    import json
    import networkx as nx
    from .enrich import load_euris_sailing_speed, enrich_euris_with_speed
    
    logger.info("Enriching EURIS graph with sailing speed")
    
    # Load graph
    with open(euris_dir / "graph.pickle", "rb") as f:
        graph = pickle.load(f)
    
    # Load and apply sailing speed
    sailing_speed = load_euris_sailing_speed(euris_export)
    graph = enrich_euris_with_speed(graph, sailing_speed)
    
    # Export enriched graph
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / "graph.pickle", "wb") as f:
        pickle.dump(graph, f)
    
    summary = {
        "num_nodes": graph.number_of_nodes(),
        "num_edges": graph.number_of_edges(),
        "num_connected_components": nx.number_connected_components(graph),
        "enrichment": ["sailing_speed"],
    }
    with open(output_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    logger.info("EURIS enriched graph at %s", output_dir)


@cli.command()
@click.option("--fis-enriched", type=click.Path(exists=True, path_type=pathlib.Path), default="output/fis-enriched")
@click.option("--euris-enriched", type=click.Path(exists=True, path_type=pathlib.Path), default="output/euris-enriched")
@click.option("--export-dir", type=click.Path(exists=True, path_type=pathlib.Path), default="output/fis-export")
@click.option("--output-dir", type=click.Path(path_type=pathlib.Path), default="output/merged-graph")
def merge(fis_enriched: pathlib.Path, euris_enriched: pathlib.Path, export_dir: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Merge FIS and EURIS graphs via border nodes."""
    import pickle
    import json
    import networkx as nx
    import geopandas as gpd
    import pandas as pd
    from shapely import wkt
    
    logger.info("Merging FIS and EURIS graphs")
    
    with open(fis_enriched / "graph.pickle", "rb") as f:
        fis = pickle.load(f)
    with open(euris_enriched / "graph.pickle", "rb") as f:
        euris = pickle.load(f)
    
    connections = find_geometric_border_connections(fis, euris)
    merged = merge_graphs(fis, euris, connections)
    
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / "graph.pickle", "wb") as f:
        pickle.dump(merged, f)
    
    # Export nodes as geoparquet and geojson
    node_data = []
    skip_cols = {'euris_nodes'}  # Non-serializable columns to skip
    for node_id, attrs in merged.nodes(data=True):
        row = {"node_id": node_id}
        for k, v in attrs.items():
            if k in skip_cols or isinstance(v, (list, dict)):
                continue
            if hasattr(v, 'wkt'):  # Geometry object
                if k == 'geometry':
                    row['geometry'] = v
            elif k == 'geometry_wkt':
                row['geometry'] = wkt.loads(v)
            else:
                row[k] = v
        node_data.append(row)
    if node_data:
        nodes_gdf = gpd.GeoDataFrame(node_data, crs="EPSG:4326")
        nodes_gdf.to_parquet(output_dir / "nodes.geoparquet")
        nodes_gdf.to_file(output_dir / "nodes.geojson", driver="GeoJSON")
        logger.info("Exported %d nodes", len(nodes_gdf))
    
    # Export edges as geoparquet and geojson
    edge_data = []
    for u, v, attrs in merged.edges(data=True):
        row = {"source": u, "target": v, **attrs}
        if "geometry_wkt" in row:
            row["geometry"] = wkt.loads(row.pop("geometry_wkt"))
        elif "geometry" in row and isinstance(row["geometry"], str):
            row["geometry"] = wkt.loads(row["geometry"])
        edge_data.append(row)
    if edge_data:
        edges_gdf = gpd.GeoDataFrame(edge_data, crs="EPSG:4326")
        edges_gdf.to_parquet(output_dir / "edges.geoparquet")
        edges_gdf.to_file(output_dir / "edges.geojson", driver="GeoJSON")
        logger.info("Exported %d edges", len(edges_gdf))
    
    summary = {
        "num_nodes": merged.number_of_nodes(),
        "num_edges": merged.number_of_edges(),
        "num_connected_components": nx.number_connected_components(merged),
        "border_connections": len(connections),
    }
    with open(output_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    # Export border connections for inspection
    # Convert list of dicts to GeoDataFrame
    # Keys: foreign_node, foreign_cc, bridgehead_node, fis_node, distance
    
    border_rows = []
    for c in connections:
        # Get geometry from bridgehead node
        bh_node = euris.nodes[c['bridgehead_node']]
        geom = None
        if 'geometry' in bh_node:
           geom = bh_node['geometry']
        
        border_rows.append({
            "fis_junction_id": c['fis_node'],
            "euris_node_id": c['foreign_node'],
            "bridgehead": c['bridgehead_node'],
            "distance": c['distance'],
            "geometry": geom
        })
    
    if border_rows:
        border_gdf = gpd.GeoDataFrame(border_rows, crs="EPSG:4326")
        border_gdf.to_file(output_dir / "border_connections.geojson", driver="GeoJSON")
        logger.info("Exported %d geometric border connections", len(border_gdf))
    
    logger.info("Merged graph exported to %s", output_dir)


@cli.command()
def all() -> None:
    """Run full pipeline: fis -> euris -> enrich -> merge."""
    from click.testing import CliRunner
    runner = CliRunner()
    
    for cmd in [fis, euris, enrich_fis, enrich_euris, merge]:
        logger.info("Running: %s", cmd.name)
        result = runner.invoke(cmd)
        if result.exit_code != 0:
            logger.error("Failed: %s", result.output)
            raise click.ClickException(f"Command {cmd.name} failed")


if __name__ == "__main__":
    cli()
