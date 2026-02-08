"""Command-line interface for FIS graph building."""

import logging
import pathlib

import click

from .build import build_graph
from .io import export_graph, load_fis_data
from .integrate import load_euris_graph, load_border_nodes, find_border_connections, merge_graphs

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
    default="fis-export",
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
    "--euris-pickle",
    type=click.Path(exists=True, path_type=pathlib.Path),
    default="euris-export/v0.1.0/export-graph-v0.1.0.pickle",
    help="Path to EURIS graph pickle.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=pathlib.Path),
    default="output/euris-graph",
    help="Output directory for EURIS graph.",
)
def euris(euris_pickle: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Export EURIS graph (nodes/edges only)."""
    logger.info("Loading EURIS graph from %s", euris_pickle)
    graph = load_euris_graph(euris_pickle)
    
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    import pickle
    import json
    import networkx as nx
    
    with open(output_dir / "graph.pickle", "wb") as f:
        pickle.dump(graph, f)
    
    summary = {
        "num_nodes": graph.number_of_nodes(),
        "num_edges": graph.number_of_edges(),
        "num_connected_components": nx.number_connected_components(graph),
    }
    with open(output_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    logger.info("EURIS graph exported to %s", output_dir)


@cli.command()
@click.option("--fis-dir", type=click.Path(exists=True, path_type=pathlib.Path), default="output/fis-graph")
@click.option("--output-dir", type=click.Path(path_type=pathlib.Path), default="output/fis-enriched")
def enrich_fis(fis_dir: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Enrich FIS graph with additional attributes (placeholder)."""
    import shutil
    logger.info("Enriching FIS graph (placeholder - copying as-is)")
    output_dir = pathlib.Path(output_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    shutil.copytree(fis_dir, output_dir)
    logger.info("FIS enriched graph at %s (TODO: add attributes, split at structures)", output_dir)


@cli.command()
@click.option("--euris-dir", type=click.Path(exists=True, path_type=pathlib.Path), default="output/euris-graph")
@click.option("--euris-export", type=click.Path(exists=True, path_type=pathlib.Path), default="euris-export")
@click.option("--output-dir", type=click.Path(path_type=pathlib.Path), default="output/euris-enriched")
def enrich_euris(euris_dir: pathlib.Path, euris_export: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Enrich EURIS graph with SailingSpeed attributes."""
    import pickle
    import json
    import networkx as nx
    from .enrich import load_sailing_speed, enrich_euris_with_speed
    
    logger.info("Enriching EURIS graph with sailing speed")
    
    # Load graph
    with open(euris_dir / "graph.pickle", "rb") as f:
        graph = pickle.load(f)
    
    # Load and apply sailing speed
    sailing_speed = load_sailing_speed(euris_export)
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
@click.option("--export-dir", type=click.Path(exists=True, path_type=pathlib.Path), default="fis-export")
@click.option("--output-dir", type=click.Path(path_type=pathlib.Path), default="output/merged-graph")
def merge(fis_enriched: pathlib.Path, euris_enriched: pathlib.Path, export_dir: pathlib.Path, output_dir: pathlib.Path) -> None:
    """Merge FIS and EURIS graphs via border nodes."""
    import pickle
    import json
    import networkx as nx
    
    logger.info("Merging FIS and EURIS graphs")
    
    with open(fis_enriched / "graph.pickle", "rb") as f:
        fis = pickle.load(f)
    with open(euris_enriched / "graph.pickle", "rb") as f:
        euris = pickle.load(f)
    
    border_nodes = load_border_nodes(export_dir)
    connections = find_border_connections(fis, euris, border_nodes)
    merged = merge_graphs(fis, euris, connections)
    
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / "graph.pickle", "wb") as f:
        pickle.dump(merged, f)
    
    summary = {
        "num_nodes": merged.number_of_nodes(),
        "num_edges": merged.number_of_edges(),
        "num_connected_components": nx.number_connected_components(merged),
        "border_connections": len(connections),
    }
    with open(output_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
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
