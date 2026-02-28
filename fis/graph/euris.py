"""EURIS graph building logic."""

import logging
import pathlib
import pickle
import re
from glob import glob

import geopandas as gpd
import networkx as nx
import pandas as pd
import pyproj
import shapely
from tqdm.auto import tqdm

logger = logging.getLogger(__name__)


def concat_nodes(data_dir: pathlib.Path) -> gpd.GeoDataFrame:
    """Concatenate all EURIS node files into a single GeoDataFrame.
    
    Args:
        data_dir: Directory containing Node_*.geojson files.
        
    Returns:
        Concatenated GeoDataFrame with deduplicated nodes.
    """
    node_paths = list(data_dir.glob('Node_*.geojson'))
    logger.info("Found %d node files", len(node_paths))
    
    if not node_paths:
        raise FileNotFoundError(f"No Node_*.geojson files found in {data_dir}")
    
    node_gdfs = []
    for node_path in tqdm(node_paths, desc="Reading node files"):
        gdf = gpd.read_file(node_path)
        gdf['path'] = node_path.name
        node_gdfs.append(gdf)
    
    node_gdf = pd.concat(node_gdfs)
    
    # Deduplicate
    uniq_columns = set(node_gdf.columns) - {'path'}
    n_duplicated = node_gdf.duplicated(subset=uniq_columns).sum()
    node_gdf = node_gdf.drop_duplicates(subset=uniq_columns)
    logger.info("Removed %d duplicated nodes, kept %d", n_duplicated, len(node_gdf))
    
    # Add node_id
    node_path_re = re.compile(r'Node_(?P<countrycode>[A-Z]+)_\d+.geojson')
    node_gdf['countrycode_locode'] = node_gdf['locode'].apply(lambda x: x[:2])
    node_gdf['countrycode_path'] = node_gdf['path'].apply(
        lambda x: node_path_re.match(x).group('countrycode') if node_path_re.match(x) else None
    )
    node_gdf['countrycode'] = node_gdf['countrycode_locode']
    node_gdf['node_id'] = node_gdf.apply(lambda row: f"{row['countrycode']}_{row['objectcode']}", axis=1)
    
    return node_gdf


def concat_sections(data_dir: pathlib.Path) -> gpd.GeoDataFrame:
    """Concatenate all EURIS section files into a single GeoDataFrame.
    
    Args:
        data_dir: Directory containing FairwaySection_*.geojson files.
        
    Returns:
        Concatenated GeoDataFrame with deduplicated sections.
    """
    section_paths = list(data_dir.glob('FairwaySection_*.geojson'))
    logger.info("Found %d section files", len(section_paths))
    
    if not section_paths:
        raise FileNotFoundError(f"No FairwaySection_*.geojson files found in {data_dir}")
    
    section_gdfs = []
    for section_path in tqdm(section_paths, desc="Reading section files"):
        gdf = gpd.read_file(section_path)
        gdf['path'] = section_path.name
        section_gdfs.append(gdf)
    
    section_gdf = pd.concat(section_gdfs)
    
    # Deduplicate
    uniq_columns = set(section_gdf.columns) - {'path'}
    n_duplicated = section_gdf.duplicated(subset=uniq_columns).sum()
    section_gdf = section_gdf.drop_duplicates(subset=uniq_columns)
    logger.info("Removed %d duplicated sections, kept %d", n_duplicated, len(section_gdf))
    
    return section_gdf


def build_euris_graph(
    node_gdf: gpd.GeoDataFrame,
    section_gdf: gpd.GeoDataFrame,
) -> nx.Graph:
    """Build EURIS network graph from nodes and sections.
    
    Args:
        node_gdf: Node GeoDataFrame with node_id column.
        section_gdf: Section GeoDataFrame with code column.
        
    Returns:
        NetworkX graph with nodes and edges.
    """
    logger.info("Building node-section administration...")
    node_section = section_gdf[['code']].merge(
        node_gdf[['sectionref', 'node_id']],
        left_on='code',
        right_on='sectionref'
    )[['sectionref', 'node_id']]
    
    left_df = node_section.groupby('sectionref').first()
    right_df = node_section.groupby('sectionref').last()
    
    edge_df = pd.merge(left_df, right_df, left_index=True, right_index=True, suffixes=['_from', '_to'])
    edge_df = edge_df.rename(columns={"node_id_from": "source", "node_id_to": "target"})
    
    section_gdf = section_gdf.merge(edge_df.reset_index(), left_on='code', right_on='sectionref')
    
    logger.info("Building graph from %d sections...", len(section_gdf))
    graph = nx.from_pandas_edgelist(section_gdf, source='source', target='target', edge_attr=True)
    
    # Update node info
    logger.info("Updating node information for %d nodes...", len(node_gdf))
    for _, row in tqdm(node_gdf.iterrows(), total=len(node_gdf), desc="Updating nodes"):
        n = row['node_id']
        if n not in graph.nodes:
            continue
        node = graph.nodes[n]
        euris_nodes = node.get('euris_nodes', [])
        euris_nodes.append(row.to_dict())
        node['euris_nodes'] = euris_nodes
        node.update(row.to_dict())
    
    # Connect borders
    logger.info("Connecting border nodes...")
    border_node_gdf = node_gdf[~node_gdf['borderpoint'].isna()]
    border_locode_connections = pd.merge(
        border_node_gdf[['node_id', 'borderpoint']],
        node_gdf[['node_id', 'locode']],
        left_on='borderpoint',
        right_on='locode'
    )
    border_locode_connections = border_locode_connections.rename(
        columns={'node_id_x': 'source', 'node_id_y': 'target'}
    )
    logger.info("Found %d border connections", len(border_locode_connections))
    
    def geometry_for_border(row):
        source_geometry = graph.nodes[row['source']]['geometry']
        target_geometry = graph.nodes[row['target']]['geometry']
        return shapely.LineString([source_geometry, target_geometry])
    
    if len(border_locode_connections) > 0:
        border_locode_connections['geometry'] = border_locode_connections.apply(geometry_for_border, axis=1)
        border_graph = nx.from_pandas_edgelist(
            border_locode_connections, source='source', target='target', edge_attr=True
        )
        graph.add_edges_from(
            (e[0], e[1], attrs) for e, attrs in border_graph.edges.items()
        )
        for e in graph.edges:
            graph.edges[e]['is_border'] = e in border_graph.edges
    
    # Compute subgraphs
    logger.info("Computing subgraphs...")
    for i, component in enumerate(nx.connected_components(graph)):
        subgraph = graph.subgraph(component)
        for edge in subgraph.edges.values():
            edge['subgraph'] = i
        for node in subgraph.nodes.values():
            node['subgraph'] = i
    
    # Add length
    logger.info("Computing edge lengths...")
    geod = pyproj.Geod(ellps="WGS84")
    for edge in tqdm(graph.edges.values(), total=graph.number_of_edges(), desc="Edge lengths"):
        edge['length_m'] = geod.geometry_length(edge['geometry'])
    
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
        data=graph.edges.values(),
        index=graph.edges.keys()
    ).reset_index(names=['source', 'target'])
    edge_gdf = gpd.GeoDataFrame(edge_df, crs='EPSG:4326')
    edge_gdf.to_file(output_dir / "edges.geojson")
    edge_gdf.to_parquet(output_dir / "edges.geoparquet")
    
    # GeoJSON and GeoParquet nodes
    # For geoparquet, list/dict types are difficult, but we handle euris_nodes
    node_df = []
    for node_id, attrs in graph.nodes(data=True):
        row = {"n": node_id}
        for k, v in attrs.items():
            if k == 'euris_nodes' or isinstance(v, (list, dict)):
                continue
            row[k] = v
        node_df.append(row)
        
    node_gdf = gpd.GeoDataFrame(node_df, crs='EPSG:4326')
    node_gdf.to_file(output_dir / "nodes.geojson")
    node_gdf.to_parquet(output_dir / "nodes.geoparquet")
    
    # Summary
    import json
    summary = {
        "num_nodes": graph.number_of_nodes(),
        "num_edges": graph.number_of_edges(),
        "num_connected_components": nx.number_connected_components(graph),
    }
    with open(output_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    # Copy ris-index.gpkg
    import shutil
    ris_index_candidates = list(pathlib.Path("output/euris-export").glob("**/ris-index.gpkg"))
    if ris_index_candidates:
        shutil.copy2(ris_index_candidates[0], output_dir / "ris-index.gpkg")
        logger.info("Copied ris-index.gpkg to %s", output_dir)
    else:
        logger.warning("Could not find ris-index.gpkg to copy.")
        
    logger.info("Exported EURIS graph to %s", output_dir)
