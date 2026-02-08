"""EURIS graph enrichment functions.

Adds sailing speed attributes to EURIS graph edges.
"""

import glob
import logging
import pathlib

import geopandas as gpd
import networkx as nx
import pandas as pd

logger = logging.getLogger(__name__)


def load_euris_sailing_speed(euris_export_dir: pathlib.Path) -> gpd.GeoDataFrame:
    """Load and combine all SailingSpeed files from EURIS export.
    
    Args:
        euris_export_dir: Path to euris-export directory.
        
    Returns:
        Combined GeoDataFrame with sailing speed data.
    """
    pattern = str(euris_export_dir / "SailingSpeed_*.geojson")
    files = glob.glob(pattern)
    
    if not files:
        logger.warning("No SailingSpeed files found in %s", euris_export_dir)
        return gpd.GeoDataFrame()
    
    gdfs = []
    for f in files:
        gdf = gpd.read_file(f)
        gdf['country'] = pathlib.Path(f).stem.split('_')[1]
        gdfs.append(gdf)
        logger.info("Loaded %d sailing speed records from %s", len(gdf), f)
    
    combined = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
    logger.info("Combined %d sailing speed records", len(combined))
    return combined


def enrich_euris_with_speed(
    graph: nx.Graph,
    sailing_speed: gpd.GeoDataFrame,
) -> nx.Graph:
    """Add sailing speed attributes to EURIS graph edges via sectionref.
    
    Args:
        graph: EURIS networkx graph.
        sailing_speed: SailingSpeed GeoDataFrame with sectionref column.
        
    Returns:
        Enriched graph with maxspeed on edges.
    """
    if sailing_speed.empty or 'sectionref' not in sailing_speed.columns:
        logger.warning("No sailing speed data or missing sectionref column")
        return graph
    
    speed_cols = ['maxspeed', 'calspeed', 'direction', 'shipcategory']
    available_cols = [c for c in speed_cols if c in sailing_speed.columns]
    
    speed_lookup = (
        sailing_speed[['sectionref'] + available_cols]
        .dropna(subset=['sectionref'])
        .drop_duplicates('sectionref')
        .set_index('sectionref')
        .rename(columns={c: f'speed_{c}' for c in available_cols})
    )
    
    logger.info("Built speed lookup with %d sectionref entries", len(speed_lookup))
    
    enriched_count = 0
    for u, v, data in graph.edges(data=True):
        ref = data.get('sectionref')
        if ref and ref in speed_lookup.index:
            attrs = speed_lookup.loc[ref].dropna().to_dict()
            data.update(attrs)
            enriched_count += 1
    
    logger.info("Enriched %d edges with sailing speed", enriched_count)
    return graph
