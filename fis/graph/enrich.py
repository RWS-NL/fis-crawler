"""Enrichment functions for adding attributes to graphs."""

import glob
import logging
import pathlib
from typing import Dict, List

import geopandas as gpd
import networkx as nx

logger = logging.getLogger(__name__)


def load_sailing_speed(euris_export_dir: pathlib.Path) -> gpd.GeoDataFrame:
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
        # Extract country code from filename
        country = pathlib.Path(f).stem.split('_')[1]
        gdf['country'] = country
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
    
    # Build lookup by sectionref
    speed_by_ref = {}
    for _, row in sailing_speed.iterrows():
        ref = row.get('sectionref')
        if ref:
            speed_by_ref[ref] = {
                'speed_maxspeed': row.get('maxspeed'),
                'speed_calspeed': row.get('calspeed'),
                'speed_direction': row.get('direction'),
                'speed_shipcategory': row.get('shipcategory'),
            }
    
    logger.info("Built speed lookup with %d sectionref entries", len(speed_by_ref))
    
    # Match edges by sectionref
    enriched_count = 0
    for u, v, data in graph.edges(data=True):
        ref = data.get('sectionref')
        if ref and ref in speed_by_ref:
            data.update(speed_by_ref[ref])
            enriched_count += 1
    
    logger.info("Enriched %d edges with sailing speed", enriched_count)
    return graph


# Need pandas for concat
import pandas as pd
