"""Enrichment functions for adding attributes to graphs.

Uses vectorized pandas operations for performance and clarity.
"""

import glob
import logging
import pathlib
from typing import Optional

import geopandas as gpd
import networkx as nx
import pandas as pd

logger = logging.getLogger(__name__)


# =============================================================================
# FIS Enrichment
# =============================================================================

def load_fis_enrichment_data(
    export_dir: pathlib.Path,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Load FIS enrichment data: sections, maximumdimensions, navigability.
    
    Args:
        export_dir: Path to fis-export directory.
        
    Returns:
        Tuple of (sections, maxdim, navigability) GeoDataFrames.
    """
    sections = gpd.read_parquet(export_dir / "section.geoparquet")
    maxdim = gpd.read_parquet(export_dir / "maximumdimensions.geoparquet")
    navigability = gpd.read_parquet(export_dir / "navigability.geoparquet")
    
    logger.info(
        "Loaded FIS data: %d sections, %d maximumdimensions, %d navigability",
        len(sections), len(maxdim), len(navigability)
    )
    
    return sections, maxdim, navigability


def build_section_enrichment(
    sections: gpd.GeoDataFrame,
    maxdim: gpd.GeoDataFrame,
    navigability: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Build enrichment lookup by joining sections with attributes.
    
    Uses geometry-based matching (since FIS uses same geometries for related data).
    
    Args:
        sections: Sections GeoDataFrame.
        maxdim: Maximum dimensions GeoDataFrame.
        navigability: Navigability (CEMT classification) GeoDataFrame.
        
    Returns:
        DataFrame indexed by section Id with enrichment columns.
    """
    # Use geometry WKT as join key (FIS uses exact matching geometries)
    sections = sections.copy()
    sections['_geom_key'] = sections.geometry.apply(lambda g: g.wkt)
    
    # Prepare maximumdimensions columns
    dim_cols = [
        'GeneralDepth', 'GeneralLength', 'GeneralWidth', 'GeneralHeight',
        'SeaFairingDepth', 'SeaFairingLength', 'SeaFairingWidth', 'SeaFairingHeight',
        'PushedDepth', 'PushedLength', 'PushedWidth',
        'CoupledDepth', 'CoupledLength', 'CoupledWidth',
    ]
    dim_available = [c for c in dim_cols if c in maxdim.columns]
    
    maxdim = maxdim.copy()
    maxdim['_geom_key'] = maxdim.geometry.apply(lambda g: g.wkt)
    maxdim_select = maxdim[['_geom_key'] + dim_available].drop_duplicates('_geom_key')
    # Prefix columns
    maxdim_select = maxdim_select.rename(columns={c: f'dim_{c}' for c in dim_available})
    
    # Prepare navigability columns  
    nav_cols = ['Classification', 'Code', 'Description']
    nav_available = [c for c in nav_cols if c in navigability.columns]
    
    navigability = navigability.copy()
    navigability['_geom_key'] = navigability.geometry.apply(lambda g: g.wkt)
    nav_select = navigability[['_geom_key'] + nav_available].drop_duplicates('_geom_key')
    # Prefix and add cemt_class alias
    nav_select = nav_select.rename(columns={c: f'nav_{c}' for c in nav_available})
    if 'nav_Code' in nav_select.columns:
        nav_select['cemt_class'] = nav_select['nav_Code']
    
    # Join enrichment to sections
    enriched = sections[['Id', '_geom_key']].merge(
        maxdim_select, on='_geom_key', how='left'
    ).merge(
        nav_select, on='_geom_key', how='left'
    ).drop(columns=['_geom_key'])
    
    # Report coverage
    dim_matched = enriched[[c for c in enriched.columns if c.startswith('dim_')]].notna().any(axis=1).sum()
    nav_matched = enriched['cemt_class'].notna().sum() if 'cemt_class' in enriched.columns else 0
    logger.info("Matched %d sections with dimensions, %d with navigability", dim_matched, nav_matched)
    
    return enriched.set_index('Id')


def enrich_fis_graph(
    graph: nx.Graph,
    sections: gpd.GeoDataFrame,
    enrichment: pd.DataFrame,
) -> nx.Graph:
    """Add enrichment attributes to FIS graph edges.
    
    Matches graph edges (which use junction IDs) to sections (which have
    StartJunctionId/EndJunctionId), then applies the enrichment attributes.
    
    Args:
        graph: FIS networkx graph (nodes are junction IDs).
        sections: Sections GeoDataFrame with junction ID columns.
        enrichment: DataFrame indexed by section Id with enrichment attrs.
        
    Returns:
        Graph with enriched edge attributes.
    """
    # Build edge → section mapping (using both directions for undirected)
    section_lookup = (
        sections[['Id', 'StartJunctionId', 'EndJunctionId']]
        .dropna(subset=['StartJunctionId', 'EndJunctionId'])
        .assign(
            start=lambda df: df['StartJunctionId'].astype(int),
            end=lambda df: df['EndJunctionId'].astype(int),
        )
    )
    
    edge_to_section = {
        **{(row.start, row.end): row.Id for row in section_lookup.itertuples()},
        **{(row.end, row.start): row.Id for row in section_lookup.itertuples()},
    }
    
    logger.info("Built edge-to-section mapping with %d entries", len(edge_to_section) // 2)
    
    # Apply enrichment to edges
    enriched_count = 0
    for u, v, data in graph.edges(data=True):
        section_id = edge_to_section.get((u, v))
        if section_id is not None and section_id in enrichment.index:
            attrs = enrichment.loc[section_id].dropna().to_dict()
            data.update(attrs)
            enriched_count += 1
    
    logger.info("Enriched %d / %d edges", enriched_count, graph.number_of_edges())
    return graph


# =============================================================================
# EURIS Enrichment  
# =============================================================================

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
    
    # Build lookup indexed by sectionref
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
    
    # Match edges by sectionref attribute
    enriched_count = 0
    for u, v, data in graph.edges(data=True):
        ref = data.get('sectionref')
        if ref and ref in speed_lookup.index:
            attrs = speed_lookup.loc[ref].dropna().to_dict()
            data.update(attrs)
            enriched_count += 1
    
    logger.info("Enriched %d edges with sailing speed", enriched_count)
    return graph
