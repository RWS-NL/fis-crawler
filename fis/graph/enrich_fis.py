"""FIS graph enrichment functions.

Adds attributes from maximumdimensions, navigability, navigationspeed,
fairwaydepth, fairwaytype, and tidalarea to FIS graph edges.
"""

import logging
import pathlib

import geopandas as gpd
import networkx as nx
import pandas as pd

logger = logging.getLogger(__name__)


def load_fis_enrichment_data(export_dir: pathlib.Path) -> dict[str, gpd.GeoDataFrame]:
    """Load all FIS enrichment datasets.
    
    Args:
        export_dir: Path to fis-export directory.
        
    Returns:
        Dict of dataset name to GeoDataFrame.
    """
    datasets = {}
    required = ['section', 'maximumdimensions', 'navigability']
    optional = ['navigationspeed', 'fairwaydepth', 'fairwaytype', 'tidalarea']
    
    for name in required + optional:
        path = export_dir / f"{name}.geoparquet"
        if path.exists():
            datasets[name] = gpd.read_parquet(path)
            logger.info("Loaded %s: %d records", name, len(datasets[name]))
        elif name in required:
            raise FileNotFoundError(f"Required file not found: {path}")
        else:
            logger.warning("Optional file not found: %s", path)
    
    return datasets


def match_by_geometry(
    sections: gpd.GeoDataFrame,
    data: gpd.GeoDataFrame,
    columns: list[str],
    prefix: str,
) -> pd.DataFrame:
    """Match data to sections by exact geometry WKT.
    
    Args:
        sections: Sections GeoDataFrame with Id column.
        data: Data GeoDataFrame to match.
        columns: Columns to extract from data.
        prefix: Prefix to add to column names.
        
    Returns:
        DataFrame indexed by section Id with prefixed columns.
    """
    if data is None or data.empty:
        return pd.DataFrame(index=sections['Id'])
    
    available = [c for c in columns if c in data.columns]
    if not available:
        return pd.DataFrame(index=sections['Id'])
    
    # Use geometry WKT as join key
    sections = sections.copy()
    sections['_geom_key'] = sections.geometry.apply(lambda g: g.wkt)
    
    data = data.copy()
    data['_geom_key'] = data.geometry.apply(lambda g: g.wkt)
    
    # Select and deduplicate
    data_select = data[['_geom_key'] + available].drop_duplicates('_geom_key')
    data_select = data_select.rename(columns={c: f'{prefix}{c}' for c in available})
    
    # Join
    result = sections[['Id', '_geom_key']].merge(
        data_select, on='_geom_key', how='left'
    ).drop(columns=['_geom_key']).set_index('Id')
    
    matched = result.notna().any(axis=1).sum()
    logger.info("Matched %d sections by geometry for %s", matched, prefix)
    
    return result


def match_by_route_km(
    sections: gpd.GeoDataFrame,
    data: gpd.GeoDataFrame,
    columns: list[str],
    prefix: str,
) -> pd.DataFrame:
    """Match data to sections by RouteId and overlapping km ranges.
    
    Uses range overlap: section [km_begin, km_end] overlaps data [km_begin, km_end]
    when they share the same RouteId.
    
    Args:
        sections: Sections with RouteId, RouteKmBegin, RouteKmEnd.
        data: Data with same columns.
        columns: Columns to extract.
        prefix: Prefix for output columns.
        
    Returns:
        DataFrame indexed by section Id with prefixed columns.
    """
    if data is None or data.empty:
        return pd.DataFrame(index=sections['Id'])
    
    # Check required columns
    required = ['RouteId', 'RouteKmBegin', 'RouteKmEnd']
    for col in required:
        if col not in sections.columns or col not in data.columns:
            logger.warning("Missing %s column for route/km matching", col)
            return pd.DataFrame(index=sections['Id'])
    
    available = [c for c in columns if c in data.columns]
    if not available:
        return pd.DataFrame(index=sections['Id'])
    
    # Build section index
    sections = sections.copy()
    sections = sections.dropna(subset=['RouteId', 'RouteKmBegin', 'RouteKmEnd'])
    
    data = data.copy()
    data = data.dropna(subset=['RouteId', 'RouteKmBegin', 'RouteKmEnd'])
    
    # Group data by RouteId for efficient lookup
    data_by_route = data.groupby('RouteId')
    
    results = []
    for _, section in sections.iterrows():
        section_id = section['Id']
        route_id = section['RouteId']
        s_begin = min(section['RouteKmBegin'], section['RouteKmEnd'])
        s_end = max(section['RouteKmBegin'], section['RouteKmEnd'])
        
        if route_id not in data_by_route.groups:
            continue
        
        route_data = data_by_route.get_group(route_id)
        
        # Find overlapping records
        for _, row in route_data.iterrows():
            d_begin = min(row['RouteKmBegin'], row['RouteKmEnd'])
            d_end = max(row['RouteKmBegin'], row['RouteKmEnd'])
            
            # Overlap check: ranges overlap if not (s_end < d_begin or d_end < s_begin)
            if not (s_end < d_begin or d_end < s_begin):
                result_row = {'Id': section_id}
                for col in available:
                    result_row[f'{prefix}{col}'] = row[col]
                results.append(result_row)
                break  # Take first match
    
    if not results:
        return pd.DataFrame(index=sections['Id'])
    
    result_df = pd.DataFrame(results).drop_duplicates('Id').set_index('Id')
    
    # Reindex to include all section IDs
    all_ids = sections['Id'].unique()
    result_df = result_df.reindex(all_ids)
    
    matched = result_df.notna().any(axis=1).sum()
    logger.info("Matched %d sections by route/km for %s", matched, prefix)
    
    return result_df


def build_section_enrichment(datasets: dict[str, gpd.GeoDataFrame]) -> pd.DataFrame:
    """Build enrichment lookup by joining all datasets to sections.
    
    Args:
        datasets: Dict of dataset name to GeoDataFrame.
        
    Returns:
        DataFrame indexed by section Id with all enrichment columns.
    """
    sections = datasets['section']
    
    # Geometry-based matching
    maxdim_cols = [
        'GeneralDepth', 'GeneralLength', 'GeneralWidth', 'GeneralHeight',
        'SeaFairingDepth', 'SeaFairingLength', 'SeaFairingWidth', 'SeaFairingHeight',
        'PushedDepth', 'PushedLength', 'PushedWidth',
        'CoupledDepth', 'CoupledLength', 'CoupledWidth',
    ]
    maxdim_df = match_by_geometry(
        sections, datasets.get('maximumdimensions'), maxdim_cols, 'dim_'
    )
    
    nav_cols = ['Classification', 'Code', 'Description']
    nav_df = match_by_geometry(
        sections, datasets.get('navigability'), nav_cols, 'nav_'
    )
    # Add cemt_class alias
    if 'nav_Code' in nav_df.columns:
        nav_df['cemt_class'] = nav_df['nav_Code']
    
    # Route/km-based matching
    speed_cols = ['Speed', 'MaxSpeedUp', 'MaxSpeedDown', 'CalibratedSpeedUp', 'CalibratedSpeedDown']
    speed_df = match_by_route_km(
        sections, datasets.get('navigationspeed'), speed_cols, 'speed_'
    )
    
    depth_cols = ['MinimalDepthLowerLimit', 'MinimalDepthUpperLimit', 'ReferenceLevel']
    depth_df = match_by_route_km(
        sections, datasets.get('fairwaydepth'), depth_cols, 'depth_'
    )
    
    type_cols = ['CharacterTypeCode']
    type_df = match_by_route_km(
        sections, datasets.get('fairwaytype'), type_cols, 'type_'
    )
    
    # Tidal area - just mark as boolean
    tidal_df = match_by_route_km(
        sections, datasets.get('tidalarea'), ['Name'], 'tidal_'
    )
    if 'tidal_Name' in tidal_df.columns:
        tidal_df['is_tidal'] = tidal_df['tidal_Name'].notna()
        tidal_df = tidal_df.drop(columns=['tidal_Name'])
    
    # Combine all enrichment
    enrichment = pd.concat([maxdim_df, nav_df, speed_df, depth_df, type_df, tidal_df], axis=1)
    
    # Summary stats
    for prefix, desc in [('dim_', 'dimensions'), ('cemt_', 'CEMT'), ('speed_', 'speed'), 
                         ('depth_', 'depth'), ('type_', 'type'), ('is_tidal', 'tidal')]:
        cols = [c for c in enrichment.columns if c.startswith(prefix)]
        if cols:
            count = enrichment[cols].notna().any(axis=1).sum()
            logger.info("Total sections with %s: %d", desc, count)
    
    return enrichment


def enrich_fis_graph(
    graph: nx.Graph,
    sections: gpd.GeoDataFrame,
    enrichment: pd.DataFrame,
) -> nx.Graph:
    """Add enrichment attributes to FIS graph edges.
    
    Args:
        graph: FIS networkx graph (nodes are junction IDs).
        sections: Sections GeoDataFrame with junction ID columns.
        enrichment: DataFrame indexed by section Id with enrichment attrs.
        
    Returns:
        Graph with enriched edge attributes.
    """
    # Build edge → section mapping
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
    
    # Apply enrichment
    enriched_count = 0
    for u, v, data in graph.edges(data=True):
        section_id = edge_to_section.get((u, v))
        if section_id is not None and section_id in enrichment.index:
            attrs = enrichment.loc[section_id].dropna().to_dict()
            data.update(attrs)
            if attrs:
                enriched_count += 1
    
    logger.info("Enriched %d / %d edges", enriched_count, graph.number_of_edges())
    return graph
