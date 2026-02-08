"""Unit tests for FIS graph enrichment functions."""

import pandas as pd
import geopandas as gpd
import networkx as nx
import pytest
from shapely.geometry import LineString, Point

from fis.graph.enrich import (
    match_by_geometry,
    match_by_route_km,
    build_section_enrichment,
    enrich_fis_graph,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_sections():
    """Create sample sections GeoDataFrame."""
    return gpd.GeoDataFrame({
        'Id': [1, 2, 3],
        'Name': ['Section A', 'Section B', 'Section C'],
        'RouteId': [100, 100, 200],
        'RouteKmBegin': [0.0, 5.0, 0.0],
        'RouteKmEnd': [5.0, 10.0, 8.0],
        'StartJunctionId': [1001, 1002, 1003],
        'EndJunctionId': [1002, 1004, 1005],
        'geometry': [
            LineString([(0, 0), (1, 0)]),
            LineString([(1, 0), (2, 0)]),
            LineString([(0, 1), (1, 1)]),
        ],
    }, crs='EPSG:4326')


@pytest.fixture
def sample_maxdim(sample_sections):
    """Create sample maximumdimensions GeoDataFrame matching section geometries."""
    return gpd.GeoDataFrame({
        'Id': [101, 102],
        'GeneralDepth': [3.5, 2.0],
        'GeneralWidth': [40.0, 25.0],
        'geometry': [
            sample_sections.iloc[0].geometry,  # Matches section 1
            sample_sections.iloc[2].geometry,  # Matches section 3
        ],
    }, crs='EPSG:4326')


@pytest.fixture
def sample_navigability(sample_sections):
    """Create sample navigability GeoDataFrame."""
    return gpd.GeoDataFrame({
        'Id': [201],
        'Classification': ['CEMT'],
        'Code': ['IV'],
        'Description': ['Class IV waterway'],
        'geometry': [sample_sections.iloc[0].geometry],  # Matches section 1
    }, crs='EPSG:4326')


@pytest.fixture
def sample_speed():
    """Create sample navigationspeed GeoDataFrame."""
    return gpd.GeoDataFrame({
        'Id': [301, 302],
        'RouteId': [100, 100],
        'RouteKmBegin': [0.0, 6.0],
        'RouteKmEnd': [4.0, 10.0],
        'Speed': [12.0, 8.0],
        'MaxSpeedUp': [15.0, 10.0],
        'geometry': [
            LineString([(0, 0), (0.5, 0)]),
            LineString([(0.5, 0), (1, 0)]),
        ],
    }, crs='EPSG:4326')


@pytest.fixture
def sample_graph():
    """Create sample FIS graph."""
    G = nx.Graph()
    G.add_edge(1001, 1002, section_id=1)
    G.add_edge(1002, 1004, section_id=2)
    G.add_edge(1003, 1005, section_id=3)
    return G


# =============================================================================
# Tests for match_by_geometry
# =============================================================================

class TestMatchByGeometry:
    """Tests for geometry-based matching."""
    
    def test_matches_exact_geometry(self, sample_sections, sample_maxdim):
        """Should match data to sections by exact geometry WKT."""
        result = match_by_geometry(
            sample_sections,
            sample_maxdim,
            columns=['GeneralDepth', 'GeneralWidth'],
            prefix='dim_',
        )
        
        assert 'dim_GeneralDepth' in result.columns
        assert 'dim_GeneralWidth' in result.columns
        assert result.loc[1, 'dim_GeneralDepth'] == 3.5  # Section 1
        assert result.loc[3, 'dim_GeneralWidth'] == 25.0  # Section 3
        assert pd.isna(result.loc[2, 'dim_GeneralDepth'])  # Section 2 not matched
    
    def test_handles_empty_data(self, sample_sections):
        """Should return empty DataFrame for empty input data."""
        empty_gdf = gpd.GeoDataFrame()
        result = match_by_geometry(
            sample_sections, empty_gdf, ['SomeCol'], 'test_'
        )
        
        assert len(result) == len(sample_sections)
        assert result.empty or result.isna().all().all()
    
    def test_handles_missing_columns(self, sample_sections, sample_maxdim):
        """Should gracefully handle missing columns."""
        result = match_by_geometry(
            sample_sections,
            sample_maxdim,
            columns=['NonExistentColumn'],
            prefix='missing_',
        )
        
        assert result.empty or 'missing_NonExistentColumn' not in result.columns


# =============================================================================
# Tests for match_by_route_km
# =============================================================================

class TestMatchByRouteKm:
    """Tests for route/km-based matching."""
    
    def test_matches_overlapping_km_ranges(self, sample_sections, sample_speed):
        """Should match data to sections when km ranges overlap."""
        result = match_by_route_km(
            sample_sections,
            sample_speed,
            columns=['Speed', 'MaxSpeedUp'],
            prefix='speed_',
        )
        
        # Section 1 (km 0-5) should match speed record (km 0-4)
        assert result.loc[1, 'speed_Speed'] == 12.0
        assert result.loc[1, 'speed_MaxSpeedUp'] == 15.0
        
        # Section 2 (km 5-10) should match speed record (km 6-10)
        assert result.loc[2, 'speed_Speed'] == 8.0
    
    def test_no_match_different_route(self, sample_sections, sample_speed):
        """Should not match when RouteId differs."""
        result = match_by_route_km(
            sample_sections,
            sample_speed,
            columns=['Speed'],
            prefix='speed_',
        )
        
        # Section 3 is on RouteId 200, speed data is on RouteId 100
        assert pd.isna(result.loc[3, 'speed_Speed'])
    
    def test_handles_empty_data(self, sample_sections):
        """Should return empty DataFrame for empty input data."""
        empty_gdf = gpd.GeoDataFrame()
        result = match_by_route_km(
            sample_sections, empty_gdf, ['Speed'], 'speed_'
        )
        
        assert len(result) == len(sample_sections)


# =============================================================================
# Tests for build_section_enrichment
# =============================================================================

class TestBuildSectionEnrichment:
    """Tests for combined enrichment building."""
    
    def test_combines_multiple_sources(self, sample_sections, sample_maxdim, 
                                        sample_navigability, sample_speed):
        """Should combine enrichment from multiple data sources."""
        datasets = {
            'section': sample_sections,
            'maximumdimensions': sample_maxdim,
            'navigability': sample_navigability,
            'navigationspeed': sample_speed,
        }
        
        result = build_section_enrichment(datasets)
        
        # Check dimensions
        assert 'dim_GeneralDepth' in result.columns
        assert result.loc[1, 'dim_GeneralDepth'] == 3.5
        
        # Check CEMT class
        assert 'cemt_class' in result.columns
        assert result.loc[1, 'cemt_class'] == 'IV'
        
        # Check speed
        assert 'speed_Speed' in result.columns
        assert result.loc[1, 'speed_Speed'] == 12.0


# =============================================================================
# Tests for enrich_fis_graph
# =============================================================================

class TestEnrichFisGraph:
    """Tests for graph enrichment."""
    
    def test_enriches_matching_edges(self, sample_graph, sample_sections):
        """Should add enrichment attributes to graph edges."""
        enrichment = pd.DataFrame({
            'dim_GeneralDepth': [3.5, None, 2.0],
            'cemt_class': ['IV', 'III', None],
        }, index=[1, 2, 3])
        
        result = enrich_fis_graph(sample_graph, sample_sections, enrichment)
        
        # Edge 1001-1002 (section 1)
        assert result[1001][1002]['dim_GeneralDepth'] == 3.5
        assert result[1001][1002]['cemt_class'] == 'IV'
        
        # Edge 1002-1004 (section 2)
        assert result[1002][1004]['cemt_class'] == 'III'
    
    def test_handles_missing_sections(self, sample_sections):
        """Should handle edges not matching any section."""
        G = nx.Graph()
        G.add_edge(9999, 9998)  # Non-existent junction IDs
        
        enrichment = pd.DataFrame({'cemt_class': ['IV']}, index=[1])
        result = enrich_fis_graph(G, sample_sections, enrichment)
        
        # Should not crash, edge should have no enrichment
        assert 'cemt_class' not in result[9999][9998]
