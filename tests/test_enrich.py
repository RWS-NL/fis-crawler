"""Unit tests for FIS graph enrichment functions."""

import pandas as pd
import geopandas as gpd
import networkx as nx
import pytest
from shapely.geometry import LineString

from fis.graph.enrich import (
    match_by_geometry,
    match_by_route_km,
    build_fis_edge_enrichments,
    enrich_fis_graph,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_sections():
    """Create sample sections GeoDataFrame."""
    return gpd.GeoDataFrame(
        {
            "Id": [1, 2, 3],
            "Name": ["Section A", "Section B", "Section C"],
            "RouteId": [100, 100, 200],
            "FairwayId": [100, 100, 200],
            "RouteKmBegin": [0.0, 5.0, 0.0],
            "RouteKmEnd": [5.0, 10.0, 8.0],
            "StartJunctionId": [1001, 1002, 1003],
            "EndJunctionId": [1002, 1004, 1005],
            "geometry": [
                LineString([(0, 0), (1, 0)]),
                LineString([(1, 0), (2, 0)]),
                LineString([(0, 1), (1, 1)]),
            ],
        },
        crs="EPSG:4326",
    )


@pytest.fixture
def sample_maxdim(sample_sections):
    """Create sample maximumdimensions GeoDataFrame matching section geometries."""
    return gpd.GeoDataFrame(
        {
            "Id": [101, 102],
            "GeneralDepth": [3.5, 2.0],
            "GeneralWidth": [40.0, 25.0],
            "WidePushedWidth": [15.0, 10.0],
            "Note": ["Some Note", None],
            "geometry": [
                sample_sections.iloc[0].geometry,  # Matches section 1
                sample_sections.iloc[2].geometry,  # Matches section 3
            ],
        },
        crs="EPSG:4326",
    )


@pytest.fixture
def sample_navigability(sample_sections):
    """Create sample navigability GeoDataFrame."""
    return gpd.GeoDataFrame(
        {
            "Id": [201],
            "Classification": ["CEMT"],
            "Code": ["IV"],
            "Description": ["Class IV waterway"],
            "geometry": [sample_sections.iloc[0].geometry],  # Matches section 1
        },
        crs="EPSG:4326",
    )


@pytest.fixture
def sample_speed():
    """Create sample navigationspeed GeoDataFrame."""
    return gpd.GeoDataFrame(
        {
            "Id": [301, 302],
            "RouteId": [100, 100],
            "RouteKmBegin": [0.0, 6.0],
            "RouteKmEnd": [4.0, 10.0],
            "Speed": [12.0, 8.0],
            "MaxSpeedUp": [15.0, 10.0],
            "MaxSpeedConvoyUp": [10.0, 6.0],
            "geometry": [
                LineString([(0, 0), (0.5, 0)]),
                LineString([(0.5, 0), (1, 0)]),
            ],
        },
        crs="EPSG:4326",
    )


@pytest.fixture
def sample_status():
    """Create sample fairwaystatus GeoDataFrame."""
    return gpd.GeoDataFrame(
        {
            "Id": [401],
            "RouteId": [100],
            "RouteKmBegin": [0.0],
            "RouteKmEnd": [5.0],
            "TrajectCode": ["TR01"],
            "StatusCode": ["ACT"],
            "StatusDescription": ["Active"],
            "geometry": [LineString([(0, 0), (0.5, 0)])],
        },
        crs="EPSG:4326",
    )


@pytest.fixture
def sample_mgd():
    """Create sample mgdtrajectory GeoDataFrame."""
    return gpd.GeoDataFrame(
        {
            "Id": [501],
            "RouteId": [100],
            "RouteKmBegin": [0.0],
            "RouteKmEnd": [10.0],
            "FromTo": ["A to B"],
            "geometry": [LineString([(0, 0), (1, 0)])],
        },
        crs="EPSG:4326",
    )


@pytest.fixture
def sample_fairway():
    """Create sample fairway GeoDataFrame."""
    return gpd.GeoDataFrame(
        {"Id": [100], "FairwayNumber": ["FW123"], "geometry": [None]},
        crs="EPSG:4326",
    )


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
            columns=["GeneralDepth", "GeneralWidth"],
            prefix="dim_",
        )

        assert "dim_GeneralDepth" in result.columns
        assert "dim_GeneralWidth" in result.columns
        assert result.loc[1, "dim_GeneralDepth"] == 3.5  # Section 1
        assert result.loc[3, "dim_GeneralWidth"] == 25.0  # Section 3
        assert pd.isna(result.loc[2, "dim_GeneralDepth"])  # Section 2 not matched

    def test_handles_empty_data(self, sample_sections):
        """Should raise ValueError for empty input data."""
        empty_gdf = gpd.GeoDataFrame()
        with pytest.raises(
            ValueError, match="Data provided for test_ geometry matching is empty."
        ):
            match_by_geometry(sample_sections, empty_gdf, ["SomeCol"], "test_")

    def test_handles_missing_data(self, sample_sections):
        """Should return empty DataFrame for missing (None) input data."""
        result = match_by_geometry(sample_sections, None, ["SomeCol"], "test_")
        assert len(result) == len(sample_sections)
        assert len(result.columns) == 0

    def test_handles_missing_columns(self, sample_sections, sample_maxdim):
        """Should gracefully handle missing columns."""
        result = match_by_geometry(
            sample_sections,
            sample_maxdim,
            columns=["NonExistentColumn"],
            prefix="missing_",
        )

        assert result.empty or "missing_NonExistentColumn" not in result.columns


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
            columns=["Speed", "MaxSpeedUp"],
            prefix="speed_",
        )

        # Section 1 (km 0-5) should match speed record (km 0-4)
        assert result.loc[1, "speed_Speed"] == 12.0
        assert result.loc[1, "speed_MaxSpeedUp"] == 15.0

        # Section 2 (km 5-10) should match speed record (km 6-10)
        assert result.loc[2, "speed_Speed"] == 8.0

    def test_no_match_different_route(self, sample_sections, sample_speed):
        """Should not match when RouteId differs."""
        result = match_by_route_km(
            sample_sections,
            sample_speed,
            columns=["Speed"],
            prefix="speed_",
        )

        # Section 3 is on RouteId 200, speed data is on RouteId 100
        assert pd.isna(result.loc[3, "speed_Speed"])

    def test_handles_empty_data(self, sample_sections):
        """Should raise ValueError for empty input data."""
        empty_gdf = gpd.GeoDataFrame()
        with pytest.raises(
            ValueError, match="Data provided for speed_ route/km matching is empty."
        ):
            match_by_route_km(sample_sections, empty_gdf, ["Speed"], "speed_")

    def test_handles_missing_data(self, sample_sections):
        """Should return empty DataFrame for missing (None) input data."""
        result = match_by_route_km(sample_sections, None, ["Speed"], "speed_")
        assert len(result) == len(sample_sections)
        assert len(result.columns) == 0


# =============================================================================
# Tests for build_fis_edge_enrichments
# =============================================================================


class TestBuildEdgeEnrichments:
    """Tests for combined enrichment building."""

    def test_combines_multiple_sources(
        self,
        sample_sections,
        sample_maxdim,
        sample_navigability,
        sample_speed,
        sample_status,
        sample_mgd,
        sample_fairway,
    ):
        """Should combine enrichment from multiple data sources."""
        # Create minimal non-empty GDFs for other required datasets to satisfy strictness
        # We use a non-matching RouteId so they don't affect results but don't trigger empty error
        dummy_gdf = gpd.GeoDataFrame(
            {
                "Id": [999],
                "RouteId": [999],
                "RouteKmBegin": [0.0],
                "RouteKmEnd": [1.0],
                "geometry": [LineString([(10, 10), (11, 11)])],
            },
            crs="EPSG:4326",
        )

        datasets = {
            "section": sample_sections,
            "maximumdimensions": sample_maxdim,
            "navigability": sample_navigability,
            "navigationspeed": sample_speed,
            "fairwaydepth": dummy_gdf,
            "fairwaytype": dummy_gdf,
            "tidalarea": dummy_gdf,
            "fairwayclassification": dummy_gdf,
            "fairwaystatus": sample_status,
            "mgdtrajectory": sample_mgd,
            "fairway": sample_fairway,
        }

        result = build_fis_edge_enrichments(datasets)

        # Check dimensions (canonical)
        assert "dim_depth" in result.columns
        assert result.loc[1, "dim_depth"] == 3.5

        # Check CEMT class (canonical)
        assert "cemt_class" in result.columns
        assert result.loc[1, "cemt_class"] == "IV"

        # Check speed (canonical)
        assert "maxspeed" in result.columns
        assert result.loc[1, "maxspeed"] == 12.0
        assert "maxspeed_convoy_up" in result.columns
        assert result.loc[1, "maxspeed_convoy_up"] == 10.0

        # Check wide pushed (canonical)
        assert "dim_wide_pushed_width" in result.columns
        assert result.loc[1, "dim_wide_pushed_width"] == 15.0

        # Check note (canonical)
        assert "dim_note" in result.columns
        assert result.loc[1, "dim_note"] == "Some Note"

        # Check status (canonical)
        assert "status_description" in result.columns
        assert result.loc[1, "status_description"] == "Active"

        # Check MGD (canonical)
        assert "mgd_from_to" in result.columns
        assert result.loc[1, "mgd_from_to"] == "A to B"

        # Check fairway number (canonical)
        assert "fairway_number" in result.columns
        assert result.loc[1, "fairway_number"] == "FW123"

    def test_handles_missing_optional_datasets(self, sample_sections):
        """Should handle cases where optional datasets are missing from the input dict."""
        datasets = {
            "section": sample_sections,
            # All others missing (None)
        }

        # This should not raise KeyError or ValueError
        result = build_fis_edge_enrichments(datasets)

        assert len(result) == len(sample_sections)
        # Check that expected canonical columns are NOT present (as they were never created)
        assert "dim_depth" not in result.columns
        # Only fairway_number is explicitly created even if dataset is missing
        assert "fairway_number" in result.columns
        assert result["fairway_number"].isna().all()


# =============================================================================
# Tests for enrich_fis_graph
# =============================================================================


class TestEnrichFisGraph:
    """Tests for graph enrichment."""

    def test_enriches_matching_edges(self, sample_graph, sample_sections):
        """Should add enrichment attributes to graph edges."""
        edge_enrichments = pd.DataFrame(
            {
                "dim_depth": [3.5, None, 2.0],
                "cemt_class": ["IV", "III", None],
            },
            index=[1, 2, 3],
        )

        result = enrich_fis_graph(
            sample_graph, sample_sections, edge_enrichments=edge_enrichments
        )

        # Edge 1001-1002 (section 1)
        assert result[1001][1002]["dim_depth"] == 3.5
        assert result[1001][1002]["cemt_class"] == "IV"

        # Edge 1002-1004 (section 2)
        assert result[1002][1004]["cemt_class"] == "III"

    def test_handles_missing_sections(self, sample_graph, sample_sections):
        """Should handle edges not matching any section."""
        G = nx.Graph()
        G.add_edge(9999, 9998)  # Non-existent junction IDs

        edge_enrichments = pd.DataFrame({"cemt_class": ["IV"]}, index=[1])
        result = enrich_fis_graph(G, sample_sections, edge_enrichments=edge_enrichments)

        # Should not crash, edge should have no enrichment
        assert "cemt_class" not in result[9999][9998]

    def test_enriches_nodes_with_locode(self, sample_graph, sample_sections):
        """Should add locode to graph nodes from routejunction data."""
        node_enrichments = {
            "routejunction": pd.DataFrame(
                {
                    "SectionJunctionId": [1001, 1002],
                    "Code": ["NLRTM...", "NLAMS..."],
                }
            )
        }
        edge_enrichments = pd.DataFrame(index=[1, 2, 3])

        result = enrich_fis_graph(
            sample_graph,
            sample_sections,
            edge_enrichments=edge_enrichments,
            node_enrichments=node_enrichments,
        )

        assert result.nodes[1001]["locode"] == "NLRTM..."
        assert result.nodes[1002]["locode"] == "NLAMS..."
        assert "locode" not in result.nodes[1004]
