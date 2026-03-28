import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString, Point
from fis.dropins.core import (
    _map_dropins_to_sections,
)
from fis.dropins.splicing import splice_fairways
from fis.dropins.terminals import generate_terminal_graph_features


@pytest.fixture
def sample_data():
    # 1. Fairway Section: 1km straight line in EPSG:28992 (RD New)
    # Start: (100000, 400000), End: (101000, 400000)
    # Convert to 4326 for the "source" data
    line_rd = LineString([(100000, 400000), (101000, 400000)])
    line_4326 = gpd.GeoSeries([line_rd], crs="EPSG:28992").to_crs("EPSG:4326").iloc[0]

    section = {
        "id": "sec_1",
        "fairway_id": "fw_1",
        "Name": "Test Section",
        "StartJunctionId": "junc_start",
        "EndJunctionId": "junc_end",
        "geometry": line_4326.wkt,
    }
    sections_df = pd.DataFrame([section])

    # 2. Terminals
    # Close Terminal: 10m away from the middle of the section (500m along)
    close_pt_rd = Point(100500, 400010)
    close_pt_4326 = (
        gpd.GeoSeries([close_pt_rd], crs="EPSG:28992").to_crs("EPSG:4326").iloc[0]
    )

    term_close = {
        "Id": "term_close",
        "id": "term_close",
        "Name": "Close Terminal",
        "FairwaySectionId": "sec_1",
        "FairwayId": "fw_1",
        "geometry": close_pt_4326.wkt,
    }

    # Distant Terminal: 2km away from the section
    far_pt_rd = Point(100500, 402000)
    far_pt_4326 = (
        gpd.GeoSeries([far_pt_rd], crs="EPSG:28992").to_crs("EPSG:4326").iloc[0]
    )

    term_far = {
        "Id": "term_far",
        "id": "term_far",
        "Name": "Far Terminal",
        "FairwaySectionId": "sec_1",
        "FairwayId": "fw_1",
        "geometry": far_pt_4326.wkt,
    }

    return sections_df, [term_close, term_far]


def test_terminal_integration_flow(sample_data):
    sections_df, terminals = sample_data

    # 1. Map dropins to sections
    dropins_by_section = _map_dropins_to_sections([], [], terminals)
    assert "sec_1" in dropins_by_section
    assert len(dropins_by_section["sec_1"]) == 2
    assert dropins_by_section["sec_1"][0]["type"] == "terminal"

    # 2. Splice fairways
    # This should split the section at two points
    splice_fairways(sections_df, dropins_by_section, {})

    # Since both terminals are at the same projected distance (500m),
    # the splicer logic might handle them as one or very close.
    # Actually, they are exactly at 500m. Let's see how FairwaySplicer handles exact same distance.
    # EPSILON is 0.001.

    # Check if terminals now have connection_geometry
    assert "connection_geometry" in terminals[0]
    assert "connection_geometry" in terminals[1]

    # 3. Generate terminal specific features
    terminal_features = generate_terminal_graph_features(terminals)

    # Expect for EACH terminal: 1 connection node, 1 terminal node, 1 access edge = 6 total
    assert len(terminal_features) == 6

    # Verify access edges
    access_edges = [
        f
        for f in terminal_features
        if f["properties"].get("segment_type") == "terminal_access"
    ]
    assert len(access_edges) == 2

    # Verify lengths (approximate)
    close_edge = next(
        e for e in access_edges if e["properties"]["terminal_id"] == "term_close"
    )
    far_edge = next(
        e for e in access_edges if e["properties"]["terminal_id"] == "term_far"
    )

    assert close_edge["properties"]["length_m"] == pytest.approx(10.0, abs=0.5)
    assert far_edge["properties"]["length_m"] == pytest.approx(2000.0, abs=10.0)

    # Verify node types
    terminal_nodes = [
        f for f in terminal_features if f["properties"].get("node_type") == "terminal"
    ]
    assert len(terminal_nodes) == 2
    assert any(n["properties"]["name"] == "Close Terminal" for n in terminal_nodes)
    assert any(n["properties"]["name"] == "Far Terminal" for n in terminal_nodes)


def test_terminal_missing_section_id(sample_data):
    sections_df, terminals = sample_data
    # Remove section ID from one terminal
    terminals[0]["FairwaySectionId"] = None

    with pytest.raises(
        ValueError, match="has no FairwaySectionId and cannot be spliced."
    ):
        _map_dropins_to_sections([], [], terminals)
