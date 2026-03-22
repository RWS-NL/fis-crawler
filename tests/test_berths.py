import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString, Point
from fis.dropins.core import (
    _map_dropins_to_sections,
)
from fis.dropins.splicing import splice_fairways
from fis.dropins.graph import generate_berth_graph_features


@pytest.fixture
def sample_berth_data():
    # Fairway Section: 1km straight line in EPSG:28992 (RD New)
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

    # Berth: 20m away from the middle of the section (500m along)
    pt_rd = Point(100500, 400020)
    pt_4326 = gpd.GeoSeries([pt_rd], crs="EPSG:28992").to_crs("EPSG:4326").iloc[0]

    berth = {
        "Id": "berth_123",
        "Name": "Test Berth",
        "FairwaySectionId": "sec_1",
        "FairwayId": "fw_1",
        "IsrsId": "NLXXX00123",
        "geometry": pt_4326.wkt,
    }

    return sections_df, [berth]


def test_berth_integration_flow(sample_berth_data):
    sections_df, berths = sample_berth_data

    # 1. Map dropins to sections (berths passed as 4th arg)
    dropins_by_section = _map_dropins_to_sections([], [], [], berths)
    assert "sec_1" in dropins_by_section
    assert len(dropins_by_section["sec_1"]) == 1
    assert dropins_by_section["sec_1"][0]["type"] == "berth"

    # 2. Splice fairways
    splice_fairways(sections_df, dropins_by_section, {})

    # Check if berth now has connection_geometry
    assert "connection_geometry" in berths[0]

    # 3. Generate berth specific features
    berth_features = generate_berth_graph_features(berths)

    # Expect: 1 connection node, 1 berth node, 1 access edge = 3 total
    assert len(berth_features) == 3

    # Verify access edge
    access_edges = [
        f
        for f in berth_features
        if f["properties"].get("segment_type") == "berth_access"
    ]
    assert len(access_edges) == 1
    assert access_edges[0]["properties"]["berth_id"] == "berth_123"
    assert access_edges[0]["properties"]["length_m"] == pytest.approx(20.0, abs=0.5)

    # Verify node types
    berth_nodes = [
        f for f in berth_features if f["properties"].get("node_type") == "berth"
    ]
    assert len(berth_nodes) == 1
    assert berth_nodes[0]["properties"]["name"] == "Test Berth"
    assert berth_nodes[0]["properties"]["isrs_id"] == "NLXXX00123"
