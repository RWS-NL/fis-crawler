import pytest
import networkx as nx
from shapely.geometry import Point
from fis.graph.integrate import find_geometric_border_connections, merge_graphs


@pytest.fixture
def fis_graph():
    """Create a sample FIS graph."""
    g = nx.Graph()
    # FIS node near the border (e.g. Lobith)
    g.add_node(1001, x=6.1, y=51.9, Geometry=Point(6.1, 51.9))  # Real FIS node
    g.add_node(1002, x=6.0, y=51.9, Geometry=Point(6.0, 51.9))  # Internal FIS node
    g.add_edge(1001, 1002, length_m=1000)
    return g


@pytest.fixture
def euris_graph():
    """Create a sample EURIS graph with cross-border edge."""
    g = nx.Graph()
    # German node
    g.add_node("DE_1", countrycode="DE", geometry=Point(6.2, 51.9))
    # Dutch bridgehead node (duplicate of FIS 1001, slight offset)
    g.add_node("NL_1", countrycode="NL", geometry=Point(6.1001, 51.9001))
    # Internal Dutch node
    g.add_node("NL_2", countrycode="NL", geometry=Point(6.0, 51.9))

    # Border crossing edge: DE -> NL
    g.add_edge("DE_1", "NL_1", objectname="Rhine Border")
    # Internal NL edge
    g.add_edge("NL_1", "NL_2", objectname="Rhine NL")
    return g


def test_find_geometric_border_connections(fis_graph, euris_graph):
    """Test geometric stitching of EURIS-NL nodes to FIS nodes."""

    # Run stitching with strict threshold (e.g. ~50m)
    # 0.0002 degrees is roughly 20m
    connections = find_geometric_border_connections(
        fis_graph, euris_graph, distance_threshold=500
    )

    assert len(connections) == 1

    conn = connections[0]
    # Should link foreign node (DE_1) to FIS node (1001) via bridgehead (NL_1)
    assert conn["foreign_node"] == "DE_1"
    assert conn["bridgehead_node"] == "NL_1"
    assert conn["fis_node"] == 1001
    assert conn["distance"] < 500


def test_merge_graphs_with_geometric_stitch(fis_graph, euris_graph):
    """Test merging graphs with geometric connections."""

    # Create a connection object as returned by find_geometric_border_connections
    connections = [
        {
            "foreign_node": "DE_1",
            "bridgehead_node": "NL_1",
            "fis_node": 1001,
            "distance": 15.0,
            "type": "geometric",
        }
    ]

    merged = merge_graphs(fis_graph, euris_graph, connections)

    assert merged.has_node("FIS_1001")
    assert merged.has_node("EURIS_DE_1")
    # NL bridgehead should be skipped as we filter out NL nodes from EURIS
    assert not merged.has_node("EURIS_NL_1")

    # Check stitched edge exists
    # FIS_1001 <-> EURIS_DE_1
    assert merged.has_edge("FIS_1001", "EURIS_DE_1")

    edge = merged.edges["FIS_1001", "EURIS_DE_1"]
    assert edge["data_source"] == "BORDER"
    assert edge["bridgehead"] == "NL_1"


def test_latest_paths_by_country(tmp_path):
    """Only the newest dated EURIS file per country should be kept."""
    from fis.graph.euris import _latest_paths_by_country

    files = [
        tmp_path / "FairwaySection_FR_20260223.geojson",
        tmp_path / "FairwaySection_FR_20260429.geojson",
        tmp_path / "FairwaySection_DE_20260224.geojson",
        tmp_path / "FairwaySection_DE_20260429.geojson",
        tmp_path / "FairwaySection_BE_20260101.geojson",
    ]
    selected = _latest_paths_by_country(files, prefix="FairwaySection")
    names = {p.name for p in selected}
    assert names == {
        "FairwaySection_FR_20260429.geojson",
        "FairwaySection_DE_20260429.geojson",
        "FairwaySection_BE_20260101.geojson",
    }


def test_drop_mutual_common_border_duplicates():
    """Mutual code_cb pairs are deduplicated; one-sided references are kept."""
    import geopandas as gpd
    from fis.graph.euris import _drop_mutual_common_border_duplicates

    gdf = gpd.GeoDataFrame(
        {
            "code": ["AT0000100033", "SK0000100001", "DE0040100383", "AT0000100001"],
            "code_cb": ["SK0000100001", "AT0000100033", "AT0000100001", None],
            "geometry": [Point(0, 0), Point(0, 0), Point(1, 1), Point(2, 2)],
        }
    )
    deduped = _drop_mutual_common_border_duplicates(gdf)
    assert set(deduped["code"]) == {"AT0000100033", "DE0040100383", "AT0000100001"}


def test_merge_graphs_deduplicates_be_nl_borders():
    """Common-border NL edges are skipped and redundant BE overlaps are retargeted."""
    fis_g = nx.Graph()
    fis_g.add_node(35228583, Geometry=Point(4.2, 51.4))
    fis_g.add_node(30986149, Geometry=Point(4.3, 51.35))
    fis_g.add_edge(35228583, 30986149, length_m=4700)

    euris_g = nx.Graph()
    euris_g.add_node("NL_1", countrycode="NL", geometry=Point(4.3, 51.35))
    euris_g.add_node("BE_F7792", countrycode="BE", geometry=Point(4.3, 51.35))
    euris_g.add_node("BE_D7511", countrycode="BE", geometry=Point(4.2003, 51.4003))
    euris_g.add_node("BE_F5199", countrycode="BE", geometry=Point(5.8, 51.1))
    euris_g.add_node("BE_F5569", countrycode="BE", geometry=Point(5.85, 51.14))

    euris_g.add_edge("NL_1", "BE_F7792")
    euris_g.add_edge("BE_F7792", "BE_D7511", code="BE0173200000")
    euris_g.add_edge("BE_F5199", "BE_F5569", code="BE0440300000", cntrycode_cb="NL")

    connections = [
        {
            "foreign_node": "BE_F7792",
            "bridgehead_node": "NL_1",
            "fis_node": 30986149,
            "distance": 10.0,
            "type": "geometric",
            "edge_attrs": {},
        }
    ]
    merged = merge_graphs(fis_g, euris_g, connections)

    assert not merged.has_node("EURIS_BE_F7792")
    assert not merged.has_edge("EURIS_BE_F5199", "EURIS_BE_F5569")
    assert merged.has_edge("FIS_35228583", "EURIS_BE_D7511")
    edge = merged.edges["FIS_35228583", "EURIS_BE_D7511"]
    assert edge["data_source"] == "BORDER"
    assert edge["segment_type"] == "border_connection"
    assert edge["code"] == "virtual_EURIS_BE_D7511_FIS_35228583"


def test_split_edge_at_node():
    """Missing T-junctions split the target EURIS section edge into two segments."""
    from shapely.geometry import LineString
    from fis.graph.euris import _split_edge_at_node

    g = nx.Graph()
    g.add_node("BE_T3999", geometry=Point(5.46, 50.59))
    g.add_node("BE_T4036", geometry=Point(5.61, 50.65))
    g.add_node("BE_T2691", geometry=Point(5.58, 50.62))
    g.add_edge(
        "BE_T3999",
        "BE_T4036",
        code="BE6659300015",
        geometry=LineString([(5.46, 50.59), (5.58, 50.62), (5.61, 50.65)]),
    )

    _split_edge_at_node(g, "BE6659300015", "BE_T2691")

    assert not g.has_edge("BE_T3999", "BE_T4036")
    assert g.has_edge("BE_T3999", "BE_T2691")
    assert g.has_edge("BE_T2691", "BE_T4036")
    assert g.edges["BE_T3999", "BE_T2691"]["code"] == "BE6659300015"
    assert g.edges["BE_T2691", "BE_T4036"]["code"] == "BE6659300015"
