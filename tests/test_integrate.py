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
