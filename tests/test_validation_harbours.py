import networkx as nx

from fis import settings
from fis.graph.validation import GraphValidator


def _fairway_with_harbour(access_length):
    G = nx.Graph()
    G.add_edge("a", "harbour_1_connection", segment_type="clear")
    G.add_edge("harbour_1_connection", "b", segment_type="clear")
    G.add_node("harbour_1", node_type="harbour")
    G.add_edge(
        "harbour_1",
        "harbour_1_connection",
        segment_type="harbour_access",
        length_m=access_length,
    )
    return G


def test_harbour_leaf_beside_fairway_passes():
    G = _fairway_with_harbour(settings.HARBOUR_MIN_ACCESS_LENGTH_M)
    result = GraphValidator(G).check_harbours()
    assert result["status"] == "PASS"
    assert result["total_harbours"] == 1


def test_harbour_on_fairway_fails():
    G = _fairway_with_harbour(0.3)
    result = GraphValidator(G).check_harbours()
    assert result["status"] == "FAIL"
    assert [h["node"] for h in result["short_access"]] == ["harbour_1"]


def test_harbour_in_fairway_fails():
    # Harbour as a through node: fairway traffic would pass through it
    G = _fairway_with_harbour(settings.HARBOUR_MIN_ACCESS_LENGTH_M)
    G.add_edge("harbour_1", "c", segment_type="clear")
    result = GraphValidator(G).check_harbours()
    assert result["status"] == "FAIL"
    assert [h["node"] for h in result["not_leaf"]] == ["harbour_1"]


def test_connection_that_cuts_fairway_fails():
    # Issue #210: leading segment a -> connection was dropped by the splicer
    G = _fairway_with_harbour(settings.HARBOUR_MIN_ACCESS_LENGTH_M)
    G.remove_edge("a", "harbour_1_connection")
    nx.add_path(G, ["a", "x", "y", "z"], segment_type="clear")
    result = GraphValidator(G).check_harbours()
    assert result["status"] == "FAIL"
    assert [c["node"] for c in result["dangling_connections"]] == [
        "harbour_1_connection"
    ]
    assert [h["node"] for h in result["unreachable"]] == ["harbour_1"]
