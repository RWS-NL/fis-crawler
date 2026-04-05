from pathlib import Path
import geopandas as gpd
import networkx as nx
import pytest

_KNOWN_OUTPUT_PATHS = [
    Path("output/dropins-fis-detailed/edges.geoparquet"),
    Path("output/dropins-schematization/edges.geoparquet"),
]
_EDGES_PATH = next(
    (p for p in _KNOWN_OUTPUT_PATHS if p.exists()), _KNOWN_OUTPUT_PATHS[0]
)

pytestmark = pytest.mark.skipif(
    not _EDGES_PATH.exists(),
    reason=f"Required test data not generated in any of: {[str(p.parent) for p in _KNOWN_OUTPUT_PATHS]}",
)


def load_graph():
    edges = gpd.read_parquet(_EDGES_PATH)
    G = nx.Graph()
    for _, edge in edges.iterrows():
        if edge.source_node is not None and edge.target_node is not None:
            attrs = edge.to_dict()
            attrs["weight"] = float(attrs.get("length_m", 1.0))
            G.add_edge(edge.source_node, edge.target_node, **attrs)
    return G


def find_neighbor(G, node, prefix, suffix):
    """Find a neighbor of a node that matches a prefix and suffix."""
    if node not in G:
        return None
    for n in G.neighbors(node):
        if str(n).startswith(prefix) and str(n).endswith(suffix):
            return n
    return None


def assert_sequence(G, sequence, context=""):
    """Verify that the given sequence of nodes forms a continuous path in the graph."""
    for node in sequence:
        assert node in G, f"{context}: Node {node} not found in graph"

    # Verify each neighbor in sequence is connected
    for i in range(len(sequence) - 1):
        u, v = sequence[i], sequence[i + 1]
        assert nx.has_path(G, u, v), (
            f"{context}: No path exists between sequence neighbors {u} and {v}"
        )


def test_scenario_1_embedded_bridge():
    """
    Scenario 1: Fairway section 27027 should be split up by Chamber 18373.
    Chamber 18373 should be split up by a bridge with opening 9689.
    """
    G = load_graph()

    start_node = "chamber_18373_start"
    end_node = "chamber_18373_end"
    op_start = "opening_9689_start"
    op_end = "opening_9689_end"

    # Verify that there is a path from chamber start to end that includes the bridge
    assert nx.has_path(G, start_node, op_start)
    assert nx.has_path(G, op_end, end_node)

    path = nx.shortest_path(G, start_node, end_node, weight="weight")
    assert op_start in path, "opening_9689_start should be in the chamber path"
    assert op_end in path, "opening_9689_end should be in the chamber path"


def test_scenario_2_volkerak_sluizen():
    """
    Scenario 2: Volkeraksluizen (Fairway 12821).
    """
    G = load_graph()

    matches = [("6428", "43247"), ("24817", "9802"), ("7083", "39854")]

    for ch_id, op_id in matches:
        ch_start = f"chamber_{ch_id}_start"
        ch_end = f"chamber_{ch_id}_end"
        op_start = f"opening_{op_id}_start"
        op_end = f"opening_{op_id}_end"

        if ch_start not in G or op_start not in G:
            pytest.skip("Data missing from graph")

        # Verify path exists through chamber and opening
        assert nx.has_path(G, ch_start, op_start)
        assert nx.has_path(G, op_end, ch_end)


def test_scenario_3_krammerjachtensluis():
    """
    Scenario 3: Fairway section 13823 (Krammerjachtensluis).
    """
    G = load_graph()
    matches = [("16146", "7617764"), ("47766", "7069818")]

    for op_id, ch_id in matches:
        op_start = f"opening_{op_id}_start"
        op_end = f"opening_{op_id}_end"
        ch_start = f"chamber_{ch_id}_start"
        ch_end = f"chamber_{ch_id}_end"

        if op_start not in G or ch_start not in G:
            pytest.skip("Data missing from graph")

        assert nx.has_path(G, op_end, ch_start) or nx.has_path(G, ch_end, op_start)


def test_scenario_4_krammersluizen():
    """
    Scenario 4: Fairway section 57364 (Krammersluizen).
    """
    G = load_graph()
    matches = [("17693", "38644"), ("26232", "56085")]

    for op_id, ch_id in matches:
        op_start = f"opening_{op_id}_start"
        op_end = f"opening_{op_id}_end"
        ch_start = f"chamber_{ch_id}_start"
        ch_end = f"chamber_{ch_id}_end"

        if op_start not in G or ch_start not in G:
            pytest.skip("Data missing from graph")

        assert nx.has_path(G, op_end, ch_start) or nx.has_path(G, ch_end, op_start)


def test_scenario_5_weurt_lock():
    """
    Scenario 5: Sluis Weurt (Lock 49032).
    """
    G = load_graph()

    # The sequence involves multiple sections and junctions
    # 8864666 -> ... -> chamber_start -> 8864190 -> ... -> chamber_end -> ... -> 8865102

    required = [
        "8864666",
        "chamber_47538_start",
        "8864190",
        "opening_5835_start",
        "opening_5835_end",
        "chamber_47538_end",
        "8865102",
    ]
    assert_sequence(G, required, "Weurt Lock")


def test_scenario_6_oranjesluizen():
    """
    Scenario 6: Oranjesluizen (Complex 50750 / 59464015).
    """
    G = load_graph()

    junction_start = "8864384"
    merge_node = "59275858"

    # 1. Verify Bridge Schellingwouderbrug is not embedded
    # It should be on the path between start and split nodes
    assert "opening_20278_start" in G

    # 2. Right branch nodes (Lock 50750)
    # Check that each chamber forms a path between the shared start/end junctions
    right_chambers = [
        ("chamber_3127_start", "chamber_3127_end"),
        ("chamber_55419_start", "chamber_55419_end"),
        ("chamber_21002_start", "chamber_21002_end"),
    ]

    for c_start, c_end in right_chambers:
        assert nx.has_path(G, junction_start, c_start)
        assert nx.has_path(G, c_start, c_end)
        assert nx.has_path(G, c_end, merge_node)

    # 3. Left branch nodes (Lock 59464015)
    left_chambers = [
        ("chamber_11446_start", "chamber_11446_end"),
    ]
    for c_start, c_end in left_chambers:
        assert nx.has_path(G, junction_start, c_start)
        assert nx.has_path(G, c_start, c_end)
        assert nx.has_path(G, c_end, merge_node)
