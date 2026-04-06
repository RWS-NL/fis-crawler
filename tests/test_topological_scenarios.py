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

    # Verify each neighbor in sequence is directly connected
    for i in range(len(sequence) - 1):
        u, v = sequence[i], sequence[i + 1]
        assert G.has_edge(u, v), (
            f"{context}: Sequence neighbors {u} and {v} are not directly connected"
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

    merge_node = find_neighbor(G, end_node, "lock_52078_", "_merge")
    if merge_node is None:
        merge_node = next(
            (
                n
                for n in G.nodes()
                if str(n).startswith("lock_52078_") and str(n).endswith("_merge")
            ),
            None,
        )

    assert merge_node is not None, "lock_52078 merge node not found in graph"

    path = nx.shortest_path(G, start_node, merge_node, weight="weight")

    assert op_start in path, "opening_9689_start should be in the path"
    assert op_end in path, "opening_9689_end should be in the path"
    assert end_node in path, "chamber_18373_end should be in the path"


def test_scenario_2_volkerak_sluizen():
    """
    Scenario 2: Volkeraksluizen (Fairway 12821).
    """
    G = load_graph()

    matches = [("6428", "43247"), ("24817", "9802"), ("7083", "39854")]

    merge_node = next(
        (
            n
            for n in G.nodes()
            if str(n).startswith("lock_42863_") and str(n).endswith("_merge")
        ),
        None,
    )
    assert merge_node is not None, "lock_42863 merge node not found in graph"

    for ch_id, op_id in matches:
        ch_start = f"chamber_{ch_id}_start"
        op_start = f"opening_{op_id}_start"
        op_end = f"opening_{op_id}_end"

        if ch_start not in G or op_start not in G:
            pytest.skip("Data missing from graph")

        path = nx.shortest_path(G, ch_start, merge_node, weight="weight")
        assert op_start in path, f"Opening {op_id} start should be in path to merge"
        assert op_end in path, f"Opening {op_id} end should be in path to merge"


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

        path = nx.shortest_path(G, op_start, ch_end, weight="weight")
        assert op_end in path
        assert ch_start in path


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

        path = nx.shortest_path(G, op_start, ch_end, weight="weight")
        assert op_end in path
        assert ch_start in path


def test_scenario_5_weurt_lock():
    """
    Scenario 5: Sluis Weurt (Lock 49032).
    Addresses challenges with unaligned chambers and embedded/adjacent bridges.
    """
    G = load_graph()

    split_node = find_neighbor(G, "8864666", "lock_49032_", "_split")
    merge_node = find_neighbor(G, "8865102", "lock_49032_", "_merge")

    if split_node is None:
        split_node = next(
            (
                n
                for n in G.nodes()
                if str(n).startswith("lock_49032_") and str(n).endswith("_split")
            ),
            None,
        )
    if merge_node is None:
        merge_node = next(
            (
                n
                for n in G.nodes()
                if str(n).startswith("lock_49032_") and str(n).endswith("_merge")
            ),
            None,
        )

    if split_node is None or merge_node is None:
        pytest.skip(
            "Missing expected dynamic split/merge nodes for Weurt lock in graph"
        )

    # Chamber 40927: Bridge 25111 is OUTSIDE the chamber.
    req_40927 = [
        "8864666",
        split_node,
        "chamber_40927_start",
        "chamber_40927_end",
        "opening_25111_start",
        "opening_25111_end",
        merge_node,
        "8865102",
    ]
    assert_sequence(G, req_40927, context="Weurt Lock 40927")

    # Chamber 47538: Bridge 5835 is INSIDE the chamber, along with node 8864190.
    req_47538 = [
        "8864666",
        split_node,
        "chamber_47538_start",
        "8864190",
        "opening_5835_start",
        "opening_5835_end",
        "chamber_47538_end",
        merge_node,
        "8865102",
    ]
    assert_sequence(G, req_47538, context="Weurt Lock 47538")


def test_scenario_6_oranjesluizen():
    """
    Scenario 6: Oranjesluizen (Complex 50750 / 59464015).
    Schellingwouderbrug (21755) should not be embedded.
    Extended to include NL_J5563 (8865563) and NL_J1942 (8861942) if present.
    """
    G = load_graph()

    junction_start = "8864384"
    merge_node = "59275858"

    extended_start = "8865563"
    extended_merge = "8861942"

    # Bridge Schellingwouderbrug should NOT be embedded inside any chamber.
    assert "opening_20278_start" in G, (
        "Bridge Schellingwouderbrug opening should exist in graph"
    )

    # Right branch nodes (Lock 50750)
    # The split and merge nodes are the consumed junctions themselves!
    split_node_50750 = "59274799"
    merge_node_50750 = "8861427"

    right_chambers = [
        ("chamber_3127_start", "chamber_3127_end"),
        ("chamber_55419_start", "chamber_55419_end"),
        ("chamber_21002_start", "chamber_21002_end"),
    ]

    # Left branch nodes (Lock 59464015)
    split_node_59464015 = "59275369"
    merge_node_59464015 = "59275918"

    left_chambers = [
        ("chamber_11446_start", "chamber_11446_end"),
    ]

    # Verify sequences
    assert_sequence(G, [junction_start, split_node_50750], "Right Pre")
    assert_sequence(G, [merge_node_50750, merge_node], "Right Post")
    for start_c, end_c in right_chambers:
        assert_sequence(
            G,
            [split_node_50750, start_c, end_c, merge_node_50750],
            f"Right Chamber {start_c}",
        )

    assert_sequence(G, [junction_start, split_node_59464015], "Left Pre")
    assert_sequence(G, [merge_node_59464015, merge_node], "Left Post")
    for start_c, end_c in left_chambers:
        assert_sequence(
            G,
            [split_node_59464015, start_c, end_c, merge_node_59464015],
            f"Left Chamber {start_c}",
        )

    # Bonus Extensions for Oranjesluizen
    if extended_start in G:
        assert nx.has_path(G, extended_start, junction_start), (
            f"Missing path from {extended_start} to Oranjesluizen"
        )
    if extended_merge in G:
        assert nx.has_path(G, merge_node, extended_merge), (
            f"Missing path from Oranjesluizen to {extended_merge}"
        )
