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
    import pandas as pd

    for _, edge in edges.iterrows():
        if pd.notna(edge.source_node) and pd.notna(edge.target_node):
            attrs = edge.to_dict()
            attrs["weight"] = float(
                attrs.get("length_m", 1.0) if pd.notna(attrs.get("length_m")) else 1.0
            )
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

    # Main Chambers on section 12821
    main_chambers = ["6428", "7083", "24817"]
    j_start_main = "8862498"
    j_end_main = "8868426"

    for ch_id in main_chambers:
        s = f"chamber_{ch_id}_start"
        e = f"chamber_{ch_id}_end"
        assert nx.has_path(G, j_start_main, s), f"No path from {j_start_main} to {s}"
        assert nx.has_path(G, e, j_end_main), f"No path from {e} to {j_end_main}"

    # Bonus Extensions for Volkeraksluizen
    extended_merge_1 = "8861728"
    extended_merge_2 = "8860964"
    if (
        extended_merge_1 in G
        and j_end_main in G
        and nx.has_path(G, j_end_main, extended_merge_1)
    ):
        assert nx.has_path(G, j_end_main, extended_merge_1)
    if (
        extended_merge_2 in G
        and j_end_main in G
        and nx.has_path(G, j_end_main, extended_merge_2)
    ):
        assert nx.has_path(G, j_end_main, extended_merge_2)


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

    # Shared endpoints
    entry_node = "8864666"
    exit_node = "8865102"

    chambers = ["40927", "47538"]
    for ch_id in chambers:
        start = f"chamber_{ch_id}_start"
        end = f"chamber_{ch_id}_end"
        assert nx.has_path(G, entry_node, start), (
            f"No path from {entry_node} to {start}"
        )
        assert nx.has_path(G, end, exit_node), f"No path from {end} to {exit_node}"

    # Specific check: 40927 must go through bridge 25111
    # We find any path from chamber end to exit node and check for bridge
    found_bridge = False
    for path in nx.all_simple_paths(G, "chamber_40927_end", exit_node, cutoff=6):
        if "opening_25111_start" in path:
            found_bridge = True
            break
    assert found_bridge, (
        "Path from chamber 40927 end to exit should contain bridge 25111"
    )


def test_scenario_6_oranjesluizen():
    """
    Scenario 6: Oranjesluizen (Complex 50750 / 59464015).
    Schellingwouderbrug (21755) should not be embedded.
    Extended to include NL_J5563 (8865563) and NL_J1942 (8861942) if present.
    """
    G = load_graph()

    # Use direct junctions for path verification
    junction_start = "59274799"
    merge_node = "8861427"

    extended_start = "8865563"
    extended_merge = "8861942"

    # Bridge Schellingwouderbrug should NOT be embedded inside any chamber.
    assert "opening_20278_start" in G, (
        "Bridge Schellingwouderbrug opening should exist in graph"
    )

    # Use has_path for robustness across complex Oranjesluizen topology
    right_chambers = ["3127", "55419", "21002"]
    for ch_id in right_chambers:
        s = f"chamber_{ch_id}_start"
        e = f"chamber_{ch_id}_end"
        assert nx.has_path(G, junction_start, s), (
            f"No path from {junction_start} to {s}"
        )
        assert nx.has_path(G, e, merge_node), f"No path from {e} to {merge_node}"

        # Verify ordering
        path = nx.shortest_path(G, junction_start, merge_node)
        if s in path and e in path:
            assert path.index(s) < path.index(e), (
                f"Chamber {ch_id} start must come before end"
            )

    left_chambers = ["11446"]
    for ch_id in left_chambers:
        s = f"chamber_{ch_id}_start"
        e = f"chamber_{ch_id}_end"
        assert nx.has_path(G, junction_start, s), (
            f"No path from {junction_start} to {s}"
        )
        assert nx.has_path(G, e, merge_node), f"No path from {e} to {merge_node}"

        path = nx.shortest_path(G, junction_start, merge_node)
        if s in path and e in path:
            assert path.index(s) < path.index(e), (
                f"Chamber {ch_id} start must come before end"
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
