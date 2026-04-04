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
    G = nx.DiGraph()
    for _, edge in edges.iterrows():
        if edge.source_node is not None and edge.target_node is not None:
            G.add_edge(edge.source_node, edge.target_node, **edge.to_dict())
    return G


def test_scenario_1_embedded_bridge():
    """
    Scenario 1: Fairway section 27027 should be split up by Chamber 18373.
    Chamber 18373 should be split up by a bridge with opening 9689.

    Expected topology: chamber_18373_start -> ... -> opening_9689_start -> opening_9689_passage -> opening_9689_end -> ... -> chamber_18373_end
    """
    G = load_graph()

    # Check if chamber 18373 nodes exist
    start_node = "chamber_18373_start"
    end_node = "chamber_18373_end"
    assert start_node in G, f"{start_node} not found in graph"
    assert end_node in G, f"{end_node} not found in graph"

    # Check for opening 9689 nodes
    op_start = "opening_9689_start"
    op_end = "opening_9689_end"
    assert op_start in G, f"{op_start} not found in graph"
    assert op_end in G, f"{op_end} not found in graph"

    merge_node = "lock_52078_merge"
    assert merge_node in G, f"{merge_node} not found in graph"

    # Verify path exists from chamber end to opening start (bridge is on exit route)
    assert nx.has_path(G, end_node, op_start), f"No path from {end_node} to {op_start}"
    # Verify path exists from opening end to lock merge
    assert nx.has_path(G, op_end, merge_node), f"No path from {op_end} to {merge_node}"

    # Check that opening 9689 is indeed between chamber end and lock merge
    path = nx.shortest_path(G, end_node, merge_node)
    assert op_start in path, (
        "opening_9689_start should be in the path through chamber exit"
    )
    assert op_end in path, "opening_9689_end should be in the path through chamber exit"


def test_scenario_2_volkerak_sluizen():
    """
    Scenario 2: Volkeraksluizen (Fairway 12821).
    Section 12821 splits into 3 chambers (6428, 24817, 7083).
    At the south west side of each chamber connects to the split section (12821).
    Within each section an opening is placed (43247 -> 6428, 9802 -> 24817, 39854 -> 7083).
    The subsections after the opening are merged together to connect to the remainder of 12821.

    Expected topology for each lane: ... -> chamber_6428_end -> ... -> opening_43247_start -> ... -> lock_42863_merge -> ...
    """
    G = load_graph()

    matches = [("6428", "43247"), ("24817", "9802"), ("7083", "39854")]

    merge_node = (
        "lock_42863_merge"  # Assuming 42863 is the lock complex ID for Volkerak
    )
    assert merge_node in G

    for ch_id, op_id in matches:
        ch_start = f"chamber_{ch_id}_start"
        ch_end = f"chamber_{ch_id}_end"
        op_start = f"opening_{op_id}_start"
        op_end = f"opening_{op_id}_end"

        if ch_start not in G or op_start not in G:
            pytest.skip(
                f"Data for Chamber {ch_id} or Opening {op_id} missing from graph"
            )

        assert ch_end in G
        assert op_end in G

        # Verify opening is reached AFTER chamber start (may be embedded OR after end)
        assert nx.has_path(G, ch_start, op_start), (
            f"Opening {op_id} should be after chamber {ch_id} start"
        )
        assert nx.has_path(G, op_end, merge_node), (
            f"Lock merge should be after opening {op_id} end"
        )


def test_scenario_3_krammerjachtensluis():
    """
    Scenario 3: Fairway section 13823 (Krammerjachtensluis).
    Split into 2 openings (16146, 47766). Each opening connects to a chamber.
    16146 -> chamber 7617764
    47766 -> chamber 7069818
    Expected topology: opening -> chamber
    """
    G = load_graph()

    matches = [("16146", "7617764"), ("47766", "7069818")]

    for op_id, ch_id in matches:
        op_start = f"opening_{op_id}_start"
        op_end = f"opening_{op_id}_end"
        ch_start = f"chamber_{ch_id}_start"

        if op_start not in G:
            pytest.skip(
                f"Opening {op_id} not found in graph. Fairway 13823 (Houtribdijk) may not be schematized."
            )

        assert op_end in G
        assert ch_start in G

        # Verify chamber is reached AFTER opening
        assert nx.has_path(G, op_end, ch_start), (
            f"Chamber {ch_id} should be after opening {op_id}"
        )


def test_scenario_4_krammersluizen():
    """
    Scenario 4: Fairway section 57364 (Krammersluizen).
    Split into 2 openings (17693, 26232). Each followed by a chamber.
    17693 -> chamber 38644
    26232 -> chamber 56085
    Expected topology: opening -> chamber
    """
    G = load_graph()

    matches = [("17693", "38644"), ("26232", "56085")]

    for op_id, ch_id in matches:
        op_start = f"opening_{op_id}_start"
        op_end = f"opening_{op_id}_end"
        ch_start = f"chamber_{ch_id}_start"

        if op_start not in G:
            pytest.skip(
                f"Opening {op_id} not found in graph. Fairway 57364 (Naviduct) may not be schematized."
            )

        assert op_end in G
        assert ch_start in G

        # Verify chamber is reached AFTER opening
        assert nx.has_path(G, op_end, ch_start), (
            f"Chamber {ch_id} should be after opening {op_id}"
        )


@pytest.mark.xfail(reason="Issue #143: Incorrect topology for Sluis Weurt", strict=True)
def test_scenario_5_weurt_lock():
    """
    Scenario 5: Sluis Weurt (Lock 49032).
    Expected topological order of nodes:
    8864666 -> lock_49032_split -> chamber_47538_start -> 8864190 ->
    opening_5835_start -> opening_5835_end -> chamber_47538_end ->
    lock_49032_merge -> 8865102
    """
    G = load_graph()

    required_nodes = [
        "8864666",
        "lock_49032_split",
        "chamber_47538_start",
        "8864190",
        "opening_5835_start",
        "opening_5835_end",
        "chamber_47538_end",
        "lock_49032_merge",
        "8865102",
    ]

    missing_nodes = [node for node in required_nodes if node not in G]
    if missing_nodes:
        pytest.skip(f"Missing expected nodes for Weurt lock in graph: {missing_nodes}")

    # Check the ordered path sequence
    for i in range(len(required_nodes) - 1):
        source = required_nodes[i]
        target = required_nodes[i + 1]
        assert nx.has_path(G, source, target), f"No path from {source} to {target}"


@pytest.mark.xfail(
    reason="Issue #143: Incorrect topology for Oranjesluizen", strict=True
)
def test_scenario_6_oranjesluizen():
    """
    Scenario 6: Oranjesluizen (Complex 50750 / 59464015).
    Bridge should be outside lock chambers.
    Junction 30985116 -> node 59275858 (splits two locks).
    Left branch: chamber_11446.
    Right branch: 8861427 -> lock_50750_split -> chambers 3127, 55419, 21002 -> lock_50750_merge -> 59274799
    Merge back at 8864384.
    """
    G = load_graph()

    junction_start = "30985116"
    split_node = "59275858"
    merge_node = "8864384"

    # Left branch nodes
    left_nodes = ["59275918", "chamber_11446_start", "chamber_11446_end", "59275369"]

    # Right branch nodes
    right_nodes_pre = ["8861427", "lock_50750_split"]
    right_chambers = [
        ("chamber_3127_start", "chamber_3127_end"),
        ("chamber_55419_start", "chamber_55419_end"),
        ("chamber_21002_start", "chamber_21002_end"),
    ]
    right_nodes_post = ["lock_50750_merge", "59274799"]

    # Quick check if main graph features exist
    if junction_start not in G or split_node not in G or merge_node not in G:
        pytest.skip("Oranjesluizen baseline nodes missing from graph.")

    # 1. Start junction to split
    assert nx.has_path(G, junction_start, split_node), (
        "No path from junction to split node"
    )

    # 2. Left branch topology
    prev_node = split_node
    for node in left_nodes:
        assert node in G, f"Left branch node missing: {node}"
        assert nx.has_path(G, prev_node, node), (
            f"Left branch: no path {prev_node} -> {node}"
        )
        prev_node = node
    assert nx.has_path(G, prev_node, merge_node), (
        f"Left branch: no path {prev_node} -> merge {merge_node}"
    )

    # 3. Right branch topology
    prev_node = split_node
    for node in right_nodes_pre:
        assert node in G, f"Right branch pre-node missing: {node}"
        assert nx.has_path(G, prev_node, node), (
            f"Right branch: no path {prev_node} -> {node}"
        )
        prev_node = node

    for start_c, end_c in right_chambers:
        assert start_c in G and end_c in G, f"Chamber nodes missing: {start_c}, {end_c}"
        assert nx.has_path(G, prev_node, start_c), (
            f"Right branch: no path {prev_node} -> {start_c}"
        )
        assert nx.has_path(G, start_c, end_c), (
            f"Right branch: no path {start_c} -> {end_c}"
        )
        assert nx.has_path(G, end_c, right_nodes_post[0]), (
            f"Right branch: no path {end_c} -> {right_nodes_post[0]}"
        )

    prev_node = right_nodes_post[0]
    for node in right_nodes_post[1:]:
        assert node in G, f"Right branch post-node missing: {node}"
        assert nx.has_path(G, prev_node, node), (
            f"Right branch: no path {prev_node} -> {node}"
        )
        prev_node = node

    assert nx.has_path(G, prev_node, merge_node), (
        f"Right branch: no path {prev_node} -> merge {merge_node}"
    )
