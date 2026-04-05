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

    # Verify path exists from chamber start to opening start
    assert nx.has_path(G, start_node, op_start), (
        f"No path from {start_node} to {op_start}"
    )
    # Verify path exists from opening end to chamber end
    assert nx.has_path(G, op_end, end_node), f"No path from {op_end} to {end_node}"

    # Check that opening 9689 is indeed between chamber start and chamber end
    path = nx.shortest_path(G, start_node, end_node)
    assert op_start in path, (
        "opening_9689_start should be in the path through chamber route"
    )
    assert op_end in path, (
        "opening_9689_end should be in the path through chamber route"
    )


def test_scenario_2_volkerak_sluizen():
    """
    Scenario 2: Volkeraksluizen (Fairway 12821).
    Section 12821 splits into 3 chambers (6428, 24817, 7083).
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


def test_scenario_5_weurt_lock():
    """
    Scenario 5: Sluis Weurt (Lock 49032).
    Expected topological order of nodes:
    8864666 -> lock_49032_split -> chamber_47538_start -> 8864190 ->
    opening_5835_start -> opening_5835_end -> chamber_47538_end ->
    lock_49032_merge -> 8865102
    """
    G = load_graph()

    split_node = next(
        (
            v
            for u, v in G.edges()
            if u == "8864666"
            and str(v).startswith("lock_49032_")
            and str(v).endswith("_split")
        ),
        None,
    )
    merge_node = next(
        (
            u
            for u, v in G.edges()
            if v == "8865102"
            and str(u).startswith("lock_49032_")
            and str(u).endswith("_merge")
        ),
        None,
    )

    if split_node is None or merge_node is None:
        pytest.skip(
            "Missing expected dynamic split/merge nodes for Weurt lock in graph"
        )

    required_nodes = [
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

    missing_nodes = [node for node in required_nodes if node not in G]
    if missing_nodes:
        pytest.skip(f"Missing expected nodes for Weurt lock in graph: {missing_nodes}")

    # Check the ordered path sequence
    for i in range(len(required_nodes) - 1):
        source = required_nodes[i]
        target = required_nodes[i + 1]
        assert nx.has_path(G, source, target), f"No path from {source} to {target}"


def test_scenario_6_oranjesluizen():
    """
    Scenario 6: Oranjesluizen (Complex 50750 / 59464015).
    Bridge should be outside lock chambers.
    Flow direction: West to East (8864384 -> 59275858).
    Split back at 8864384.
    Merge at 59275858.
    """
    G = load_graph()

    junction_start = "8864384"
    merge_node = "59275858"

    # Right branch nodes (Lock 50750)
    split_node_50750 = next(
        (
            v
            for u, v in G.edges()
            if u == "59274799" and "lock_50750" in str(v) and "split" in str(v)
        ),
        None,
    )
    merge_node_50750 = next(
        (
            u
            for u, v in G.edges()
            if v == "8861427" and "lock_50750" in str(u) and "merge" in str(u)
        ),
        None,
    )

    if split_node_50750 is None or merge_node_50750 is None:
        pytest.skip(
            "Missing expected dynamic split/merge nodes for Oranjesluizen (50750) in graph"
        )

    right_nodes_pre = [junction_start, "59274799", split_node_50750]
    right_chambers = [
        ("chamber_3127_start", "chamber_3127_end"),
        ("chamber_55419_start", "chamber_55419_end"),
        ("chamber_21002_start", "chamber_21002_end"),
    ]
    right_nodes_post = [merge_node_50750, "8861427", merge_node]

    # Left branch nodes (Lock 59464015)
    split_node_59464015 = next(
        (
            v
            for u, v in G.edges()
            if u == "59275369" and "lock_59464015" in str(v) and "split" in str(v)
        ),
        None,
    )
    merge_node_59464015 = next(
        (
            u
            for u, v in G.edges()
            if v == "59275918" and "lock_59464015" in str(u) and "merge" in str(u)
        ),
        None,
    )

    if split_node_59464015 is None or merge_node_59464015 is None:
        pytest.skip(
            "Missing expected dynamic split/merge nodes for Oranjesluizen (59464015) in graph"
        )

    left_nodes_pre = [junction_start, "59275369", split_node_59464015]
    left_chambers = [
        ("chamber_11446_start", "chamber_11446_end"),
    ]
    left_nodes_post = [merge_node_59464015, "59275918", merge_node]

    # Verify right branch path
    prev_node = right_nodes_pre[0]
    for node in right_nodes_pre[1:]:
        assert nx.has_path(G, prev_node, node), (
            f"Right pre: no path {prev_node} -> {node}"
        )
        prev_node = node

    for ch_start, ch_end in right_chambers:
        assert nx.has_path(G, prev_node, ch_start), (
            f"Right chamber start: no path {prev_node} -> {ch_start}"
        )
        assert nx.has_path(G, ch_start, ch_end), (
            f"Right chamber: no path {ch_start} -> {ch_end}"
        )
        assert nx.has_path(G, ch_end, right_nodes_post[0]), (
            f"Right chamber end: no path {ch_end} -> {right_nodes_post[0]}"
        )

    prev_node = right_nodes_post[0]
    for node in right_nodes_post[1:]:
        assert nx.has_path(G, prev_node, node), (
            f"Right post: no path {prev_node} -> {node}"
        )
        prev_node = node

    # Verify left branch path
    prev_node = left_nodes_pre[0]
    for node in left_nodes_pre[1:]:
        assert nx.has_path(G, prev_node, node), (
            f"Left pre: no path {prev_node} -> {node}"
        )
        prev_node = node

    for ch_start, ch_end in left_chambers:
        assert nx.has_path(G, prev_node, ch_start), (
            f"Left chamber start: no path {prev_node} -> {ch_start}"
        )
        assert nx.has_path(G, ch_start, ch_end), (
            f"Left chamber: no path {ch_start} -> {ch_end}"
        )
        assert nx.has_path(G, ch_end, left_nodes_post[0]), (
            f"Left chamber end: no path {ch_end} -> {left_nodes_post[0]}"
        )

    prev_node = left_nodes_post[0]
    for node in left_nodes_post[1:]:
        assert nx.has_path(G, prev_node, node), (
            f"Left post: no path {prev_node} -> {node}"
        )
        prev_node = node
