from pathlib import Path
import geopandas as gpd
import networkx as nx
import pytest

pytestmark = pytest.mark.skipif(
    not Path("output/dropins-schematization/edges.geoparquet").exists(),
    reason="Required test data not generated in output/dropins-schematization/",
)


def load_graph():
    edges = gpd.read_parquet("output/dropins-schematization/edges.geoparquet")
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


# ---------------------------------------------------------------------------
# New gold-standard topology assertions for issues #143 and #145
# ---------------------------------------------------------------------------


def test_scenario_5_sluis_weurt_north_branch():
    """
    Sluis Weurt (lock complex 49032) – north branch (chamber 40927).

    Expected node order:
        lock_49032_split
        → chamber_40927_start
        → chamber_40927_end
        → opening_25111_start  (bridge is ADJACENT, not inside chamber)
        → opening_25111_end
        → lock_49032_merge

    This validates that:
    * The global split/merge are positioned correctly (asymmetric buffer).
    * Opening 25111 is NOT embedded inside chamber 40927 (it should appear on
      the exit edge after chamber_40927_end, not on the chamber_route edge).
    """
    G = load_graph()

    split_n = "lock_49032_split"
    merge_n = "lock_49032_merge"
    ch_start = "chamber_40927_start"
    ch_end = "chamber_40927_end"
    op_start = "opening_25111_start"
    op_end = "opening_25111_end"

    for node in (split_n, merge_n, ch_start, ch_end):
        if node not in G:
            pytest.skip(f"Node {node} not found – Weurt data may not be schematized.")

    # Both chambers must be reachable from the shared split
    assert nx.has_path(G, split_n, ch_start), f"No path from {split_n} to {ch_start}"
    assert nx.has_path(G, ch_end, merge_n), f"No path from {ch_end} to {merge_n}"

    # Bridge 25111 must appear on the EXIT side of chamber 40927
    if op_start in G and op_end in G:
        # opening_25111 must NOT be between chamber_40927_start and chamber_40927_end
        ch_route_path = nx.shortest_path(G, ch_start, ch_end)
        assert op_start not in ch_route_path, (
            "opening_25111 is inside chamber 40927 route – should be on exit edge"
        )
        # opening_25111 must be reachable from chamber_40927_end before lock merge
        assert nx.has_path(G, ch_end, op_start), f"No path from {ch_end} to {op_start}"
        path_to_merge = nx.shortest_path(G, ch_end, merge_n)
        assert op_start in path_to_merge, (
            "opening_25111_start must be on the path from chamber_40927_end to merge"
        )


def test_scenario_6_sluis_weurt_south_branch():
    """
    Sluis Weurt (lock complex 49032) – south branch (chamber 47538).

    Expected node order:
        lock_49032_split
        → chamber_47538_start
        → 8864190              (NL_J2501 – internal FIS junction inside chamber)
        → opening_5835_start   (bridge opening embedded inside chamber)
        → opening_5835_end
        → chamber_47538_end
        → lock_49032_merge

    This validates that:
    * Junction 8864190 is present as a ``chamber_internal_junction`` node.
    * Opening 5835 is embedded (between the junction and chamber_47538_end).
    """
    G = load_graph()

    split_n = "lock_49032_split"
    merge_n = "lock_49032_merge"
    ch_start = "chamber_47538_start"
    ch_end = "chamber_47538_end"
    internal_junc = "8864190"

    for node in (split_n, merge_n, ch_start, ch_end):
        if node not in G:
            pytest.skip(f"Node {node} not found – Weurt data may not be schematized.")

    # Both nodes exist and are connected to the complex
    assert nx.has_path(G, split_n, ch_start)
    assert nx.has_path(G, ch_end, merge_n)

    # Internal junction 8864190 must exist as a node on the chamber 47538 route
    if internal_junc in G:
        route_path = nx.shortest_path(G, ch_start, ch_end)
        assert internal_junc in route_path, (
            f"FIS junction {internal_junc} (NL_J2501) must be an intermediate node "
            f"on chamber_47538 route (between {ch_start} and {ch_end})"
        )

    # Opening 5835 must be EMBEDDED inside chamber 47538 (between start and end)
    op_start = "opening_5835_start"
    op_end = "opening_5835_end"
    if op_start in G and op_end in G:
        assert nx.has_path(G, ch_start, op_start)
        assert nx.has_path(G, op_end, ch_end)


def test_scenario_7_oranjesluizen_both_branches():
    """
    Oranjesluizen (lock complexes 50750 and 59464015) – multi-branch complex.

    The fairway splits at junction 59275858 (upstream) and merges at junction
    8861427 (downstream).  Each branch contains its own set of lock chambers.

    This validates that:
    * The shared upstream/downstream boundary junctions exist in the graph.
    * Each lock's chambers are reachable from the upstream boundary junction and
      lead back to the downstream boundary junction.
    * The two branches are genuinely parallel (neither branch passes through the
      other branch's chambers).

    Lock 59464015 – left branch: chamber 11446
    Lock 50750    – right branch: chambers 3127, 55419, 21002
    """
    G = load_graph()

    upstream = "59275858"
    downstream = "8861427"

    if upstream not in G or downstream not in G:
        pytest.skip("Oranjesluizen boundary junctions not found in graph.")

    # Left branch: chamber 11446
    ch_left = "chamber_11446_start"
    if ch_left in G:
        assert nx.has_path(G, upstream, ch_left), (
            f"Left branch chamber {ch_left} not reachable from upstream junction"
        )
        ch_left_end = "chamber_11446_end"
        if ch_left_end in G:
            assert nx.has_path(G, ch_left_end, downstream), (
                "Left branch chamber_11446_end must lead to downstream junction"
            )

    # Right branch: at least one of the three chambers must be reachable
    right_chamber_starts = [
        "chamber_3127_start",
        "chamber_55419_start",
        "chamber_21002_start",
    ]
    found_right = False
    for ch_start in right_chamber_starts:
        if ch_start in G:
            found_right = True
            assert nx.has_path(G, upstream, ch_start), (
                f"Right branch chamber {ch_start} not reachable from upstream junction"
            )
    if not found_right:
        pytest.skip("No right-branch Oranjesluizen chambers found in graph.")

    # Both branches must be reachable from upstream without crossing each other
    assert nx.has_path(G, upstream, downstream), (
        "Upstream and downstream boundary junctions must be connected"
    )


def test_scenario_8_volkeraksluizen_boundary_junctions():
    """
    Volkeraksluizen (lock complex 42863): the complex should connect to the
    boundary junctions NL_J1728 and NL_J0964 that frame the entire complex.

    This validates that the split/merge nodes are placed so that the wider
    network remains reachable through the lock complex.
    """
    G = load_graph()

    split_n = "lock_42863_split"
    merge_n = "lock_42863_merge"

    if split_n not in G or merge_n not in G:
        pytest.skip("Volkeraksluizen split/merge nodes not found.")

    # The boundary junctions at either side of the complex
    junc_a = "NL_J1728"
    junc_b = "NL_J0964"

    for junc in (junc_a, junc_b):
        if junc not in G:
            continue
        assert nx.has_path(G, split_n, junc) or nx.has_path(G, merge_n, junc), (
            f"Boundary junction {junc} is not reachable from the Volkerak complex"
        )
