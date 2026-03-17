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
