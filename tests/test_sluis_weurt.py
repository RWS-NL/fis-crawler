"""
Unit tests for the Sluis Weurt (lock complex 49032) synthetic fixture.

These tests run entirely from the ``sluis_weurt_complex`` fixture defined in
conftest.py – no pipeline-generated output files are required.

They exercise:
  - The schematization invariant that caused the original invalid topology
    (merge point landing inside chamber 47538 in the real data).
  - Correct graph topology for both branches of the lock:
    - North branch – chamber 40927 (no internal junctions).
    - South branch – chamber 47538 (internal junction 8864190 / NL_J2501).
  - The two branches being genuinely parallel (no cross-branch nodes on either
    branch's shortest path).
"""

import networkx as nx
import pytest
from shapely.geometry import LineString


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _build_digraph(complex_obj):
    """Build a directed NetworkX graph from build_graph_features output."""
    from fis.lock.graph import build_graph_features

    features = build_graph_features([complex_obj])
    G = nx.DiGraph()
    for f in features:
        p = f["properties"]
        if p.get("feature_type") == "fairway_segment":
            G.add_edge(p["source_node"], p["target_node"])
    return G


# ---------------------------------------------------------------------------
# 1. Invariant: split/merge must never land inside a chamber polygon
# ---------------------------------------------------------------------------


def test_valid_schematization_does_not_raise(sluis_weurt_complex):
    """
    build_graph_features must not raise when split/merge are correctly placed
    outside every chamber polygon.
    """
    from fis.lock.graph import build_graph_features

    build_graph_features([sluis_weurt_complex])  # must not raise


def test_invalid_merge_inside_chamber_47538_raises(sluis_weurt_complex):
    """
    Reproduces the original bug: when the merge point is placed inside
    chamber 47538, build_graph_features must raise AssertionError.

    Chamber 47538 covers lon 5.819–5.825, lat 51.852–51.854.
    Placing the merge at (5.822, 51.853) puts it clearly inside that polygon.
    """
    from fis.lock.graph import build_graph_features

    # Move merge point to inside chamber 47538
    sluis_weurt_complex["geometry_after_wkt"] = LineString(
        [(5.822, 51.853), (5.838, 51.8538)]
    ).wkt

    with pytest.raises(
        AssertionError, match="lock_49032_merge is inside chamber 47538"
    ):
        build_graph_features([sluis_weurt_complex])


def test_invalid_split_inside_chamber_40927_raises(sluis_weurt_complex):
    """
    When the split point is placed inside chamber 40927, build_graph_features
    must raise AssertionError.

    Chamber 40927 covers lon 5.819–5.825, lat 51.854–51.856.
    Placing the split at (5.822, 51.855) puts it clearly inside.
    """
    from fis.lock.graph import build_graph_features

    sluis_weurt_complex["geometry_before_wkt"] = LineString(
        [(5.808, 51.8538), (5.822, 51.855)]
    ).wkt

    with pytest.raises(
        AssertionError, match="lock_49032_split is inside chamber 40927"
    ):
        build_graph_features([sluis_weurt_complex])


# ---------------------------------------------------------------------------
# 2. Node existence
# ---------------------------------------------------------------------------


def test_split_and_merge_nodes_exist(sluis_weurt_complex):
    """lock_49032_split and lock_49032_merge must be present in the graph."""
    G = _build_digraph(sluis_weurt_complex)
    assert "lock_49032_split" in G.nodes, "lock_49032_split not found"
    assert "lock_49032_merge" in G.nodes, "lock_49032_merge not found"


def test_chamber_start_end_nodes_exist(sluis_weurt_complex):
    """Both chambers must emit _start and _end nodes."""
    G = _build_digraph(sluis_weurt_complex)
    for ch_id in ("40927", "47538"):
        for role in ("start", "end"):
            node = f"chamber_{ch_id}_{role}"
            assert node in G.nodes, f"{node} not found in graph"


# ---------------------------------------------------------------------------
# 3. Topology: both branches reachable from split → merge
# ---------------------------------------------------------------------------


def test_north_branch_40927_reachable_from_split(sluis_weurt_complex):
    """North branch: lock_49032_split → chamber_40927_start must be reachable."""
    G = _build_digraph(sluis_weurt_complex)
    assert nx.has_path(G, "lock_49032_split", "chamber_40927_start")


def test_north_branch_40927_connects_to_merge(sluis_weurt_complex):
    """North branch: chamber_40927_end → lock_49032_merge must be reachable."""
    G = _build_digraph(sluis_weurt_complex)
    assert nx.has_path(G, "chamber_40927_end", "lock_49032_merge")


def test_south_branch_47538_reachable_from_split(sluis_weurt_complex):
    """South branch: lock_49032_split → chamber_47538_start must be reachable."""
    G = _build_digraph(sluis_weurt_complex)
    assert nx.has_path(G, "lock_49032_split", "chamber_47538_start")


def test_south_branch_47538_connects_to_merge(sluis_weurt_complex):
    """South branch: chamber_47538_end → lock_49032_merge must be reachable."""
    G = _build_digraph(sluis_weurt_complex)
    assert nx.has_path(G, "chamber_47538_end", "lock_49032_merge")


# ---------------------------------------------------------------------------
# 4. Internal junction 8864190 (NL_J2501) on the south-branch route
# ---------------------------------------------------------------------------


def test_internal_junction_8864190_emitted_as_node(sluis_weurt_complex):
    """Junction 8864190 must be emitted as a standalone graph node."""
    G = _build_digraph(sluis_weurt_complex)
    assert "8864190" in G.nodes, (
        "Internal junction 8864190 (NL_J2501) must appear as a node in the graph"
    )


def test_internal_junction_8864190_on_chamber_47538_route(sluis_weurt_complex):
    """
    Junction 8864190 must lie on the shortest path from
    chamber_47538_start to chamber_47538_end.
    """
    G = _build_digraph(sluis_weurt_complex)
    path = nx.shortest_path(G, "chamber_47538_start", "chamber_47538_end")
    assert "8864190" in path, (
        "Internal junction 8864190 must be an intermediate node on the "
        "chamber_47538_start → chamber_47538_end route"
    )


def test_north_chamber_route_has_no_internal_junctions(sluis_weurt_complex):
    """
    Chamber 40927 has no internal junctions, so the path from
    chamber_40927_start to chamber_40927_end must be exactly two nodes long
    (start → end, one segment).
    """
    G = _build_digraph(sluis_weurt_complex)
    path = nx.shortest_path(G, "chamber_40927_start", "chamber_40927_end")
    assert len(path) == 2, (
        f"chamber_40927 route without internal junctions should be a single segment "
        f"(path length 2), got {path}"
    )


# ---------------------------------------------------------------------------
# 5. Parallel branches – no cross-branch nodes
# ---------------------------------------------------------------------------


def test_north_branch_path_does_not_cross_south_chamber(sluis_weurt_complex):
    """
    The directed path from lock_49032_split to chamber_40927_end must not
    pass through any chamber_47538_* node.
    """
    G = _build_digraph(sluis_weurt_complex)
    path = nx.shortest_path(G, "lock_49032_split", "chamber_40927_end")
    south_nodes = [n for n in path if "47538" in n]
    assert not south_nodes, (
        f"North-branch path crossed south-branch nodes: {south_nodes}"
    )


def test_south_branch_path_does_not_cross_north_chamber(sluis_weurt_complex):
    """
    The directed path from lock_49032_split to chamber_47538_end must not
    pass through any chamber_40927_* node.
    """
    G = _build_digraph(sluis_weurt_complex)
    path = nx.shortest_path(G, "lock_49032_split", "chamber_47538_end")
    north_nodes = [n for n in path if "40927" in n]
    assert not north_nodes, (
        f"South-branch path crossed north-branch nodes: {north_nodes}"
    )
