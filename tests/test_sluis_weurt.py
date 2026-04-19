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
import pandas as pd
import pytest
from shapely.geometry import LineString, Point, Polygon


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


# ---------------------------------------------------------------------------
# 6. Buffer computation: doors projected onto fairway → split/merge outside chambers
# ---------------------------------------------------------------------------


def _simulate_split_merge_from_buffers(fw_geom, lock_geom, lock_chambers_df):
    """
    Call _compute_asymmetric_lock_buffers and simulate what process_fairway_geometry
    does with the returned buffers so we can check the resulting split/merge points.
    """
    import geopandas as gpd
    from shapely.ops import substring
    from fis import settings
    from fis.lock.core import _compute_asymmetric_lock_buffers

    class _FwRow:
        geometry = fw_geom

    class _LockRow:
        geometry = lock_geom

    buf_before, buf_after = _compute_asymmetric_lock_buffers(
        _FwRow(), _LockRow(), lock_chambers_df
    )

    gs_fw = gpd.GeoSeries([fw_geom], crs="EPSG:4326").to_crs(settings.PROJECTED_CRS)
    gs_lock = gpd.GeoSeries([lock_geom], crs="EPSG:4326").to_crs(settings.PROJECTED_CRS)
    fw_rd = gs_fw.iloc[0]
    lock_rd = gs_lock.iloc[0]
    if lock_rd.geom_type != "Point":
        lock_rd = lock_rd.centroid

    lock_proj = fw_rd.project(lock_rd)
    total_len_rd = fw_rd.length
    total_len_wgs = fw_geom.length

    dist_before = max(0.0, lock_proj - buf_before)
    dist_after = min(total_len_rd, lock_proj + buf_after)

    geom_before = substring(fw_geom, 0.0, (dist_before / total_len_rd) * total_len_wgs)
    geom_after = substring(
        fw_geom, (dist_after / total_len_rd) * total_len_wgs, total_len_wgs
    )

    split_pt = Point(geom_before.coords[-1])
    merge_pt = Point(geom_after.coords[0])
    return split_pt, merge_pt


def test_buffer_computation_places_split_outside_chambers():
    """
    _compute_asymmetric_lock_buffers must produce buffers that place the split
    point outside every chamber polygon.

    Uses the same Weurt-like geometry as the fixture (E-W fairway, two chambers
    flanking the fairway) but drives the full buffer computation path rather than
    pre-setting geometry_before/after_wkt.
    """
    fw_geom = LineString([(5.808, 51.8538), (5.838, 51.8538)])
    lock_geom = Point(5.822, 51.8538)

    ch_north = Polygon([
        (5.819, 51.854), (5.825, 51.854),
        (5.825, 51.856), (5.819, 51.856),
        (5.819, 51.854),
    ])
    ch_south = Polygon([
        (5.819, 51.852), (5.825, 51.852),
        (5.825, 51.854), (5.819, 51.854),
        (5.819, 51.852),
    ])

    lock_chambers_df = pd.DataFrame([
        {"id": "40927", "geometry": ch_north.wkt, "dim_usable_length": 250},
        {"id": "47538", "geometry": ch_south.wkt, "dim_usable_length": 250},
    ])

    split_pt, merge_pt = _simulate_split_merge_from_buffers(
        fw_geom, lock_geom, lock_chambers_df
    )

    for ch_id, ch_poly in [("40927", ch_north), ("47538", ch_south)]:
        assert not ch_poly.contains(split_pt), (
            f"split_pt {split_pt.wkt} landed inside chamber {ch_id}"
        )
        assert not ch_poly.contains(merge_pt), (
            f"merge_pt {merge_pt.wkt} landed inside chamber {ch_id}"
        )


def test_buffer_computation_places_merge_outside_downstream_chamber():
    """
    When a single chamber extends far downstream of the lock centroid, the
    computed merge point must still land downstream of that chamber's exit door.

    This exercises the specific failure mode from the real Sluis Weurt data
    where the merge node landed inside chamber 47538.
    """
    # Fairway still E-W; the "lock" centroid is near the western end of the
    # chamber so the downstream extent is large.
    fw_geom = LineString([(5.808, 51.8538), (5.838, 51.8538)])
    # Lock centroid at the western edge of the chamber extent (lon 5.819)
    lock_geom = Point(5.819, 51.8538)

    # Single large chamber extending from 5.819 to 5.825 (≈ 410 m wide)
    ch_big = Polygon([
        (5.819, 51.852), (5.825, 51.852),
        (5.825, 51.854), (5.819, 51.854),
        (5.819, 51.852),
    ])

    lock_chambers_df = pd.DataFrame([
        {"id": "47538", "geometry": ch_big.wkt, "dim_usable_length": 250},
    ])

    split_pt, merge_pt = _simulate_split_merge_from_buffers(
        fw_geom, lock_geom, lock_chambers_df
    )

    assert not ch_big.contains(merge_pt), (
        f"merge_pt {merge_pt.wkt} landed inside the downstream chamber"
    )
