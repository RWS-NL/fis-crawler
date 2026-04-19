"""
Unit tests for issues #143 and #145.

Tests cover:
- Chamber route edges are split at internal FIS junction nodes.
- Asymmetric buffering: process_fairway_geometry respects buffer_before_m / buffer_after_m.
- detect_complex_groups groups locks that share boundary junctions.
- identify_embedded_structures rejects openings that are outside chamber polygons.
"""

import networkx as nx
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
from shapely import wkt
from unittest.mock import patch


# ---------------------------------------------------------------------------
# 1. Internal junction nodes in chamber routes
# ---------------------------------------------------------------------------


def test_chamber_route_with_internal_junctions_splits_correctly():
    """
    When a chamber dict carries internal_junctions, _build_chamber_route_features
    must insert each junction as an intermediate node on the chamber_route edges
    and split the single chamber_route segment into sub-segments.
    """
    from fis.lock.graph import _build_chamber_route_features

    c = {
        "id": "100",
        "fairway_id": "200",
        "fairway_name": "Test Fairway",
        "sections": [{"id": "300", "geometry": LineString([(0, 10), (0, -10)]).wkt}],
    }
    chamber_id = "999"
    chamber_node_start_id = f"chamber_{chamber_id}_start"
    chamber_node_end_id = f"chamber_{chamber_id}_end"

    door_start = Point(0, 5)
    door_end = Point(0, -5)
    split_point = Point(0, 10)
    merge_point = Point(0, -10)

    split_node_id = "lock_100_split"
    merge_node_id = "lock_100_merge"

    # Internal junction placed at (0, 0) – between door_start and door_end
    internal_junction_id = "junc_9999"
    chamber = {
        "dim_usable_length": 100,
        "dim_gate_width": 12,
        "internal_junctions": [
            {"id": internal_junction_id, "geometry": Point(0, 0)},
        ],
    }

    features = _build_chamber_route_features(
        c,
        "100",
        "200",
        chamber,
        chamber_id,
        chamber_node_start_id,
        chamber_node_end_id,
        door_start,
        door_end,
        split_point,
        merge_point,
        split_node_id,
        merge_node_id,
    )

    node_ids = {
        f["properties"]["id"]
        for f in features
        if f["properties"]["feature_type"] == "node"
    }
    segment_types = [
        f["properties"]["segment_type"]
        for f in features
        if f["properties"]["feature_type"] == "fairway_segment"
    ]

    # There should now be 3 nodes: chamber_start, internal_junction, chamber_end
    assert chamber_node_start_id in node_ids
    assert chamber_node_end_id in node_ids
    assert internal_junction_id in node_ids, (
        "Internal junction node must be emitted as a separate graph node"
    )

    # The chamber route should be split into 2 sub-segments
    route_segs = [s for s in segment_types if s == "chamber_route"]
    assert len(route_segs) == 2, (
        f"Expected 2 chamber_route sub-segments (split at internal junction), got {len(route_segs)}"
    )

    # Build a small graph and verify connectivity
    G = nx.DiGraph()
    for f in features:
        p = f["properties"]
        if p.get("feature_type") == "fairway_segment":
            G.add_edge(p["source_node"], p["target_node"])

    assert nx.has_path(G, split_node_id, internal_junction_id)
    assert nx.has_path(G, internal_junction_id, merge_node_id)
    path = nx.shortest_path(G, chamber_node_start_id, chamber_node_end_id)
    assert internal_junction_id in path, (
        "Internal junction must lie on the path from chamber_start to chamber_end"
    )


def test_chamber_route_without_internal_junctions_is_single_segment():
    """
    When no internal_junctions are present, exactly one chamber_route segment
    is emitted (the original single-segment behaviour is preserved).
    """
    from fis.lock.graph import _build_chamber_route_features

    c = {
        "id": "100",
        "fairway_id": "200",
        "fairway_name": "Test Fairway",
        "sections": [{"id": "300", "geometry": LineString([(0, 10), (0, -10)]).wkt}],
    }
    chamber_id = "888"
    chamber = {"dim_usable_length": 100, "dim_gate_width": 12, "internal_junctions": []}

    features = _build_chamber_route_features(
        c,
        "100",
        "200",
        chamber,
        chamber_id,
        f"chamber_{chamber_id}_start",
        f"chamber_{chamber_id}_end",
        Point(0, 5),
        Point(0, -5),
        Point(0, 10),
        Point(0, -10),
        "lock_100_split",
        "lock_100_merge",
    )

    route_segs = [
        f
        for f in features
        if f["properties"].get("segment_type") == "chamber_route"
    ]
    assert len(route_segs) == 1, (
        "Without internal junctions there should be a single chamber_route segment"
    )


def test_chamber_route_multiple_internal_junctions_ordered():
    """
    Multiple internal junctions must be ordered along the door_start→door_end
    direction, not by insertion order.
    """
    from fis.lock.graph import _build_chamber_route_features

    c = {
        "id": "100",
        "fairway_id": "200",
        "fairway_name": "Fairway",
        "sections": [{"id": "300", "geometry": LineString([(0, 15), (0, -15)]).wkt}],
    }
    chamber_id = "777"
    # door_start at y=10, door_end at y=-10 (decreasing y = downstream)
    # junc_B is closer to door_start (y=5), junc_A is further downstream (y=0).
    # The list is intentionally given in reversed traversal order (downstream
    # junc_A first, upstream junc_B second) to verify that the function sorts
    # them by projected distance rather than by input-list position.
    chamber = {
        "dim_usable_length": 200,
        "dim_gate_width": 10,
        "internal_junctions": [
            {"id": "junc_A", "geometry": Point(0, 0)},   # downstream (inserted first)
            {"id": "junc_B", "geometry": Point(0, 5)},   # upstream   (inserted second)
        ],
    }

    features = _build_chamber_route_features(
        c,
        "100",
        "200",
        chamber,
        chamber_id,
        f"chamber_{chamber_id}_start",
        f"chamber_{chamber_id}_end",
        Point(0, 10),   # door_start (upstream)
        Point(0, -10),  # door_end (downstream)
        Point(0, 15),
        Point(0, -15),
        "lock_100_split",
        "lock_100_merge",
    )

    G = nx.DiGraph()
    for f in features:
        p = f["properties"]
        if p.get("feature_type") == "fairway_segment":
            G.add_edge(p["source_node"], p["target_node"])

    # Path must go: start → junc_B → junc_A → end  (not start → junc_A → junc_B → end)
    path = nx.shortest_path(G, f"chamber_{chamber_id}_start", f"chamber_{chamber_id}_end")
    b_idx = path.index("junc_B")
    a_idx = path.index("junc_A")
    assert b_idx < a_idx, (
        "junc_B (closer to door_start) must come before junc_A in traversal order"
    )


# ---------------------------------------------------------------------------
# 2. Asymmetric buffering in process_fairway_geometry
# ---------------------------------------------------------------------------


def test_process_fairway_geometry_asymmetric_buffer():
    """
    When buffer_before_m != buffer_after_m the resulting geometry_before_wkt and
    geometry_after_wkt must be at different distances from the lock centroid.
    """
    from fis.utils import process_fairway_geometry

    # Long horizontal fairway section in approximate NL coordinates (WGS84)
    fw_line = LineString([(5.0, 52.0), (5.5, 52.0)])

    # Lock centroid roughly in the middle
    lock_point = Point(5.25, 52.0)

    class FwRow:
        geometry = fw_line

    class LockRow:
        geometry = lock_point

    result = process_fairway_geometry(
        FwRow(),
        LockRow(),
        buffer_before_m=1000,
        buffer_after_m=2000,
    )

    assert "geometry_before_wkt" in result
    assert "geometry_after_wkt" in result

    g_before = wkt.loads(result["geometry_before_wkt"])
    g_after = wkt.loads(result["geometry_after_wkt"])

    # The before-segment end is the split point; after-segment start is the merge point
    split_x = g_before.coords[-1][0]
    merge_x = g_after.coords[0][0]

    # The gap between split and merge should be larger on the after-side
    lock_x = lock_point.x
    dist_before = lock_x - split_x   # how far upstream the split was pushed
    dist_after = merge_x - lock_x    # how far downstream the merge was pushed

    # buffer_after_m (2000) > buffer_before_m (1000) so dist_after > dist_before
    assert dist_after > dist_before, (
        f"Expected asymmetric buffers: dist_after ({dist_after:.4f}°) should be "
        f"larger than dist_before ({dist_before:.4f}°)"
    )


def test_process_fairway_geometry_symmetric_buffer_unchanged():
    """
    Legacy symmetric buffer_dist param still works when no asymmetric params given.
    The split and merge distances from the lock centroid should be roughly equal.
    """
    from fis.utils import process_fairway_geometry

    fw_line = LineString([(5.0, 52.0), (5.5, 52.0)])
    lock_point = Point(5.25, 52.0)

    class FwRow:
        geometry = fw_line

    class LockRow:
        geometry = lock_point

    result = process_fairway_geometry(FwRow(), LockRow(), buffer_dist=500)

    assert "geometry_before_wkt" in result
    assert "geometry_after_wkt" in result

    g_before = wkt.loads(result["geometry_before_wkt"])
    g_after = wkt.loads(result["geometry_after_wkt"])

    split_x = g_before.coords[-1][0]
    merge_x = g_after.coords[0][0]
    lock_x = lock_point.x

    dist_before = lock_x - split_x
    dist_after = merge_x - lock_x

    # Should be approximately equal (within 10 % relative difference)
    assert abs(dist_before - dist_after) / max(dist_before, dist_after) < 0.1, (
        f"Symmetric buffer should produce roughly equal offsets, got "
        f"before={dist_before:.4f} after={dist_after:.4f}"
    )


def test_process_fairway_geometry_opening_expands_correct_side():
    """
    An opening on the upstream side must expand offset_before only; an opening
    on the downstream side must expand offset_after only.
    """
    from fis.utils import process_fairway_geometry

    fw_line = LineString([(5.0, 52.0), (5.5, 52.0)])
    lock_point = Point(5.25, 52.0)

    class FwRow:
        geometry = fw_line

    class LockRow:
        geometry = lock_point

    # Upstream opening (before the lock, small buffer_before so opening dominates)
    upstream_opening = {"geometry": Point(5.05, 52.0).wkt}
    result_up = process_fairway_geometry(
        FwRow(),
        LockRow(),
        buffer_before_m=50,
        buffer_after_m=50,
        openings_data=[upstream_opening],
    )

    # Downstream opening (after the lock, small buffer_after so opening dominates)
    downstream_opening = {"geometry": Point(5.45, 52.0).wkt}
    result_dn = process_fairway_geometry(
        FwRow(),
        LockRow(),
        buffer_before_m=50,
        buffer_after_m=50,
        openings_data=[downstream_opening],
    )

    # Upstream opening should push the split further back (larger before offset)
    g_up_before = wkt.loads(result_up["geometry_before_wkt"])
    g_up_after = wkt.loads(result_up["geometry_after_wkt"])
    g_dn_before = wkt.loads(result_dn["geometry_before_wkt"])
    g_dn_after = wkt.loads(result_dn["geometry_after_wkt"])

    # With upstream opening: before offset grows, after offset stays at 50m
    # With downstream opening: after offset grows, before offset stays at 50m
    split_up = g_up_before.coords[-1][0]
    split_dn = g_dn_before.coords[-1][0]
    merge_up = g_up_after.coords[0][0]
    merge_dn = g_dn_after.coords[0][0]

    assert split_up < split_dn, (
        "Upstream opening should push split further upstream (smaller x for split)"
    )
    assert merge_dn > merge_up, (
        "Downstream opening should push merge further downstream (larger x for merge)"
    )


# ---------------------------------------------------------------------------
# 3. detect_complex_groups
# ---------------------------------------------------------------------------


def test_detect_complex_groups_no_shared_junctions():
    """
    Two locks with completely disjoint fairway sections should each be in their
    own group.
    """
    from fis.lock.core import detect_complex_groups

    locks = pd.DataFrame(
        [
            {"id": "10", "fairway_id": "fw_a"},
            {"id": "20", "fairway_id": "fw_b"},
        ]
    )
    sections = gpd.GeoDataFrame(
        [
            {
                "id": "s1",
                "fairway_id": "fw_a",
                "start_junction_id": "j1",
                "end_junction_id": "j2",
                "geometry": LineString([(0, 0), (1, 0)]),
            },
            {
                "id": "s2",
                "fairway_id": "fw_b",
                "start_junction_id": "j3",
                "end_junction_id": "j4",
                "geometry": LineString([(2, 0), (3, 0)]),
            },
        ],
        crs="EPSG:4326",
    )

    groups = detect_complex_groups(locks, sections)

    assert len(groups) == 2, "Two independent locks should produce two groups"
    # Each group should have exactly one member
    for gid, members in groups.items():
        assert len(members) == 1, f"Group {gid} should have 1 member, got {members}"


def test_detect_complex_groups_shared_junction_groups_locks():
    """
    Two locks on different fairway sections that share a boundary junction must
    be grouped together – this models Oranjesluizen (50750 and 59464015).
    """
    from fis.lock.core import detect_complex_groups

    locks = pd.DataFrame(
        [
            {"id": "50750", "fairway_id": "fw_a"},
            {"id": "59464015", "fairway_id": "fw_b"},
        ]
    )
    # Both sections share junction "j_shared" as boundary
    sections = gpd.GeoDataFrame(
        [
            {
                "id": "s1",
                "fairway_id": "fw_a",
                "start_junction_id": "j_shared",
                "end_junction_id": "j_end_a",
                "geometry": LineString([(0, 0), (1, 0)]),
            },
            {
                "id": "s2",
                "fairway_id": "fw_b",
                "start_junction_id": "j_shared",
                "end_junction_id": "j_end_b",
                "geometry": LineString([(0, 0), (0, 1)]),
            },
        ],
        crs="EPSG:4326",
    )

    groups = detect_complex_groups(locks, sections)

    assert len(groups) == 1, "Locks sharing a boundary junction should form one group"
    sole_group = next(iter(groups.values()))
    assert set(sole_group) == {"50750", "59464015"}, (
        f"Oranjesluizen locks should be in the same group; got {sole_group}"
    )


def test_detect_complex_groups_three_locks_chain():
    """
    Three locks A-B-C where A shares a junction with B and B shares a junction
    with C should all be in the same group (transitive closure).
    """
    from fis.lock.core import detect_complex_groups

    locks = pd.DataFrame(
        [
            {"id": "A", "fairway_id": "fw_a"},
            {"id": "B", "fairway_id": "fw_b"},
            {"id": "C", "fairway_id": "fw_c"},
        ]
    )
    sections = gpd.GeoDataFrame(
        [
            {
                "id": "s1",
                "fairway_id": "fw_a",
                "start_junction_id": "j1",
                "end_junction_id": "j_ab",
                "geometry": LineString([(0, 0), (1, 0)]),
            },
            {
                "id": "s2",
                "fairway_id": "fw_b",
                "start_junction_id": "j_ab",
                "end_junction_id": "j_bc",
                "geometry": LineString([(1, 0), (2, 0)]),
            },
            {
                "id": "s3",
                "fairway_id": "fw_c",
                "start_junction_id": "j_bc",
                "end_junction_id": "j3",
                "geometry": LineString([(2, 0), (3, 0)]),
            },
        ],
        crs="EPSG:4326",
    )

    groups = detect_complex_groups(locks, sections)

    assert len(groups) == 1
    sole_group = next(iter(groups.values()))
    assert set(sole_group) == {"A", "B", "C"}


# ---------------------------------------------------------------------------
# 4. Polygon intersection check in identify_embedded_structures
# ---------------------------------------------------------------------------


@patch("fis.lock.graph.build_chambers_gdf")
@patch("fis.bridge.graph.build_openings_gdf")
def test_identify_embedded_polygon_chamber_inside(
    mock_build_openings, mock_build_chambers
):
    """
    An opening whose geometry intersects the chamber polygon must be matched
    as embedded.
    """
    from fis.dropins.embedded import identify_embedded_structures
    from fis import settings

    # 10×10 metre chamber polygon
    ch_poly = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    opening_inside = Point(5, 5)  # clearly inside

    chambers_gdf = gpd.GeoDataFrame(
        [{"id": "ch1", "name": "Lock Oost", "geometry": ch_poly}], crs="EPSG:28992"
    ).to_crs(settings.PROJECTED_CRS)

    openings_gdf = gpd.GeoDataFrame(
        [{"id": "op1", "name": "Brug Oost", "geometry": opening_inside}],
        crs="EPSG:28992",
    ).to_crs(settings.PROJECTED_CRS)

    mock_build_chambers.return_value = chambers_gdf
    mock_build_openings.return_value = openings_gdf

    matches = identify_embedded_structures([], [])

    assert "op1" in matches, "Opening inside chamber polygon must be matched as embedded"
    assert matches["op1"]["ch_id"] == "ch1"


@patch("fis.lock.graph.build_chambers_gdf")
@patch("fis.bridge.graph.build_openings_gdf")
def test_identify_embedded_polygon_chamber_outside_rejected(
    mock_build_openings, mock_build_chambers
):
    """
    An opening that is OUTSIDE the chamber polygon (but within 500 m) must NOT
    be matched as embedded.
    """
    from fis.dropins.embedded import identify_embedded_structures
    from fis import settings

    # 10×10 metre chamber polygon at origin
    ch_poly = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
    opening_adjacent = Point(50, 5)  # 40 m outside the polygon

    chambers_gdf = gpd.GeoDataFrame(
        [{"id": "ch1", "name": "Lock Sluis", "geometry": ch_poly}], crs="EPSG:28992"
    ).to_crs(settings.PROJECTED_CRS)

    openings_gdf = gpd.GeoDataFrame(
        [{"id": "op_adj", "name": "Brug Sluis", "geometry": opening_adjacent}],
        crs="EPSG:28992",
    ).to_crs(settings.PROJECTED_CRS)

    mock_build_chambers.return_value = chambers_gdf
    mock_build_openings.return_value = openings_gdf

    matches = identify_embedded_structures([], [])

    assert "op_adj" not in matches, (
        "Opening outside chamber polygon must NOT be classified as embedded"
    )


@patch("fis.lock.graph.build_chambers_gdf")
@patch("fis.bridge.graph.build_openings_gdf")
def test_identify_embedded_point_chamber_legacy_behaviour(
    mock_build_openings, mock_build_chambers
):
    """
    For Point-geometry chambers (legacy test data), the old distance-and-scoring
    behaviour must be preserved (no intersection requirement).
    """
    from fis.dropins.embedded import identify_embedded_structures
    from fis import settings

    chambers_gdf = gpd.GeoDataFrame(
        [{"id": "ch1", "name": "Main Lock", "geometry": Point(0, 0)}],
        crs="EPSG:28992",
    ).to_crs(settings.PROJECTED_CRS)

    openings_gdf = gpd.GeoDataFrame(
        [{"id": "op1", "name": "Main Bridge", "geometry": Point(10, 0)}],
        crs="EPSG:28992",
    ).to_crs(settings.PROJECTED_CRS)

    mock_build_chambers.return_value = chambers_gdf
    mock_build_openings.return_value = openings_gdf

    matches = identify_embedded_structures([], [])

    # Legacy: Point chamber at distance 10 m should still be matched
    assert "op1" in matches, (
        "Point-geometry chambers must use legacy distance matching (no intersection check)"
    )
