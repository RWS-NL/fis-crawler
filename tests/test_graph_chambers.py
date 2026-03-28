from shapely.geometry import Point, LineString
from fis.lock.graph import _build_chamber_route_features


def test_build_chamber_route_features():
    # Mock input data
    c = {
        "id": 100,
        "fairway_id": 200,
        "fairway_name": "Test Fairway",
        "sections": [{"id": 300, "geometry": LineString([(0, 10), (0, -10)]).wkt}],
    }
    chamber_id = 999
    chamber_node_start_id = f"chamber_{chamber_id}_start"
    chamber_node_end_id = f"chamber_{chamber_id}_end"

    # Simple straight line scenario
    door_start = Point(0, 5)
    door_end = Point(0, -5)
    split_point = Point(0, 10)
    merge_point = Point(0, -10)

    split_node_id = f"lock_{c['id']}_split"
    merge_node_id = f"lock_{c['id']}_merge"

    chamber = {"dim_length": 100, "dim_width": 12}

    from fis import utils

    lock_id = utils.stringify_id(c["id"])
    fairway_id = utils.stringify_id(c.get("fairway_id"))

    features = _build_chamber_route_features(
        c,
        lock_id,
        fairway_id,
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

    assert len(features) == 5, "Should generate 5 features: 2 nodes + 3 segments"

    feature_types = [f["properties"]["feature_type"] for f in features]
    assert feature_types.count("node") == 2
    assert feature_types.count("fairway_segment") == 3

    # Check Approach Segment
    approach_segments = [
        f for f in features if f["properties"].get("segment_type") == "chamber_approach"
    ]
    assert len(approach_segments) == 1
    approach = approach_segments[0]
    assert approach["properties"]["source_node"] == split_node_id
    assert approach["properties"]["target_node"] == chamber_node_start_id
    assert approach["properties"]["length_m"] > 0
    assert len(approach["geometry"]["coordinates"]) == 2

    # Check Chamber Route Segment
    route_segments = [
        f for f in features if f["properties"].get("segment_type") == "chamber_route"
    ]
    assert len(route_segments) == 1
    route = route_segments[0]
    assert route["properties"]["source_node"] == chamber_node_start_id
    assert route["properties"]["target_node"] == chamber_node_end_id
    assert route["properties"]["length_m"] > 0

    # Check Exit Segment
    exit_segments = [
        f for f in features if f["properties"].get("segment_type") == "chamber_exit"
    ]
    assert len(exit_segments) == 1
    exit_seg = exit_segments[0]
    assert exit_seg["properties"]["source_node"] == chamber_node_end_id
    assert exit_seg["properties"]["target_node"] == merge_node_id
    assert exit_seg["properties"]["length_m"] > 0


def test_lock_overlapping_multiple_sections():
    """
    Test that when a lock overlaps two fairway sections meeting in the middle,
    the approach and exit segments get the correct section IDs based on geometry,
    rather than just the first section in the list.
    """
    from shapely.geometry import LineString, Polygon
    from fis.lock.graph import build_graph_features

    # Construct a lock complex spanning from x=0 to x=100
    # Two sections:
    #   "21084" from x=-50 to x=50 (intersecting the start/approach)
    #   "7849" from x=50 to x=150 (intersecting the end/exit)
    # The lock chamber goes from x=20 to x=80

    c = {
        "id": "51064",
        "geometry": Polygon([(0, -10), (100, -10), (100, 10), (0, 10), (0, -10)]).wkt,
        "geometry_before_wkt": LineString(
            [(-50, 0), (0, 0)]
        ).wkt,  # split point at (0, 0)
        "geometry_after_wkt": LineString(
            [(100, 0), (150, 0)]
        ).wkt,  # merge point at (100, 0)
        "fairway_id": "fw1",
        "sections": [
            {
                "id": "21084",
                "geometry": LineString([(-50, 0), (50, 0)]).wkt,
                "relation": "overlap",
            },
            {
                "id": "7849",
                "geometry": LineString([(50, 0), (150, 0)]).wkt,
                "relation": "overlap",
            },
        ],
        "locks": [
            {
                "id": "lock1",
                "chambers": [
                    {
                        "id": "24969",
                        "geometry": Polygon(
                            [(20, -5), (80, -5), (80, 5), (20, 5), (20, -5)]
                        ).wkt,
                        "dim_length": 60,
                        "dim_width": 10,
                    }
                ],
            }
        ],
    }

    # Generate graph features
    features = build_graph_features([c])

    # Extract the approach, route and exit segments
    approach = next(
        f
        for f in features
        if f.get("properties", {}).get("segment_type") == "chamber_approach"
    )
    route = next(
        f
        for f in features
        if f.get("properties", {}).get("segment_type") == "chamber_route"
    )
    exit_seg = next(
        f
        for f in features
        if f.get("properties", {}).get("segment_type") == "chamber_exit"
    )

    # Verify the approach segment gets section 21084
    assert approach["properties"]["section_id"] == "21084", (
        "Approach segment should match the start section"
    )

    # Verify the exit segment gets section 7849
    assert exit_seg["properties"]["section_id"] == "7849", (
        "Exit segment should match the end section"
    )

    # Route segment should also be assigned to one of the overlapping sections
    assert route["properties"]["section_id"] in ("21084", "7849")


def test_lock_section_fallback_proximity():
    """
    Verify that the midpoint-distance fallback is used when no sections
    have a significant overlap (>1mm) with the segment.
    """
    from shapely.geometry import LineString, Polygon
    from fis.lock.graph import build_graph_features

    # Lock from x=0 to x=100
    # Fairway is at y=0.
    # We place two sections offset slightly in Y so they don't intersect,
    # but "sec2" is closer to the midpoint (x=10, y=0) of the approach.
    c = {
        "id": "51064",
        "geometry": Polygon([(0, -10), (100, -10), (100, 10), (0, 10), (0, -10)]).wkt,
        "geometry_before_wkt": LineString([(-50, 0), (0, 0)]).wkt,
        "geometry_after_wkt": LineString([(100, 0), (150, 0)]).wkt,
        "fairway_id": "fw1",
        "sections": [
            {
                "id": "sec1",
                "geometry": LineString([(-10, 5), (110, 5)]).wkt,  # 5m away
                "relation": "overlap",
            },
            {
                "id": "sec2",
                "geometry": LineString([(-10, 1), (110, 1)]).wkt,  # Only 1m away
                "relation": "overlap",
            },
        ],
        "locks": [
            {
                "id": "lock1",
                "chambers": [
                    {
                        "id": "24969",
                        "geometry": Polygon(
                            [(20, -5), (80, -5), (80, 5), (20, 5), (20, -5)]
                        ).wkt,
                        "dim_length": 60,
                        "dim_width": 10,
                    }
                ],
            }
        ],
    }

    features = build_graph_features([c])

    # No section intersects the approach segment.
    # The midpoint of approach should pick "sec2" (the closer one).
    approach = next(
        f
        for f in features
        if f.get("properties", {}).get("segment_type") == "chamber_approach"
    )
    assert approach["properties"]["section_id"] == "sec2"


def test_lock_section_point_touch_ignored():
    """
    Verify that if one section touches only at a point (length 0),
    and another section is closer to the midpoint, the midpoint selection wins.
    """
    from shapely.geometry import LineString, Polygon
    from fis.lock.graph import build_graph_features

    # Lock split point at (0,0).
    # Approach segment from (0,0) to (10,0).
    # sec1 touches at (0,0) exactly.
    # sec2 is offset by 1m but covers the length.
    c = {
        "id": "51064",
        "geometry": Polygon([(0, -10), (100, -10), (100, 10), (0, 10), (0, -10)]).wkt,
        "geometry_before_wkt": LineString([(-50, 0), (0, 0)]).wkt,
        "geometry_after_wkt": LineString([(100, 0), (150, 0)]).wkt,
        "fairway_id": "fw1",
        "sections": [
            {
                "id": "sec1",
                "geometry": LineString([(-50, 0), (0, 0)]).wkt,  # Touches at (0,0)
                "relation": "overlap",
            },
            {
                "id": "sec2",
                "geometry": LineString([(0, 1), (100, 1)]).wkt,  # Near midpoint
                "relation": "overlap",
            },
        ],
        "locks": [
            {
                "id": "lock1",
                "chambers": [
                    {
                        "id": "24969",
                        "geometry": Polygon(
                            [(20, -5), (80, -5), (80, 5), (20, 5), (20, -5)]
                        ).wkt,
                        "dim_length": 60,
                        "dim_width": 10,
                    }
                ],
            }
        ],
    }

    features = build_graph_features([c])

    # The approach segment from (0,0) to door_start.
    # It point-touches sec1 at (0,0). Intersection is a Point, length 0.
    # It should pick sec2 because sec2 is closer to the midpoint (5,0) than sec1.
    approach = next(
        f
        for f in features
        if f.get("properties", {}).get("segment_type") == "chamber_approach"
    )
    assert approach["properties"]["section_id"] == "sec2"
