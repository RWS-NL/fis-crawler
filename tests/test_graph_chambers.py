from shapely.geometry import Point
from fis.lock.graph import _build_chamber_route_features


def test_build_chamber_route_features():
    # Mock input data
    c = {
        "id": 100,
        "fairway_id": 200,
        "fairway_name": "Test Fairway",
        "sections": [{"id": 300}],
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

    features = _build_chamber_route_features(
        c,
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
