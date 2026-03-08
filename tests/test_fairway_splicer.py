import pytest
from shapely.geometry import LineString, Point
from fis.splicer import FairwaySplicer, ObstacleCut


@pytest.fixture
def straight_fairway():
    # A simple straight line from (0,0) to (100,0)
    return LineString([(0, 0), (100, 0)])


def test_splicer_no_obstacles(straight_fairway):
    splicer = FairwaySplicer(straight_fairway)
    segments = splicer.splice([])

    assert len(segments) == 1
    assert segments[0].start_distance == 0.0
    assert segments[0].end_distance == 100.0
    assert segments[0].source_obstacle_id is None
    assert segments[0].target_obstacle_id is None
    assert segments[0].geometry.equals(straight_fairway)


def test_splicer_single_obstacle(straight_fairway):
    splicer = FairwaySplicer(straight_fairway)
    obs = ObstacleCut(
        id="L1", geometry=Point(50, 0), projected_distance=50.0, buffer_distance=5.0
    )

    segments = splicer.splice([obs])

    # Expect 2 segments: (0 to 45) and (55 to 100)
    assert len(segments) == 2

    assert segments[0].start_distance == 0.0
    assert segments[0].end_distance == 45.0
    assert segments[0].source_obstacle_id is None
    assert segments[0].target_obstacle_id == "L1"

    assert segments[1].start_distance == 55.0
    assert segments[1].end_distance == 100.0
    assert segments[1].source_obstacle_id == "L1"
    assert segments[1].target_obstacle_id is None


def test_splicer_consecutive_obstacles(straight_fairway):
    splicer = FairwaySplicer(straight_fairway)
    obs1 = ObstacleCut(
        id="L1", geometry=Point(20, 0), projected_distance=20.0, buffer_distance=2.0
    )
    obs2 = ObstacleCut(
        id="B1", geometry=Point(80, 0), projected_distance=80.0, buffer_distance=3.0
    )

    # Deliberately unordered to test the splicer's sorting
    segments = splicer.splice([obs2, obs1])

    # Expect 3 segments: (0 to 18), (22 to 77), (83 to 100)
    assert len(segments) == 3

    assert segments[0].end_distance == 18.0
    assert segments[0].target_obstacle_id == "L1"

    assert segments[1].start_distance == 22.0
    assert segments[1].end_distance == 77.0
    assert segments[1].source_obstacle_id == "L1"
    assert segments[1].target_obstacle_id == "B1"

    assert segments[2].start_distance == 83.0
    assert segments[2].source_obstacle_id == "B1"
    assert segments[2].target_obstacle_id is None


def test_splicer_obstacles_overlapping_buffer(straight_fairway):
    splicer = FairwaySplicer(straight_fairway)
    # L1 and L2 are very close, their buffers overlap
    obs1 = ObstacleCut(
        id="L1", geometry=Point(40, 0), projected_distance=40.0, buffer_distance=10.0
    )
    obs2 = ObstacleCut(
        id="L2", geometry=Point(45, 0), projected_distance=45.0, buffer_distance=10.0
    )

    segments = splicer.splice([obs1, obs2])

    # Segment 1: Start to L1's begin buffer (0 to 30)
    # The gap between L1 end buffer (50) and L2 start buffer (35) is negative!
    # Segment 2 should not exist because there is no space between the two drops-in.
    # Segment 3: L2's end buffer to End (55 to 100)

    assert len(segments) == 2

    assert segments[0].start_distance == 0.0
    assert segments[0].end_distance == 30.0
    assert segments[0].target_obstacle_id == "L1"

    assert segments[1].start_distance == 55.0
    assert segments[1].end_distance == 100.0
    assert segments[1].source_obstacle_id == "L2"
