import pytest
from shapely.geometry import LineString, Point
from fis.splicer import FairwaySplicer, StructureCut


@pytest.fixture
def straight_fairway():
    # A 100m straight line from (0,0) to (100,0)
    return LineString([(0, 0), (100, 0)])


def test_splicer_no_structures(straight_fairway):
    splicer = FairwaySplicer(straight_fairway)
    segments = splicer.splice([])

    assert len(segments) == 1
    assert segments[0].geometry.length == 100.0
    assert segments[0].source_structure_id is None
    assert segments[0].target_structure_id is None


def test_splicer_single_structure(straight_fairway):
    splicer = FairwaySplicer(straight_fairway)
    obs = StructureCut(
        id="L1",
        geometry=Point(50, 0),
        projected_distance=50.0,
        buffer_before=10.0,
        buffer_after=10.0,
    )
    segments = splicer.splice([obs])

    # Should have two segments: [0, 40] and [60, 100]
    assert len(segments) == 2
    assert segments[0].geometry.length == 40.0
    assert segments[0].source_structure_id is None
    assert segments[0].target_structure_id == "L1"

    assert segments[1].geometry.length == 40.0
    assert segments[1].source_structure_id == "L1"
    assert segments[1].target_structure_id is None


def test_splicer_consecutive_structures(straight_fairway):
    splicer = FairwaySplicer(straight_fairway)
    obs1 = StructureCut(
        id="L1",
        geometry=Point(30, 0),
        projected_distance=30.0,
        buffer_before=5.0,
        buffer_after=5.0,
    )
    obs2 = StructureCut(
        id="B1",
        geometry=Point(70, 0),
        projected_distance=70.0,
        buffer_before=5.0,
        buffer_after=5.0,
    )
    segments = splicer.splice([obs1, obs2])

    # Segments: [0, 25], [35, 65], [75, 100]
    assert len(segments) == 3
    assert segments[0].geometry.length == 25.0
    assert segments[0].target_structure_id == "L1"

    assert segments[1].geometry.length == 30.0
    assert segments[1].source_structure_id == "L1"
    assert segments[1].target_structure_id == "B1"

    assert segments[2].geometry.length == 25.0
    assert segments[2].source_structure_id == "B1"
    assert segments[2].target_structure_id is None


def test_splicer_structures_overlapping_buffer(straight_fairway):
    splicer = FairwaySplicer(straight_fairway)
    # L1 at 30 with 10m buffer ends at 40
    obs1 = StructureCut(
        id="L1",
        geometry=Point(30, 0),
        projected_distance=30.0,
        buffer_before=10.0,
        buffer_after=10.0,
    )
    # L2 at 45 with 10m buffer starts at 35 (overlaps L1 buffer)
    obs2 = StructureCut(
        id="L2",
        geometry=Point(45, 0),
        projected_distance=45.0,
        buffer_before=10.0,
        buffer_after=10.0,
    )
    segments = splicer.splice([obs1, obs2])

    # Expected:
    # Seg 1: [0, 20] (ends at L1)
    # Gap: [20, 40] (L1)
    # Gap: [40, 55] (L2) - Note: the logic max(current_dist, start_of_cut) handles this.
    # Seg 2: [55, 100] (starts at L2)
    assert len(segments) == 2
    assert segments[0].geometry.length == pytest.approx(20.0)
    assert segments[0].target_structure_id == "L1"

    assert segments[1].geometry.length == pytest.approx(45.0)
    assert segments[1].source_structure_id == "L2"
