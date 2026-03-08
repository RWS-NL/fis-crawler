import pytest
from shapely.geometry import LineString, Point

from fis.splicer import SplicedSegment
from fis.bridge.graph import BridgeComplex, BridgeOpening


@pytest.fixture
def dummy_segments():
    seg1 = SplicedSegment(
        geometry=LineString([(0, 0), (50, 0)]),
        start_distance=0,
        end_distance=50,
        target_obstacle_id="B1",
    )
    seg2 = SplicedSegment(
        geometry=LineString([(55, 0), (100, 0)]),
        start_distance=55,
        end_distance=100,
        source_obstacle_id="B1",
    )
    return seg1, seg2


def test_bridge_graph_generation(dummy_segments):
    b1 = BridgeComplex(
        id="B1",
        name="Test Bridge",
        geometry=Point(52.5, 0),
        openings=[
            BridgeOpening(id="O1", width=10.0, height=5.0),
            BridgeOpening(id="O2", width=12.0, height=None),  # No height restriction
        ],
    )

    seg1, seg2 = dummy_segments
    graph = b1.get_internal_graph(approach_segment=seg1, next_segment=seg2)

    # Check node generation
    assert "bridge_split_B1" in graph.nodes
    assert "bridge_merge_B1" in graph.nodes

    # split should be at end of approach (50, 0)
    # merge should be at start of next (55, 0)
    split_geom = graph.nodes["bridge_split_B1"]["geometry"]
    merge_geom = graph.nodes["bridge_merge_B1"]["geometry"]

    assert split_geom.x == 50
    assert merge_geom.x == 55

    # Check edge generation (parallel)
    edges = list(graph.edges(keys=True, data=True))
    assert len(edges) == 2

    # Verify both edges go from split -> merge and contain proper attrs
    for u, v, k, d in edges:
        assert u == "bridge_split_B1"
        assert v == "bridge_merge_B1"
        assert d["type"] == "bridge_passage"
        if d["opening_id"] == "O1":
            assert d["width"] == 10.0
            assert d["height"] == 5.0
        elif d["opening_id"] == "O2":
            assert d["width"] == 12.0
            assert d["height"] is None
