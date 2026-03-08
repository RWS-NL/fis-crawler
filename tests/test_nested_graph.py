import networkx as nx
from shapely.geometry import LineString, Point

from fis.splicer import FairwaySplicer, ObstacleCut
from fis.bridge.graph import BridgeComplex, BridgeOpening


def test_nested_lock_and_bridge():
    """
    Integration test verifying that FairwaySplicer correctly splits a line
    such that consecutive obstacles can be glued together into a continuous
    networkx routing graph.
    """
    fairway = LineString([(0, 0), (100, 0)])

    # 1. Define Obstacles
    obs_lock = ObstacleCut(
        id="L1", geometry=Point(30, 0), projected_distance=30.0, buffer_distance=2.0
    )
    obs_bridge = ObstacleCut(
        id="B1", geometry=Point(70, 0), projected_distance=70.0, buffer_distance=2.0
    )

    # 2. Splice the Fairway
    splicer = FairwaySplicer(fairway)
    segments = splicer.splice([obs_lock, obs_bridge])

    # We should have 3 segments
    assert len(segments) == 3

    # 3. Build the Integrated Graph
    G = nx.MultiDiGraph()

    # Add fairway segments as edges
    for i, seg in enumerate(segments):
        start_node = (
            f"node_{seg.start_distance}"
            if not seg.source_obstacle_id
            else f"merge_{seg.source_obstacle_id}"
        )
        end_node = (
            f"node_{seg.end_distance}"
            if not seg.target_obstacle_id
            else f"split_{seg.target_obstacle_id}"
        )

        G.add_node(start_node)
        G.add_node(end_node)
        G.add_edge(start_node, end_node, type="fairway_segment")

    # Mock Lock Generation
    # Lock is at L1. It spans from split_L1 to merge_L1
    G.add_edge("split_L1", "merge_L1", type="lock_passage")

    # Bridge Generation (using our real domain object)
    b1 = BridgeComplex(
        id="B1",
        name="Test Bridge",
        geometry=Point(70, 0),
        openings=[BridgeOpening(id="O1", width=10.0, height=None)],
    )
    # The bridge approaches are segments[1] and segments[2]
    bridge_graph = b1.get_internal_graph(
        approach_segment=segments[1], next_segment=segments[2]
    )

    # Compose the bridge domain graph into the master graph
    # NetworkX compose handles merging matching node IDs directly
    # Note: Our bridge graph yields 'bridge_split_B1' instead of 'split_B1',
    # so we need to map the naming or adapt. For this test, we rewrite bridge nodes to match.
    mapping = {"bridge_split_B1": "split_B1", "bridge_merge_B1": "merge_B1"}
    bridge_graph = nx.relabel_nodes(bridge_graph, mapping)
    G = nx.compose(G, bridge_graph)

    # 4. Verify Connectivity!
    # A path must exist from the very beginning (0.0) to the very end (100.0)
    start_node = "node_0.0"
    end_node = "node_100.0"

    assert nx.has_path(G, start_node, end_node), "Graph is disconnected!"

    paths = list(nx.all_simple_paths(G, start_node, end_node))
    assert len(paths) == 1

    # Expected Path:
    # node_0.0 -> split_L1 -> merge_L1 -> split_B1 -> merge_B1 -> node_100.0
    expected_path = [
        "node_0.0",
        "split_L1",
        "merge_L1",
        "split_B1",
        "merge_B1",
        "node_100.0",
    ]
    assert paths[0] == expected_path
