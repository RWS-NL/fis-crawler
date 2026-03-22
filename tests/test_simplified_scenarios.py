from pathlib import Path
import geopandas as gpd
import pandas as pd
import networkx as nx
import pytest
from fis.dropins.core import build_integrated_dropins_graph


@pytest.fixture
def simplified_graph(tmp_path):
    """
    Generate a simplified graph from the test data subset.
    """
    export_dir = Path("tests/data/fis-export")
    disk_dir = Path("tests/data/disk-export")
    output_dir = tmp_path / "simplified"
    output_dir.mkdir()

    if not export_dir.exists():
        pytest.skip("Test data subset not found")

    build_integrated_dropins_graph(
        export_dir=export_dir,
        disk_dir=disk_dir,
        output_dir=output_dir,
        mode="simplified",
    )

    edges = gpd.read_parquet(output_dir / "edges.geoparquet")
    G = nx.DiGraph()
    for _, edge in edges.iterrows():
        if pd.notna(edge.source_node) and pd.notna(edge.target_node):
            G.add_edge(edge.source_node, edge.target_node, **edge.to_dict())
    return G, edges


def test_simplified_bridge_attributes(simplified_graph):
    """
    Verify that standalone bridges are simplified and have aggregated attributes.
    """
    G, edges = simplified_graph

    # Erasmusbrug is not in the Volkerak/Krammer subset,
    # but there are standalone bridges in that area.
    # Let's find a bridge passage.
    bridge_passages = edges[edges["segment_type"] == "bridge_passage"]
    assert len(bridge_passages) > 0, "No bridge passages found in simplified graph"

    for _, edge in bridge_passages.iterrows():
        assert edge.structure_type == "bridge"
        assert pd.notna(edge.structure_id)
        # Check for aggregated dimensions (at least one should be present)
        assert "dim_width" in edge or "dim_height" in edge
        assert pd.notna(edge.constituent_ids)


def test_simplified_lock_topology(simplified_graph):
    """
    Verify that locks are collapsed to single passage edges in simplified mode.
    """
    G, edges = simplified_graph

    lock_passages = edges[edges["segment_type"] == "lock_passage"]
    assert len(lock_passages) > 0, "No lock passages found in simplified graph"

    for _, edge in lock_passages.iterrows():
        assert edge.structure_type == "lock"
        assert pd.notna(edge.structure_id)

        # In simplified mode, there should be no "chamber_start" or "opening_start" nodes
        # for this lock in the global graph.
        # (Though they might exist if other tests ran, but here we have a fresh G).
        nodes = list(G.nodes)
        for node in nodes:
            if not isinstance(node, str):
                continue
            assert not node.startswith("chamber_"), (
                f"Micro-node {node} should not exist in simplified mode"
            )
            assert not node.startswith("opening_"), (
                f"Micro-node {node} should not exist in simplified mode"
            )


def test_simplified_connectivity(simplified_graph):
    """
    Ensure the network remains connected through simplified passages.
    """
    G, edges = simplified_graph

    # Find a lock passage and verify we can go from its split to its merge
    lock_passages = edges[edges["segment_type"] == "lock_passage"]
    sample_lock = lock_passages.iloc[0]

    assert nx.has_path(G, sample_lock.source_node, sample_lock.target_node)
    path = nx.shortest_path(G, sample_lock.source_node, sample_lock.target_node)
    assert len(path) == 2  # Direct edge in simplified mode
