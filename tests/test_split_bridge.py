import pytest
from pathlib import Path
import geopandas as gpd
import pandas as pd
import networkx as nx
from fis.dropins.core import build_integrated_dropins_graph
from fis import utils


@pytest.fixture(scope="module")
def split_test_graph(tmp_path_factory):
    """
    Generate a simplified graph from the test data subset to test splitting behavior.
    """
    export_dir = Path("tests/data/fis-export")
    disk_dir = Path("tests/data/disk-export")
    output_dir = tmp_path_factory.mktemp("split_test_simplified")

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


def test_bridge_35761_has_bridge_passage(split_test_graph):
    """
    Regression test: Bridge 35761 should have a corresponding bridge_passage edge.
    """
    G, edges = split_test_graph

    bridge_passages = edges[edges["segment_type"] == "bridge_passage"]
    # Look for structure_id '35761' (standardized string)
    matches = bridge_passages[bridge_passages["structure_id"] == "35761"]

    assert len(matches) > 0, "Bridge 35761 is missing its bridge_passage edge."


def test_bridge_34113_splits_fairway_12821(split_test_graph):
    """
    Bug 1: Bridge 34113 is on fairway 12821.
    It should split the fairway even if considered 'embedded' because we are in simplified mode.
    """
    G, edges = split_test_graph

    bridge_passages = edges[edges["segment_type"] == "bridge_passage"]
    # Look for structure_id '34113'
    matches = bridge_passages[bridge_passages["structure_id"] == "34113"]

    assert len(matches) > 0, "Bridge 34113 passage not found in the graph."

    b_edge = matches.iloc[0]

    # Check neighbors to verify connectivity and section retention
    # In simplified mode, it's connected to other segments of section 12821
    in_edges = list(G.in_edges(b_edge.source_node, data=True))
    out_edges = list(G.out_edges(b_edge.target_node, data=True))

    has_12821_in = any(
        utils.stringify_id(data.get("section_id")) == "12821" for u, v, data in in_edges
    )
    has_12821_out = any(
        utils.stringify_id(data.get("section_id")) == "12821"
        for u, v, data in out_edges
    )

    assert has_12821_in or has_12821_out, (
        "Fairway 12821 is not properly split by bridge 34113."
    )
    assert utils.stringify_id(b_edge.get("section_id")) == "12821", (
        "Bridge 34113 passage is missing section_id 12821."
    )


def test_lock_passage_42863_retains_section_id_12821(split_test_graph):
    """
    Bug 2: Fairway section lock_passage_42863 should have section_id 12821.
    """
    G, edges = split_test_graph

    lock_passages = edges[edges["segment_type"] == "lock_passage"]
    matches = lock_passages[lock_passages["structure_id"] == "42863"]

    assert len(matches) > 0, "Lock passage 42863 not found in the graph."

    l_edge = matches.iloc[0]

    assert utils.stringify_id(l_edge.get("section_id")) == "12821", (
        f"lock_passage_42863 missing section_id 12821, got {l_edge.get('section_id')}"
    )
