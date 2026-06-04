#!/usr/bin/env python
import pathlib
import pickle
import networkx as nx


def main():
    print("Extracting lock sub-graph fixtures...")
    output_dir = pathlib.Path("tests/data/fixtures")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load the base graph
    graph_path = pathlib.Path("output/merged-graph/graph.pickle")
    if not graph_path.exists():
        print(f"Error: {graph_path} not found. Please run 'fis graph merge' first.")
        return

    with open(graph_path, "rb") as f:
        G = pickle.load(f)

    # The 14 validated lock complexes with entry/exit nodes
    complexes = [
        {"name": "volkeraksluizen", "entry": "FIS_8860743", "exit": "FIS_8866727"},
        {"name": "krammersluizen", "entry": "FIS_8864545", "exit": "FIS_8866367"},
        {"name": "oranjesluizen", "entry": "FIS_8864384", "exit": "FIS_59275858"},
        {"name": "ijmuiden_sluizen", "entry": "FIS_8864991", "exit": "FIS_8861863"},
        {"name": "terneuzen_sluizen", "entry": "FIS_8867489", "exit": "FIS_8863105"},
        {"name": "lorentzsluizen", "entry": "FIS_8864239", "exit": "FIS_8860933"},
        {"name": "sluis_weurt", "entry": "FIS_8864666", "exit": "FIS_8865102"},
        {"name": "sluis_eefde", "entry": "FIS_8860918", "exit": "FIS_30986757"},
        {"name": "sluis_born", "entry": "FIS_8868208", "exit": "FIS_8867148"},
        {"name": "sluis_maasbracht", "entry": "FIS_8861292", "exit": "FIS_8862583"},
        {"name": "sluis_heel", "entry": "FIS_8864929", "exit": "FIS_8865890"},
        {"name": "sluis_grave", "entry": "FIS_8861448", "exit": "FIS_8865198"},
        {"name": "kreekraksluizen", "entry": "FIS_8868181", "exit": "FIS_8867425"},
        {"name": "sluis_linne", "entry": "FIS_8864929", "exit": "FIS_8861324"},
    ]

    for comp in complexes:
        name = comp["name"]
        entry = comp["entry"]
        exit_node = comp["exit"]

        if entry not in G or exit_node not in G:
            print(f"Skipping {name}: Entry {entry} or Exit {exit_node} not in graph.")
            continue

        print(f"Extracting sub-graph for {name}...")

        # Find all nodes in the neighborhood of entry and exit node
        # Let's get nodes within a distance of 6 in the undirected graph
        undirected_G = G.to_undirected()

        # Get nodes within radius of entry
        nodes_entry = set(
            nx.single_source_shortest_path_length(undirected_G, entry, cutoff=6).keys()
        )
        # Get nodes within radius of exit
        nodes_exit = set(
            nx.single_source_shortest_path_length(
                undirected_G, exit_node, cutoff=6
            ).keys()
        )

        # Combine them
        subgraph_nodes = nodes_entry.union(nodes_exit)

        # Extract the induced subgraph
        sub_G = G.subgraph(subgraph_nodes).copy()

        # Save to fixture pickle
        fixture_path = output_dir / f"{name}_fixture.pickle"
        with open(fixture_path, "wb") as f:
            pickle.dump(sub_G, f)

        print(
            f"  Saved {sub_G.number_of_nodes()} nodes and {sub_G.number_of_edges()} edges to {fixture_path}"
        )

    print("Sub-graph fixtures extraction complete!")


if __name__ == "__main__":
    main()
