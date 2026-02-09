
import pickle
import networkx as nx
import pathlib
import sys

def inspect_euris():
    path = pathlib.Path('output/euris-enriched/graph.pickle')
    if not path.exists():
        print(f"File not found: {path}")
        return

    print(f"Loading {path}...")
    with open(path, 'rb') as f:
        graph = pickle.load(f)
        
    u = 'NL_J4210'
    v = 'DE_J1144'
    
    print(f"\nSearching for edge {u} <-> {v}")
    
    if graph.has_edge(u, v):
        edge = graph.edges[u, v]
        print("Edge FOUND!")
        print("Attributes:")
        for k, v_attr in edge.items():
            print(f"  {k}: {v_attr}")
    elif graph.has_edge(v, u):
        edge = graph.edges[v, u]
        print("Edge FOUND (reversed)!")
        print("Attributes:")
        for k, v_attr in edge.items():
            print(f"  {k}: {v_attr}")
    else:
        print("Edge NOT found directly.")
        if graph.has_node(u) and graph.has_node(v):
            print("Both nodes exist.")
            try:
                path = nx.shortest_path(graph, u, v)
                print(f"Shortest path: {path}")
            except:
                print("No path found.")
        else:
            print(f"Node {u} exists: {graph.has_node(u)}")
            print(f"Node {v} exists: {graph.has_node(v)}")

if __name__ == "__main__":
    inspect_euris()
