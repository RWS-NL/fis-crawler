import pickle
import networkx as nx
import pathlib
import json

from fis.graph.validation import GraphValidator

with open("output/merged-graph/graph.pickle", "rb") as f:
    graph = pickle.load(f)

print(f"Nodes: {graph.number_of_nodes()}, Edges: {graph.number_of_edges()}")

validator = GraphValidator(graph, pathlib.Path("config/schema.toml"))
stats = validator.check_statistics()
print(stats)
