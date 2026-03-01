import pickle
import sys
from shapely import wkt
from shapely.geometry import Point


def get_geom(node_data):
    """Extract shapely geometry from node data."""
    geom = node_data.get("geometry") or node_data.get("Geometry")
    if isinstance(geom, str):
        try:
            return wkt.loads(geom)
        except Exception:
            return None
    if isinstance(geom, (Point,)):
        return geom
    if "x" in node_data and "y" in node_data:
        try:
            return Point(float(node_data["x"]), float(node_data["y"]))
        except Exception:
            return None
    return None


def inspect():
    try:
        with open("output/merged-graph/graph.pickle", "rb") as f:
            merged = pickle.load(f)
    except Exception as e:
        print(f"Error loading graph: {e}")
        sys.exit(1)

    print("--- Lobith Node Inspection: FIS_22638200 ---")
    if not merged.has_node("FIS_22638200"):
        print("Node FIS_22638200 not found!")
        return

    root_geom = get_geom(merged.nodes["FIS_22638200"])
    print(f"Root Node Coords: {root_geom}")

    print("\n--- Neighbors ---")
    for n in merged.neighbors("FIS_22638200"):
        edge = merged.edges["FIS_22638200", n]
        n_data = merged.nodes[n]
        n_geom = get_geom(n_data)

        dist_m = -1
        if root_geom and n_geom:
            try:
                # Approx distance in meters (lat ~52)
                dist_deg = root_geom.distance(n_geom)
                dist_m = dist_deg * 111000 * 0.6
            except Exception:
                pass

        print(f"Neighbor: {n}")
        print(f"  Source: {edge.get('data_source')}")
        print(f"  Coords: {n_geom}")
        print(f"  Distance: {dist_m:.2f} m")

        if "FIS_22638449" in n:
            print("  [TARGET] This is the FIS edge user wants to remove/check.")

    print("\n--- Neighbors of FIS_22637860 ---")
    if merged.has_node("FIS_22637860"):
        neighbors = list(merged.neighbors("FIS_22637860"))
        print(f"Node FIS_22637860 has {len(neighbors)} neighbors: {neighbors}")
        for n in neighbors:
            edge = merged.edges["FIS_22637860", n]
            print(f"  -> {n} (Edge ID: {edge.get('Id')})")
    else:
        print("Node FIS_22637860 NOT found.")

    # Check edge attributes
    print("\n--- Edge Attributes for FIS_22638200 ---")
    for n in merged.neighbors("FIS_22638200"):
        edge = merged.edges["FIS_22638200", n]
        print(f"Edge to {n}: {edge}")
        if "22638449" in str(edge):
            print("  MATCH FOUND IN ATTRIBUTES (22638449)")


if __name__ == "__main__":
    inspect()
