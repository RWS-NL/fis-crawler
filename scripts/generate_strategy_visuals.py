import geopandas as gpd
import matplotlib.pyplot as plt


def plot_topology(lock_id, output_path, title):
    print(f"Generating visual for {lock_id}...")
    edges = gpd.read_parquet("output/dropins-schematization/edges.geoparquet")
    nodes = gpd.read_parquet("output/dropins-schematization/nodes.geoparquet")

    # Filter to relevant spatial area
    l_edges = edges[edges["id"].str.contains(lock_id)].copy()
    if l_edges.empty:
        print(f"No edges found for {lock_id}")
        return

    # Get spatial bounds and buffer slightly
    bounds = l_edges.total_bounds

    # Filter nodes within bounds
    l_nodes = nodes.cx[
        bounds[0] - 0.005 : bounds[2] + 0.005, bounds[1] - 0.005 : bounds[3] + 0.005
    ].copy()

    fig, ax = plt.subplots(figsize=(12, 8))

    # Plot edges
    l_edges.plot(ax=ax, color="blue", linewidth=2, alpha=0.6, label="Graph Edges")

    # Plot nodes
    l_nodes.plot(ax=ax, color="red", markersize=30, zorder=5, label="Nodes")

    # Annotate key nodes
    for _, row in l_nodes.iterrows():
        nid = str(row["id"])
        # Only annotate interesting nodes to avoid clutter
        if any(
            x in nid
            for x in [
                "chamber",
                "split",
                "merge",
                "8864190",
                "8861427",
                "59274799",
                "opening",
            ]
        ):
            ax.annotate(
                nid,
                (row.geometry.x, row.geometry.y),
                xytext=(5, 5),
                textcoords="offset points",
                fontsize=8,
                alpha=0.8,
                bbox=dict(facecolor="white", alpha=0.5, edgecolor="none"),
            )

    ax.set_title(title)
    ax.set_axis_off()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


# Generate Weurt
plot_topology(
    "49032",
    "docs/strategy/images/weurt_topology.png",
    "Sluis Weurt Topology (Lock 49032)",
)

# Generate Oranjesluizen
plot_topology(
    "50750",
    "docs/strategy/images/oranjesluizen_topology.png",
    "Oranjesluizen Topology (Lock 50750)",
)

print("Visuals generated in docs/strategy/images/")
