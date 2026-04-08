import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx


def plot_topology(lock_id, output_path, title, buffer=0.002):
    print(f"Generating visual for {lock_id}...")
    edges = gpd.read_parquet("output/dropins-schematization/edges.geoparquet")
    nodes = gpd.read_parquet("output/dropins-schematization/nodes.geoparquet")

    # 1. Find the lock/chamber footprint to determine spatial area
    mask = edges["id"].str.contains(lock_id)
    if mask.sum() == 0:
        print(f"No edges found for {lock_id}")
        return

    # Get bounds from the lock-specific edges
    bounds = edges[mask].total_bounds
    xmin, ymin, xmax, ymax = bounds

    # 2. Filter ALL edges and nodes in this spatial area for context
    # Use a slightly larger buffer to see connections
    l_edges = edges.cx[
        xmin - buffer : xmax + buffer, ymin - buffer : ymax + buffer
    ].copy()
    l_nodes = nodes.cx[
        xmin - buffer : xmax + buffer, ymin - buffer : ymax + buffer
    ].copy()

    fig, ax = plt.subplots(figsize=(14, 10))

    # Set CRS to 3857 for contextily if needed, but we can use providers.Esri.WorldImagery
    # Better to plot in 4326 and add basemap in 4326 if supported,
    # or reproject everything to 3857 for standard web map tiles.
    l_edges_3857 = l_edges.to_crs(epsg=3857)
    l_nodes_3857 = l_nodes.to_crs(epsg=3857)

    # Plot edges with different styles
    # Highlight lock-internal routes vs normal fairways
    is_lock = l_edges_3857["id"].str.contains(lock_id)
    l_edges_3857[~is_lock].plot(
        ax=ax, color="blue", linewidth=1.5, alpha=0.8, label="Fairway Segments"
    )
    l_edges_3857[is_lock].plot(
        ax=ax, color="cyan", linewidth=3, alpha=0.9, label="Lock/Chamber Routes"
    )

    # Plot nodes
    l_nodes_3857.plot(ax=ax, color="red", markersize=40, zorder=5, label="Nodes")

    # Annotate key nodes
    for _, row in l_nodes_3857.iterrows():
        nid = str(row["id"])
        # Annotate chambers, splits, merges, and specific junction IDs
        if any(
            x in nid
            for x in [
                "chamber",
                "split",
                "merge",
                "8864190",
                "8861427",
                "59274799",
                "8864666",
                "8865102",
                "opening",
            ]
        ):
            ax.annotate(
                nid,
                (row.geometry.x, row.geometry.y),
                xytext=(5, 5),
                textcoords="offset points",
                fontsize=9,
                weight="bold",
                bbox=dict(
                    facecolor="white",
                    alpha=0.7,
                    edgecolor="none",
                    boxstyle="round,pad=0.2",
                ),
            )

    # Add basemap
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron)

    ax.set_title(title, fontsize=16, pad=20)
    ax.set_axis_off()
    plt.legend(loc="lower right")
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()


# Generate Weurt with generous buffer
plot_topology(
    "49032",
    "docs/strategy/images/weurt_topology.png",
    "Sluis Weurt Topology (Lock 49032)",
    buffer=0.003,
)

# Generate Oranjesluizen - ensure we see the full branching
plot_topology(
    "50750",
    "docs/strategy/images/oranjesluizen_topology.png",
    "Oranjesluizen Topology (Lock 50750)",
    buffer=0.005,
)

print("Visuals regenerated with full context and basemaps in docs/strategy/images/")
