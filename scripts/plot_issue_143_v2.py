import matplotlib.pyplot as plt
import contextily as ctx
import geopandas as gpd
import os

print("Creating debug plots in output/debug...")
os.makedirs("output/debug", exist_ok=True)

try:
    edges = gpd.read_parquet("output/dropins-fis-detailed/edges.geoparquet")
    nodes = gpd.read_parquet("output/dropins-fis-detailed/nodes.geoparquet")
except Exception:
    edges = gpd.read_parquet("output/dropins-schematization/edges.geoparquet")
    nodes = gpd.read_parquet("output/dropins-schematization/nodes.geoparquet")


def plot_area(title, filename, highlight_nodes, bbox):
    print(f"Plotting {title}...")
    fig, ax = plt.subplots(figsize=(14, 14))

    minx, miny, maxx, maxy = bbox

    # Get all nodes and edges in the bbox
    sub_nodes = nodes.cx[minx:maxx, miny:maxy].copy()

    if not sub_nodes.empty:
        # Find edges between these nodes
        sub_edges_idx = edges["source_node"].isin(sub_nodes["id"]) & edges[
            "target_node"
        ].isin(sub_nodes["id"])
        sub_edges = edges[sub_edges_idx].copy()

        # Convert to Web Mercator for contextily
        sub_nodes_wm = sub_nodes.to_crs(epsg=3857)
        sub_edges_wm = sub_edges.to_crs(epsg=3857)

        # Split into normal and highlighted
        highlighted_nodes_mask = sub_nodes_wm["id"].isin(highlight_nodes)
        normal_nodes = sub_nodes_wm[~highlighted_nodes_mask]
        hl_nodes = sub_nodes_wm[highlighted_nodes_mask]

        highlighted_edges_mask = sub_edges_wm["source_node"].isin(
            highlight_nodes
        ) | sub_edges_wm["target_node"].isin(highlight_nodes)
        normal_edges = sub_edges_wm[~highlighted_edges_mask]
        hl_edges = sub_edges_wm[highlighted_edges_mask]

        # Plot normal edges
        if not normal_edges.empty:
            normal_edges.plot(ax=ax, color="gray", linewidth=2, alpha=0.5, zorder=1)

        # Plot highlighted edges
        if not hl_edges.empty:
            hl_edges.plot(ax=ax, color="red", linewidth=3, alpha=0.8, zorder=2)

        # Plot normal nodes
        if not normal_nodes.empty:
            normal_nodes.plot(ax=ax, color="blue", markersize=30, zorder=3)
            # Annotate normal nodes with small font
            for idx, row in normal_nodes.iterrows():
                label = (
                    str(row["id"])
                    .replace("chamber_", "ch_")
                    .replace("opening_", "op_")
                    .replace("lock_", "lk_")
                )
                ax.annotate(
                    label,
                    xy=(row.geometry.x, row.geometry.y),
                    xytext=(3, 3),
                    textcoords="offset points",
                    fontsize=6,
                    color="darkblue",
                    alpha=0.7,
                    zorder=4,
                )

        # Plot highlighted nodes
        if not hl_nodes.empty:
            hl_nodes.plot(ax=ax, color="red", markersize=80, zorder=5)
            # Annotate highlighted nodes
            for idx, row in hl_nodes.iterrows():
                label = (
                    str(row["id"])
                    .replace("chamber_", "ch_")
                    .replace("opening_", "op_")
                    .replace("lock_", "lk_")
                )
                ax.annotate(
                    label,
                    xy=(row.geometry.x, row.geometry.y),
                    xytext=(5, 5),
                    textcoords="offset points",
                    fontsize=9,
                    color="black",
                    fontweight="bold",
                    bbox=dict(facecolor="white", edgecolor="red", alpha=0.9),
                    zorder=10,
                )

        # Add basemap
        try:
            ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
        except Exception as e:
            print(f"Warning: Could not add basemap: {e}")

        ax.set_title(title, fontsize=16)
        ax.set_axis_off()
        plt.tight_layout()
        plt.savefig(f"output/debug/{filename}.png", dpi=300, bbox_inches="tight")
        print(f"Saved output/debug/{filename}.png")
    else:
        print(f"Warning: No nodes found for {title} in bbox {bbox}")
    plt.close()


# Weurt Lock
weurt_hl = [
    "lock_49032_split",
    "chamber_47538_start",
    "8864190",
    "opening_5835_start",
    "opening_5835_end",
    "chamber_47538_end",
    "lock_49032_merge",
]
weurt_bbox = (5.78, 51.83, 5.82, 51.86)
plot_area(
    "Sluis Weurt - All branches & incorrectly placed nodes highlighted",
    "weurt_topology_all",
    weurt_hl,
    bbox=weurt_bbox,
)

# Oranjesluizen
oranje_hl = ["59275858", "lock_50750_split", "lock_50750_merge"]
oranje_bbox = (4.95, 52.37, 4.98, 52.39)
plot_area(
    "Oranjesluizen - All branches & incorrectly placed nodes highlighted",
    "oranjesluizen_topology_all",
    oranje_hl,
    bbox=oranje_bbox,
)

print("Done.")
