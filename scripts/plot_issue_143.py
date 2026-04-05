import matplotlib.pyplot as plt
import contextily as ctx
import geopandas as gpd
import os

print("Creating debug plots in output/debug...")
os.makedirs("output/debug", exist_ok=True)

# Load data
try:
    edges = gpd.read_parquet("output/dropins-fis-detailed/edges.geoparquet")
    nodes = gpd.read_parquet("output/dropins-fis-detailed/nodes.geoparquet")
except Exception as e:
    print(f"Error loading parquet files: {e}")
    # try the other path
    try:
        edges = gpd.read_parquet("output/dropins-schematization/edges.geoparquet")
        nodes = gpd.read_parquet("output/dropins-schematization/nodes.geoparquet")
    except Exception as e:
        print(f"Error loading alternative parquet files: {e}")
        exit(1)


def plot_area(title, filename, required_nodes, bbox=None):
    print(f"Plotting {title}...")
    fig, ax = plt.subplots(figsize=(12, 12))

    sub_nodes = nodes[nodes["id"].isin(required_nodes)].copy()

    if sub_nodes.empty and bbox:
        # Fallback: plot by bbox (minx, miny, maxx, maxy)
        minx, miny, maxx, maxy = bbox
        sub_nodes = nodes.cx[minx:maxx, miny:maxy].copy()

    if not sub_nodes.empty:
        # Find edges between these nodes or connected to these nodes
        sub_edges_idx = edges["source_node"].isin(sub_nodes["id"]) | edges[
            "target_node"
        ].isin(sub_nodes["id"])
        sub_edges = edges[sub_edges_idx].copy()

        # Convert to Web Mercator for contextily
        try:
            sub_nodes_wm = sub_nodes.to_crs(epsg=3857)
            sub_edges_wm = sub_edges.to_crs(epsg=3857)

            # Plot edges
            if not sub_edges_wm.empty:
                sub_edges_wm.plot(ax=ax, color="blue", linewidth=2, alpha=0.6, zorder=2)

            # Plot nodes
            sub_nodes_wm.plot(ax=ax, color="red", markersize=50, zorder=5)

            # Annotate nodes
            for idx, row in sub_nodes_wm.iterrows():
                # Make label simpler to read
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
                    fontsize=8,
                    color="black",
                    bbox=dict(facecolor="white", edgecolor="none", alpha=0.8),
                    zorder=10,
                )

            # Add basemap
            try:
                ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
            except Exception as e:
                print(f"Warning: Could not add basemap: {e}")

            ax.set_title(title)
            ax.set_axis_off()
            plt.tight_layout()
            plt.savefig(f"output/debug/{filename}.png", dpi=300, bbox_inches="tight")
            print(f"Saved output/debug/{filename}.png")
        except Exception as e:
            print(f"Error plotting {filename}: {e}")
    else:
        print(f"Warning: No nodes found for {title}")
    plt.close()


# Weurt Lock (Issue #143)
weurt_nodes = [
    "8864666",
    "lock_49032_split",
    "chamber_47538_start",
    "8864190",
    "opening_5835_start",
    "opening_5835_end",
    "chamber_47538_end",
    "lock_49032_merge",
    "8865102",
]
# add surrounding nodes to provide context
weurt_bbox = (5.80, 51.84, 5.83, 51.86)
plot_area(
    "Sluis Weurt - Incorrect Topology", "weurt_topology", weurt_nodes, bbox=weurt_bbox
)

# Oranjesluizen (Issue #143)
oranje_nodes = [
    "30985116",
    "59275858",
    "chamber_11446_start",
    "chamber_11446_end",
    "8861427",
    "lock_50750_split",
    "lock_50750_merge",
    "59274799",
    "8864384",
]
oranje_bbox = (4.95, 52.37, 4.98, 52.39)
plot_area(
    "Oranjesluizen - Incorrect Topology",
    "oranjesluizen_topology",
    oranje_nodes,
    bbox=oranje_bbox,
)

print("Done.")
