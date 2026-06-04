#!/usr/bin/env python
import pathlib
import pickle
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import box


def main():
    print("Generating lock diagnostics...")
    output_dir = pathlib.Path("output/lock-diagnostics")
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load the base graph
    graph_path = pathlib.Path("output/merged-graph/graph.pickle")
    if not graph_path.exists():
        print(f"Error: {graph_path} not found. Please run 'fis graph merge' first.")
        return

    with open(graph_path, "rb") as f:
        G = pickle.load(f)

    # Convert graph edges to a GeoDataFrame for easy plotting
    edges_data = []
    for u, v, data in G.edges(data=True):
        geom = data.get("geometry")
        if geom is not None:
            edges_data.append({"source": u, "target": v, "geometry": geom})
    edges_gdf = gpd.GeoDataFrame(edges_data, crs="EPSG:4326")

    # Convert graph nodes to a GeoDataFrame
    nodes_data = []
    for node, data in G.nodes(data=True):
        geom = data.get("geometry")
        if geom is not None:
            nodes_data.append({"id": node, "geometry": geom})
    nodes_gdf = gpd.GeoDataFrame(nodes_data, crs="EPSG:4326")

    # 2. Load lock/bridge/berth structures
    chambers_path = pathlib.Path("output/lock-schematization/chamber.geoparquet")
    locks_path = pathlib.Path("output/lock-schematization/lock.geoparquet")
    berths_path = pathlib.Path("output/lock-schematization/berths.geoparquet")
    bridges_path = pathlib.Path("output/bridge-schematization/bridge.geoparquet")
    openings_path = pathlib.Path("output/bridge-schematization/opening.geoparquet")

    chambers = gpd.read_parquet(chambers_path) if chambers_path.exists() else None
    gpd.read_parquet(locks_path) if locks_path.exists() else None
    berths = gpd.read_parquet(berths_path) if berths_path.exists() else None
    gpd.read_parquet(bridges_path) if bridges_path.exists() else None
    openings = gpd.read_parquet(openings_path) if openings_path.exists() else None

    # The 14 validated lock complexes
    complexes = [
        {"name": "Volkeraksluizen", "entry": "FIS_8860743", "exit": "FIS_8866727"},
        {"name": "Krammersluizen", "entry": "FIS_8864545", "exit": "FIS_8866367"},
        {"name": "Oranjesluizen", "entry": "FIS_8864384", "exit": "FIS_59275858"},
        {"name": "IJmuiden Sluizen", "entry": "FIS_8864991", "exit": "FIS_8861863"},
        {"name": "Terneuzen Sluizen", "entry": "FIS_8867489", "exit": "FIS_8863105"},
        {"name": "Lorentzsluizen", "entry": "FIS_8864239", "exit": "FIS_8860933"},
        {"name": "Sluis Weurt", "entry": "FIS_8864666", "exit": "FIS_8865102"},
        {"name": "Sluis Eefde", "entry": "FIS_8860918", "exit": "FIS_30986757"},
        {"name": "Sluis Born", "entry": "FIS_8868208", "exit": "FIS_8867148"},
        {"name": "Sluis Maasbracht", "entry": "FIS_8861292", "exit": "FIS_8862583"},
        {"name": "Sluis Heel", "entry": "FIS_8864929", "exit": "FIS_8865890"},
        {"name": "Sluis Grave", "entry": "FIS_8861448", "exit": "FIS_8865198"},
        {"name": "Kreekraksluizen", "entry": "FIS_8868181", "exit": "FIS_8867425"},
        {"name": "Sluis Linne", "entry": "FIS_8864929", "exit": "FIS_8861324"},
    ]

    for comp in complexes:
        name = comp["name"]
        entry_id = comp["entry"]
        exit_id = comp["exit"]

        if entry_id not in G or exit_id not in G:
            print(f"Skipping {name}: Entry {entry_id} or Exit {exit_id} not in graph.")
            continue

        print(f"Plotting {name}...")

        # Get coordinates
        p1 = G.nodes[entry_id]["geometry"]
        p2 = G.nodes[exit_id]["geometry"]

        # Calculate bounding box with padding
        minx = min(p1.x, p2.x) - 0.015
        maxx = max(p1.x, p2.x) + 0.015
        miny = min(p1.y, p2.y) - 0.015
        maxy = max(p1.y, p2.y) + 0.015
        bbox = box(minx, miny, maxx, maxy)

        # Subset graph data
        edges_sub = edges_gdf[edges_gdf.intersects(bbox)]
        nodes_sub = nodes_gdf[nodes_gdf.intersects(bbox)]

        fig, ax = plt.subplots(figsize=(10, 8))

        # Plot base graph
        if not edges_sub.empty:
            edges_sub.plot(
                ax=ax, color="blue", linewidth=1.5, alpha=0.6, label="Base Fairway"
            )
        if not nodes_sub.empty:
            nodes_sub.plot(
                ax=ax, color="darkblue", markersize=20, alpha=0.8, label="Junctions"
            )

        # Plot entry/exit nodes
        gpd.GeoDataFrame([{"geometry": p1}], crs="EPSG:4326").plot(
            ax=ax,
            color="green",
            markersize=100,
            marker="^",
            label=f"Entry ({entry_id})",
        )
        gpd.GeoDataFrame([{"geometry": p2}], crs="EPSG:4326").plot(
            ax=ax, color="red", markersize=100, marker="v", label=f"Exit ({exit_id})"
        )

        # Plot locks/chambers
        if chambers is not None and not chambers.empty:
            ch_sub = chambers[chambers.intersects(bbox)]
            if not ch_sub.empty:
                ch_sub.plot(
                    ax=ax,
                    color="orange",
                    alpha=0.4,
                    edgecolor="darkorange",
                    label="Chamber Area",
                )

        # Plot bridges/openings
        if openings is not None and not openings.empty:
            op_sub = openings[openings.intersects(bbox)]
            if not op_sub.empty:
                op_sub.plot(
                    ax=ax,
                    color="purple",
                    alpha=0.5,
                    edgecolor="indigo",
                    label="Bridge Opening",
                )

        # Plot berths
        if berths is not None and not berths.empty:
            b_sub = berths[berths.intersects(bbox)]
            if not b_sub.empty:
                b_sub.plot(
                    ax=ax,
                    color="cyan",
                    markersize=30,
                    edgecolor="darkcyan",
                    label="Berth Point",
                )

        ax.set_title(
            f"Lock Complex Diagnostic: {name}\nEntry: {entry_id} | Exit: {exit_id}"
        )
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.legend(loc="upper right")

        # Save figure
        fig.savefig(
            output_dir / f"{name.lower().replace(' ', '_')}.png",
            dpi=150,
            bbox_inches="tight",
        )
        plt.close(fig)

    print("Diagnostics generation complete!")


if __name__ == "__main__":
    main()
