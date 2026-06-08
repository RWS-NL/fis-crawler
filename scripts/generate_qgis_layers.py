#!/usr/bin/env python
import pathlib
import pickle
import geopandas as gpd
import pandas as pd
from shapely.geometry import box


def main():
    print("Generating QGIS layers for lock complexes...")
    output_dir = pathlib.Path("output/lock-diagnostics")
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load the base graph
    graph_path = pathlib.Path("output/merged-graph/graph.pickle")
    if not graph_path.exists():
        print(f"Error: {graph_path} not found. Please run 'fis graph merge' first.")
        return

    with open(graph_path, "rb") as f:
        G = pickle.load(f)

    # Convert graph edges to a GeoDataFrame
    edges_data = []
    for u, v, data in G.edges(data=True):
        geom = data.get("geometry")
        if geom is not None:
            edges_data.append({"source_node": u, "target_node": v, "geometry": geom})
    edges_gdf = gpd.GeoDataFrame(edges_data, crs="EPSG:4326")

    # Convert graph nodes to a GeoDataFrame
    nodes_data = []
    for node, data in G.nodes(data=True):
        geom = data.get("geometry")
        if geom is not None:
            nodes_data.append({"node_id": node, "geometry": geom})
    nodes_gdf = gpd.GeoDataFrame(nodes_data, crs="EPSG:4326")

    # 2. Load lock/bridge/berth structures
    chambers_path = pathlib.Path("output/lock-schematization/chamber.geoparquet")
    berths_path = pathlib.Path("output/lock-schematization/berths.geoparquet")
    openings_path = pathlib.Path("output/bridge-schematization/opening.geoparquet")

    chambers = (
        gpd.read_parquet(chambers_path)
        if chambers_path.exists()
        else gpd.GeoDataFrame(crs="EPSG:4326")
    )
    berths = (
        gpd.read_parquet(berths_path)
        if berths_path.exists()
        else gpd.GeoDataFrame(crs="EPSG:4326")
    )
    openings = (
        gpd.read_parquet(openings_path)
        if openings_path.exists()
        else gpd.GeoDataFrame(crs="EPSG:4326")
    )

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

    boundaries_list = []
    edges_list = []
    nodes_list = []
    chambers_list = []
    openings_list = []
    berths_list = []

    for comp in complexes:
        name = comp["name"]
        entry_id = comp["entry"]
        exit_id = comp["exit"]

        if entry_id not in G or exit_id not in G:
            continue

        p1 = G.nodes[entry_id]["geometry"]
        p2 = G.nodes[exit_id]["geometry"]

        # Calculate bounding box with padding
        minx = min(p1.x, p2.x) - 0.015
        maxx = max(p1.x, p2.x) + 0.015
        miny = min(p1.y, p2.y) - 0.015
        maxy = max(p1.y, p2.y) + 0.015
        bbox_geom = box(minx, miny, maxx, maxy)

        # 1. Add boundary polygon for atlas coverage
        boundaries_list.append(
            {
                "complex_name": name,
                "entry_node": entry_id,
                "exit_node": exit_id,
                "geometry": bbox_geom,
            }
        )

        # 2. Add edges inside this complex boundary
        edges_sub = edges_gdf[edges_gdf.intersects(bbox_geom)].copy()
        edges_sub["complex_name"] = name
        edges_list.append(edges_sub)

        # 3. Add nodes inside this complex boundary
        nodes_sub = nodes_gdf[nodes_gdf.intersects(bbox_geom)].copy()
        nodes_sub["complex_name"] = name
        nodes_list.append(nodes_sub)

        # 4. Add chambers
        if not chambers.empty:
            ch_sub = chambers[chambers.intersects(bbox_geom)].copy()
            if not ch_sub.empty:
                ch_sub["complex_name"] = name
                ch_sub_clean = ch_sub[["id", "name", "complex_name", "geometry"]].copy()
                chambers_list.append(ch_sub_clean)

        # 5. Add openings
        if not openings.empty:
            op_sub = openings[openings.intersects(bbox_geom)].copy()
            if not op_sub.empty:
                op_sub["complex_name"] = name
                op_sub_clean = op_sub[["id", "complex_name", "geometry"]].copy()
                openings_list.append(op_sub_clean)

        # 6. Add berths
        if not berths.empty:
            b_sub = berths[berths.intersects(bbox_geom)].copy()
            if not b_sub.empty:
                b_sub["complex_name"] = name
                b_sub_clean = b_sub[["id", "name", "complex_name", "geometry"]].copy()
                berths_list.append(b_sub_clean)

    # Combine all subsets
    boundaries_gdf = gpd.GeoDataFrame(boundaries_list, crs="EPSG:4326")

    # Save everything to a GeoPackage
    gpkg_path = output_dir / "lock_diagnostics.gpkg"
    if gpkg_path.exists():
        gpkg_path.unlink()

    print("Writing boundaries layer...")
    boundaries_gdf.to_file(gpkg_path, layer="boundaries", driver="GPKG")

    if edges_list:
        print("Writing network_edges layer...")
        pd.concat(edges_list).to_file(gpkg_path, layer="network_edges", driver="GPKG")

    if nodes_list:
        print("Writing network_nodes layer...")
        pd.concat(nodes_list).to_file(gpkg_path, layer="network_nodes", driver="GPKG")

    if chambers_list:
        print("Writing lock_chambers layer...")
        pd.concat(chambers_list).to_file(
            gpkg_path, layer="lock_chambers", driver="GPKG"
        )

    if openings_list:
        print("Writing bridge_openings layer...")
        pd.concat(openings_list).to_file(
            gpkg_path, layer="bridge_openings", driver="GPKG"
        )

    if berths_list:
        print("Writing berths layer...")
        pd.concat(berths_list).to_file(gpkg_path, layer="berths", driver="GPKG")

    print(f"GeoPackage successfully created at: {gpkg_path}")


if __name__ == "__main__":
    main()
