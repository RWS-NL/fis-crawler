#!/usr/bin/env python3
"""
Route Waypoint Verification Script.
Validates the shortest paths between key O-D hubs against expected checkpoints.
"""

import sys
import pathlib
import pickle
import networkx as nx
import numpy as np
import pandas as pd

from fis.ivs.assign import get_edge_weight_soft, build_edge_structures_lookup


def get_representative_node(G, locode_prefix):
    candidates = []
    for n, d in G.nodes(data=True):
        loc = d.get("locode")
        if loc and str(loc).upper().startswith(locode_prefix):
            geom = d.get("geometry")
            if geom and hasattr(geom, "x"):
                candidates.append((n, geom.x, geom.y, G.degree(n)))

    if not candidates:
        return None

    # Compute centroid
    avg_x = np.mean([c[1] for c in candidates])
    avg_y = np.mean([c[2] for c in candidates])

    # Filter candidates with degree >= 2 if any exist to avoid dead-ends
    deg2_candidates = [c for c in candidates if c[3] >= 2]
    subset = deg2_candidates if deg2_candidates else candidates

    # Pick the one closest to centroid
    best_node = None
    min_dist = float("inf")
    for n, x, y, deg in subset:
        dist = (x - avg_x) ** 2 + (y - avg_y) ** 2
        if dist < min_dist:
            min_dist = dist
            best_node = n
    return best_node


def check_lobith(path, G):
    # Rotterdam - Duisburg must pass through Lobith (NLLOB)
    for n in path:
        loc = G.nodes[n].get("locode")
        if loc and str(loc).upper().startswith("NLLOB"):
            return True, f"Passed Lobith node: {n} ({loc})"
    return False, "Did not pass any NLLOB (Lobith) node"


def check_ark(path, G):
    # Amsterdam - Duisburg/Rotterdam must use Amsterdam-Rijnkanaal (fairway_id 15384 or similar)
    ark_ids = {15384, 38782, 22637927}
    for u, v in zip(path[:-1], path[1:]):
        d = G[u][v]
        fid = d.get("fairway_id")
        if fid is not None:
            try:
                if int(float(fid)) in ark_ids:
                    return (
                        True,
                        f"Used Amsterdam-Rijnkanaal edge: {u} -> {v} (fairway_id {fid})",
                    )
            except ValueError:
                pass
    return False, "Did not use Amsterdam-Rijnkanaal"


def check_kreekrak_volkerak(path, G):
    # Rotterdam - Antwerpen must pass Volkerak (12821) or Kreekrak (40158)
    target_ids = {"12821", "40158"}
    for u, v in zip(path[:-1], path[1:]):
        d = G[u][v]
        edge_id = str(d.get("fis_id") or d.get("code") or "")
        if edge_id in target_ids:
            name = "Volkerak" if edge_id == "12821" else "Kreekrak"
            return True, f"Passed {name} section: {u} -> {v} (fis_id {edge_id})"
    return False, "Did not pass Volkerak or Kreekrak sections"


def check_eemshaven_rotterdam(path, G):
    # Eemshaven - Rotterdam: Lorentzsluizen (59274911) or Stevinsluis (19573) or Noordzee
    locks = {"59274911", "19573"}
    for u, v in zip(path[:-1], path[1:]):
        d = G[u][v]
        edge_id = str(d.get("fis_id") or d.get("code") or "")
        if edge_id in locks:
            name = "Lorentzsluizen" if edge_id == "59274911" else "Stevinsluis"
            return True, f"Passed {name} (Afsluitdijk lock): {u} -> {v}"
        wname = str(d.get("water_name", "")).strip().lower()
        if "noordzee" in wname or "sea" in wname:
            return True, f"Used Noordzee sea route: {u} -> {v}"
    return False, "Did not pass Afsluitdijk locks or Noordzee"


def main():
    graph_path = pathlib.Path("output/merged-graph/graph.pickle")
    if not graph_path.exists():
        print(
            f"Error: Merged graph not found at {graph_path}. Please run `make merge-graphs` first.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Loading merged graph from {graph_path}...")
    with open(graph_path, "rb") as f:
        G = pickle.load(f)

    print(
        f"Graph loaded with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges."
    )

    # Load DTV database and lookup
    lookup = build_edge_structures_lookup()

    # Use a zero-dimension vessel to validate base network connectivity/topology
    ship_dims = {"beam": 0.0, "length": 0.0, "height": 0.0, "draft": 0.0}
    print(f"Routing using topological vessel dimensions: {ship_dims}")

    def weight_func(u, v, d):
        edge_id = d.get("fis_id") or d.get("code")
        struct_data = lookup.get(str(edge_id), {"chambers": [], "openings": []})
        return get_edge_weight_soft(d, struct_data, ship_dims)

    # Define validation pairs
    pairs = [
        {
            "name": "Rotterdam to Duisburg",
            "start": "NLRTM",
            "end": "DEDUI",
            "check_func": check_lobith,
            "desc": "Must pass through Lobith (NLLOB)",
        },
        {
            "name": "Amsterdam to Duisburg",
            "start": "NLAMS",
            "end": "DEDUI",
            "check_func": check_ark,
            "desc": "Must use Amsterdam-Rijnkanaal (ARK)",
        },
        {
            "name": "Rotterdam to Antwerpen",
            "start": "NLRTM",
            "end": "BEANR",
            "check_func": check_kreekrak_volkerak,
            "desc": "Must use Kreekrak (40158) or Volkerak (12821)",
        },
        {
            "name": "Rotterdam to Amsterdam",
            "start": "NLRTM",
            "end": "NLAMS",
            "check_func": check_ark,
            "desc": "Must use Amsterdam-Rijnkanaal",
        },
        {
            "name": "Eemshaven to Rotterdam",
            "start": "NLEEM",
            "end": "NLRTM",
            "check_func": check_eemshaven_rotterdam,
            "desc": "Must use Afsluitdijk locks or Noordzee",
        },
    ]

    results = []

    for pair in pairs:
        start_code = pair["start"]
        end_code = pair["end"]

        start_node = get_representative_node(G, start_code)
        end_node = get_representative_node(G, end_code)

        if not start_node or not end_node:
            results.append(
                {
                    "Route": pair["name"],
                    "Start Node": start_node or "NOT FOUND",
                    "End Node": end_node or "NOT FOUND",
                    "Status": "RESOLVE_FAIL",
                    "Details": f"Could not geocode O-D nodes for {start_code} -> {end_code}",
                }
            )
            continue

        try:
            path = nx.shortest_path(G, start_node, end_node, weight=weight_func)

            # Check waypoints
            ok, details = pair["check_func"](path, G)
            status = "PASS" if ok else "FAIL"

            results.append(
                {
                    "Route": pair["name"],
                    "Start Node": f"{start_node} ({start_code})",
                    "End Node": f"{end_node} ({end_code})",
                    "Status": status,
                    "Details": details,
                }
            )

        except nx.NetworkXNoPath:
            results.append(
                {
                    "Route": pair["name"],
                    "Start Node": f"{start_node} ({start_code})",
                    "End Node": f"{end_node} ({end_code})",
                    "Status": "NO_PATH",
                    "Details": "No path exists between start and end node.",
                }
            )

    df_res = pd.DataFrame(results)

    # Generate Markdown Report
    report_path = pathlib.Path("output/route_validation_report.md")
    print("\nRoute Waypoint Validation Results:")
    print(df_res.to_string(index=False))

    md_content = f"""# Route Waypoint Validation Report

This report summarizes the verification of solved shortest paths between major hubs on the integrated FIS-EURIS network graph.

## Validation Table

{df_res.to_markdown(index=False)}

## Details & Recommendations
- **Rotterdam - Duisburg**: Passes through Lobith to check base transboundary connectivity.
- **Amsterdam - Duisburg**: Tests connectivity through the Amsterdam-Rijnkanaal (ARK).
- **Rotterdam - Antwerpen**: Checks the Schelde-Rijn connection via Volkerak or Kreekrak locks.
- **Eemshaven - Rotterdam**: Tests Afsluitdijk lock routing vs. coastal Noordzee routing.
"""
    report_path.write_text(md_content, encoding="utf-8")
    print(f"\nSaved markdown report to {report_path}")

    # Return exit code based on failures
    if "FAIL" in df_res["Status"].values or "NO_PATH" in df_res["Status"].values:
        print(
            "\nVerification FAILED: One or more routes failed waypoint validation.",
            file=sys.stderr,
        )
        sys.exit(1)
    else:
        print(
            "\nVerification PASSED: All routes successfully validated along waypoints."
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
