"""Route network graph construction for spatial berth capacity analysis.

Builds an undirected NetworkX graph from normalized routes, route junctions,
and berths with linear referencing along fairways.
"""

from __future__ import annotations

import logging
from typing import Any

import geopandas as gpd
import networkx as nx
import pandas as pd
from pandas.core.groupby.generic import DataFrameGroupBy
from shapely.geometry import LineString
from shapely.ops import substring

logger = logging.getLogger(__name__)


def extract_segment_geometry(
    full_geom: LineString | None,
    km_a: float,
    km_b: float,
    full_km_begin: float,
    full_km_end: float,
) -> LineString | None:
    """Extracts a sliced segment geometry along a route between km_a and km_b.

    Computes the normalized fractional positions (in [0.0, 1.0]) of km_a and
    km_b along the route's kilometrage span [full_km_begin, full_km_end] and
    slices the corresponding LineString substring.
    """
    km_span_total = abs(full_km_end - full_km_begin)
    if (
        full_geom is None
        or full_geom.is_empty
        or km_span_total <= 1e-4
        or full_geom.length <= 0
    ):
        return None

    # Normalized fractional position along the route geometry (0.0 at start, 1.0 at end),
    # accounting for routes whose kilometrage runs in ascending or descending order.
    if full_km_end >= full_km_begin:
        fraction_start = (km_a - full_km_begin) / km_span_total
        fraction_end = (km_b - full_km_begin) / km_span_total
    else:
        fraction_start = (full_km_begin - km_a) / km_span_total
        fraction_end = (full_km_begin - km_b) / km_span_total

    fraction_start = max(0.0, min(1.0, fraction_start))
    fraction_end = max(0.0, min(1.0, fraction_end))

    if abs(fraction_end - fraction_start) <= 1e-6:
        return None

    start_distance = min(fraction_start, fraction_end) * full_geom.length
    end_distance = max(fraction_start, fraction_end) * full_geom.length
    segment_geom = substring(full_geom, start_distance, end_distance)
    return segment_geom


def extract_route_points(
    route: pd.Series,
    rj_by_route: DataFrameGroupBy,
    berth_by_route: DataFrameGroupBy,
    full_km_begin: float,
    full_km_end: float,
) -> list[tuple[float, str, str, dict[str, Any]]]:
    """Collects and sorts reference points (endpoints, junctions, berths) along a route."""
    route_id = route["id"]
    empty_flags = {"is_overnight": False, "is_kegel": False}

    points: list[tuple[float, str, str, dict[str, Any]]] = [
        (full_km_begin, f"start_{route_id}", "endpoint", dict(empty_flags)),
        (full_km_end, f"end_{route_id}", "endpoint", dict(empty_flags)),
    ]

    if route_id in rj_by_route.groups:
        for _, rj in rj_by_route.get_group(route_id).iterrows():
            # RouteJunctions link multiple routes via a shared SectionJunctionId
            # (`section_junction_id`), which merges connecting routes into a single
            # topological graph node `rj_{section_junction_id}`.
            rj_node_id = f"rj_{rj['section_junction_id']}"
            km = (
                float(rj["route_km_begin"])
                if pd.notna(rj.get("route_km_begin"))
                else full_km_begin
            )
            points.append((km, rj_node_id, "junction", dict(empty_flags)))

    if route_id in berth_by_route.groups:
        for _, b in berth_by_route.get_group(route_id).iterrows():
            raw_km = b.get("route_km")
            km = float(raw_km) if pd.notna(raw_km) else full_km_begin
            points.append(
                (
                    km,
                    f"berth_{b['id']}",
                    "berth",
                    {
                        "berth_id": b["id"],
                        "name": b.get("name"),
                        "is_overnight": bool(b.get("is_overnight", False)),
                        "is_kegel": bool(b.get("is_kegel", False)),
                        "capacity_ships_85m": float(b.get("capacity_ships_85m", 0.0)),
                        "capacity_ships_110m": float(b.get("capacity_ships_110m", 0.0)),
                        "capacity_ships_135m": float(b.get("capacity_ships_135m", 0.0)),
                        "effective_length_m": float(b.get("effective_length_m", 0.0)),
                        "has_cardropoff": bool(b.get("has_cardropoff", False)),
                    },
                )
            )

    points.sort(key=lambda x: x[0])
    return points


def build_route_network_graph(
    routes: gpd.GeoDataFrame,
    rjs: gpd.GeoDataFrame,
    berths: gpd.GeoDataFrame,
    type_map: dict[str, str],
    expected_parallel_edges: int | None = None,
) -> tuple[nx.Graph, list[dict[str, Any]]]:
    """Constructs an undirected network graph from routes, junctions, and berths."""
    logger.info("Constructing NetworkX route graph...")
    G = nx.Graph()
    segment_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    parallel_edge_count = 0

    rj_by_route = rjs.groupby("route_id")
    berth_by_route = berths.groupby("route_id")

    for _, route in routes.iterrows():
        route_id = route["id"]
        route_name = str(route.get("name") or f"Route {route_id}")
        route_code = str(route.get("code") or "")
        network_type = type_map.get(route_id, "Niet geclassificeerd / Zee")
        full_geom = route.geometry

        full_km_begin = (
            float(route["route_km_begin"])
            if pd.notna(route.get("route_km_begin"))
            else 0.0
        )
        full_km_end = (
            float(route["route_km_end"])
            if pd.notna(route.get("route_km_end"))
            else full_km_begin
        )

        points = extract_route_points(
            route, rj_by_route, berth_by_route, full_km_begin, full_km_end
        )

        for (km_a, id_a, type_a, attrs_a), (km_b, id_b, type_b, attrs_b) in zip(
            points, points[1:]
        ):
            if id_a not in G:
                G.add_node(id_a, type=type_a, km=km_a, route_id=route_id, **attrs_a)
            if id_b not in G:
                G.add_node(id_b, type=type_b, km=km_b, route_id=route_id, **attrs_b)

            dist_km = abs(km_b - km_a)
            seg_geom = extract_segment_geometry(
                full_geom, km_a, km_b, full_km_begin, full_km_end
            )

            edge_data = {
                "weight": dist_km,
                "route_id": route_id,
                "route_name": route_name,
                "route_code": route_code,
                "network_type": network_type,
                "km_from": km_a,
                "km_to": km_b,
                "length_km": dist_km,
                "geometry": seg_geom,
            }

            # Canonical undirected node-pair key so segments connecting the same
            # two junctions in either direction map to the same key. If two routes
            # share the exact same consecutive junction pair (a parallel edge),
            # we deduplicate the segment and retain the shorter connection.
            pair_key = (id_a, id_b) if id_a <= id_b else (id_b, id_a)
            if pair_key in segment_by_pair:
                parallel_edge_count += 1
                if dist_km < segment_by_pair[pair_key]["length_km"]:
                    G.edges[id_a, id_b].update(edge_data)
                    segment_by_pair[pair_key] = {
                        "source": id_a,
                        "target": id_b,
                        **edge_data,
                    }
            else:
                G.add_edge(id_a, id_b, **edge_data)
                segment_by_pair[pair_key] = {
                    "source": id_a,
                    "target": id_b,
                    **edge_data,
                }

    if expected_parallel_edges is not None:
        assert parallel_edge_count == expected_parallel_edges, (
            f"Expected {expected_parallel_edges} parallel edge(s), found {parallel_edge_count}"
        )

    route_segments = list(segment_by_pair.values())
    logger.info(
        "Built route graph: %d nodes, %d edges, %d route segments (removed %d parallel edge(s))",
        G.number_of_nodes(),
        G.number_of_edges(),
        len(route_segments),
        parallel_edge_count,
    )
    return G, route_segments
