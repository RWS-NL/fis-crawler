"""Route network graph construction for spatial berth capacity analysis.

Builds a NetworkX graph from routes, route junctions, and berths with linear
referencing along fairways.
"""

from __future__ import annotations

import logging
from typing import Any

import geopandas as gpd
import networkx as nx
import pandas as pd
from shapely.geometry import LineString, MultiLineString
from shapely.ops import substring

logger = logging.getLogger(__name__)


def extract_segment_geometry(
    full_geom: LineString | MultiLineString | None,
    km_a: float,
    km_b: float,
    full_km_begin: float,
    full_km_end: float,
) -> LineString | None:
    """Extracts a sliced segment geometry along a route between km_a and km_b.

    Supports both increasing (full_km_end >= full_km_begin) and decreasing
    (full_km_end < full_km_begin) kilometrage along fairway routes.
    """
    km_span_total = abs(full_km_end - full_km_begin)
    if (
        full_geom is None
        or full_geom.is_empty
        or km_span_total <= 1e-4
        or full_geom.length <= 0
    ):
        return None

    if full_km_end >= full_km_begin:
        f_a = (km_a - full_km_begin) / km_span_total
        f_b = (km_b - full_km_begin) / km_span_total
    else:
        f_a = (full_km_begin - km_a) / km_span_total
        f_b = (full_km_begin - km_b) / km_span_total

    f_a = max(0.0, min(1.0, f_a))
    f_b = max(0.0, min(1.0, f_b))

    if abs(f_b - f_a) <= 1e-6:
        return None

    try:
        return substring(
            full_geom,
            min(f_a, f_b) * full_geom.length,
            max(f_a, f_b) * full_geom.length,
        )
    except Exception:
        return None


def add_or_update_node(
    G: nx.Graph,
    node_id: str,
    node_type: str,
    km: float,
    route_id: int,
    attrs: dict[str, Any],
) -> None:
    """Adds a node to the graph or updates existing node attributes."""
    if node_id not in G:
        G.add_node(node_id, type=node_type, km=km, route_id=route_id, **attrs)
    else:
        for k, v in attrs.items():
            if v:
                G.nodes[node_id][k] = v


def extract_route_points(
    route: pd.Series,
    rj_by_route: Any,
    berth_by_route: Any,
) -> list[tuple[float, str, str, dict[str, Any]]]:
    """Collects and sorts all reference points (endpoints, junctions, berths) along a route.

    Returns a list of tuples: (km, node_id, node_type, attributes).
    """
    route_id = int(route["Id"])
    full_km_begin = (
        float(route["RouteKmBegin"]) if pd.notna(route.get("RouteKmBegin")) else 0.0
    )
    full_km_end = (
        float(route["RouteKmEnd"])
        if pd.notna(route.get("RouteKmEnd"))
        else full_km_begin
    )

    points: list[tuple[float, str, str, dict[str, Any]]] = [
        (
            full_km_begin,
            f"start_{route_id}",
            "endpoint",
            {"is_overnight": False, "is_kegel": False},
        ),
        (
            full_km_end,
            f"end_{route_id}",
            "endpoint",
            {"is_overnight": False, "is_kegel": False},
        ),
    ]

    if route_id in rj_by_route.groups:
        route_rjs = rj_by_route.get_group(route_id)
        for _, rj in route_rjs.iterrows():
            sec_junc_id = rj.get("SectionJunctionId")
            rj_node_id = (
                f"rj_{int(sec_junc_id)}"
                if pd.notna(sec_junc_id)
                else f"rj_id_{rj['Id']}"
            )
            km = (
                float(rj["RouteKmBegin"])
                if pd.notna(rj.get("RouteKmBegin"))
                else full_km_begin
            )
            points.append(
                (
                    km,
                    rj_node_id,
                    "junction",
                    {"is_overnight": False, "is_kegel": False},
                )
            )

    if route_id in berth_by_route.groups:
        route_berths = berth_by_route.get_group(route_id)
        for _, b in route_berths.iterrows():
            km = (
                float(b["RouteKmBegin"])
                if pd.notna(b.get("RouteKmBegin"))
                else full_km_begin
            )
            b_node_id = f"berth_{b['Id']}"
            points.append(
                (
                    km,
                    b_node_id,
                    "berth",
                    {
                        "berth_id": b["Id"],
                        "name": b["Name"],
                        "is_overnight": b["is_overnight"],
                        "is_kegel": b["is_kegel"],
                        "capacity": b["capacity_ships"],
                        "effective_length_m": b["effective_length_m"],
                        "has_cardropoff": b["has_cardropoff"],
                    },
                )
            )

    points.sort(key=lambda x: x[0])
    return points


def add_segment_edge_and_record(
    G: nx.Graph,
    route_segments: list[dict[str, Any]],
    id_a: str,
    id_b: str,
    edge_data: dict[str, Any],
) -> None:
    """Adds an edge to the graph (keeping minimal distance) and records segment metadata."""
    dist_km = edge_data["weight"]
    if G.has_edge(id_a, id_b):
        existing_weight = G[id_a][id_b].get("weight", float("inf"))
        if dist_km < existing_weight:
            G[id_a][id_b]["weight"] = dist_km
    else:
        G.add_edge(id_a, id_b, **edge_data)

    route_segments.append(
        {
            "source": id_a,
            "target": id_b,
            **edge_data,
        }
    )


def process_single_route(
    G: nx.Graph,
    route_segments: list[dict[str, Any]],
    route: pd.Series,
    rj_by_route: Any,
    berth_by_route: Any,
    type_map: dict[int, str],
) -> None:
    """Processes nodes, geometries, and edges for a single fairway route."""
    route_id = int(route["Id"])
    route_name = str(route.get("Name", f"Route {route_id}"))
    route_code = str(route.get("Code", ""))
    netwerk_type = type_map.get(route_id, "Niet geclassificeerd / Zee")
    full_geom = route.geometry

    full_km_begin = (
        float(route["RouteKmBegin"]) if pd.notna(route.get("RouteKmBegin")) else 0.0
    )
    full_km_end = (
        float(route["RouteKmEnd"])
        if pd.notna(route.get("RouteKmEnd"))
        else full_km_begin
    )

    points = extract_route_points(route, rj_by_route, berth_by_route)

    for i in range(len(points) - 1):
        km_a, id_a, type_a, attrs_a = points[i]
        km_b, id_b, type_b, attrs_b = points[i + 1]

        add_or_update_node(G, id_a, type_a, km_a, route_id, attrs_a)
        add_or_update_node(G, id_b, type_b, km_b, route_id, attrs_b)

        dist_km = abs(km_b - km_a)
        seg_geom = extract_segment_geometry(
            full_geom, km_a, km_b, full_km_begin, full_km_end
        )

        edge_data = {
            "weight": dist_km,
            "route_id": route_id,
            "route_name": route_name,
            "route_code": route_code,
            "netwerk_type": netwerk_type,
            "km_from": km_a,
            "km_to": km_b,
            "length_km": dist_km,
            "geometry": seg_geom,
        }

        add_segment_edge_and_record(G, route_segments, id_a, id_b, edge_data)


def build_route_network_graph(
    routes: gpd.GeoDataFrame,
    rjs: gpd.GeoDataFrame,
    berths: gpd.GeoDataFrame,
    type_map: dict[int, str],
) -> tuple[nx.Graph, list[dict[str, Any]]]:
    """Constructs a network graph from the route network, with berths and junctions as nodes."""
    logger.info("Constructing NetworkX route graph...")
    G = nx.Graph()
    route_segments: list[dict[str, Any]] = []

    rj_by_route = rjs.groupby("RouteId")
    berth_by_route = berths.groupby("RouteId")

    for _, route in routes.iterrows():
        process_single_route(
            G=G,
            route_segments=route_segments,
            route=route,
            rj_by_route=rj_by_route,
            berth_by_route=berth_by_route,
            type_map=type_map,
        )

    logger.info(
        "Built route graph: %d nodes, %d edges, %d route segments",
        G.number_of_nodes(),
        G.number_of_edges(),
        len(route_segments),
    )
    return G, route_segments
