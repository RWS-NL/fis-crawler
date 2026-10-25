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
from shapely import line_merge
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
    """Extracts a sliced segment geometry along a route between km_a and km_b."""
    km_span_total = abs(full_km_end - full_km_begin)
    if (
        full_geom is None
        or full_geom.is_empty
        or km_span_total <= 1e-4
        or full_geom.length <= 0
    ):
        return None

    if isinstance(full_geom, MultiLineString):
        full_geom = line_merge(full_geom)
        if isinstance(full_geom, MultiLineString):
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

    return substring(
        full_geom,
        min(f_a, f_b) * full_geom.length,
        max(f_a, f_b) * full_geom.length,
    )


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
            sec_junc_id = rj.get("section_junction_id")
            rj_node_id = (
                f"rj_{sec_junc_id}"
                if pd.notna(sec_junc_id) and sec_junc_id is not None
                else f"rj_id_{rj['id']}"
            )
            km = (
                float(rj["route_km_begin"])
                if pd.notna(rj.get("route_km_begin"))
                else full_km_begin
            )
            points.append((km, rj_node_id, "junction", dict(empty_flags)))

    if route_id in berth_by_route.groups:
        for _, b in berth_by_route.get_group(route_id).iterrows():
            raw_km = b.get("route_km", b.get("route_km_begin"))
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
) -> tuple[nx.Graph, list[dict[str, Any]]]:
    """Constructs an undirected network graph from routes, junctions, and berths."""
    logger.info("Constructing NetworkX route graph...")
    G = nx.Graph()
    route_segments: list[dict[str, Any]] = []

    rj_by_route = rjs.groupby("route_id")
    berth_by_route = berths.groupby("route_id")

    for _, route in routes.iterrows():
        route_id = route["id"]
        route_name = str(route.get("name") or f"Route {route_id}")
        route_code = str(route.get("code") or "")
        netwerk_type = type_map.get(route_id, "Niet geclassificeerd / Zee")
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
                "netwerk_type": netwerk_type,
                "km_from": km_a,
                "km_to": km_b,
                "length_km": dist_km,
                "geometry": seg_geom,
            }

            if G.has_edge(id_a, id_b):
                if dist_km < G[id_a][id_b].get("weight", float("inf")):
                    G[id_a][id_b]["weight"] = dist_km
            else:
                G.add_edge(id_a, id_b, **edge_data)

            route_segments.append({"source": id_a, "target": id_b, **edge_data})

    logger.info(
        "Built route graph: %d nodes, %d edges, %d route segments",
        G.number_of_nodes(),
        G.number_of_edges(),
        len(route_segments),
    )
    return G, route_segments
