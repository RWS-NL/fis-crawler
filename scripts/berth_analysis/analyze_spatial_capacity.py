"""Spatial capacity and network distance analysis for berths (ligplaatsen).

Analyzes berth capacity, kegel (hazardous goods) berths, and car drop-off facilities
against the spatial spacing criteria from Richtlijnen Vaarwegen 2020 (RVW 2020 § 6.3-6.6)
and the Integrale Mobiliteitsanalyse (IMA 2021 Achtergrondrapportage Vaarwegen § 2.3):
1. Overnight berth spacing: <= 30 km (<= 15 km to nearest berth along fairway).
2. Kegel berth spacing: <= 60 km (<= 30 km to nearest kegel berth along fairway).
3. Car drop-off facility: Required for overnight ports with capacity >= 15 ships.
4. Route-level capacity and maximum intensity calculation up to the ship-length step,
   split for regular overnight berths and kegel berths (RVW 2020 § 6.4.2).
"""

from __future__ import annotations

import logging
from pathlib import Path
import click
import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
from shapely.ops import substring

logger = logging.getLogger(__name__)

# RVW 2020 & navigation constants
NOMINAL_SHIP_LENGTH_M = 110.0  # CEMT Va standard vessel
CLEARANCE_SPACING_M = 10.0  # RVW 2020 p. 146 spacing s for class V+
SLOT_LENGTH_M = NOMINAL_SHIP_LENGTH_M + CLEARANCE_SPACING_M  # 120.0 m

NORMAL_DAY_SAILING_HOURS = 14.0  # Exploitatievorm A1: max 14 sailing hours per day
AVERAGE_SAILING_SPEED_KMH = 12.0  # Average commercial navigation speed (km/h)
DAILY_SAILING_DISTANCE_KM = (
    NORMAL_DAY_SAILING_HOURS * AVERAGE_SAILING_SPEED_KMH
)  # 168.0 km

MAX_CLUSTER_SPAN_KM = 0.5  # Max cluster diameter along fairway (500 m)
CAR_DROPOFF_MIN_CAPACITY_SHIPS = (
    15  # RVW § 6.6: capacity >= 15 ships requires car drop-off
)


def load_input_datasets(
    export_dir: Path,
) -> tuple[
    gpd.GeoDataFrame,
    gpd.GeoDataFrame,
    gpd.GeoDataFrame,
    gpd.GeoDataFrame,
    gpd.GeoDataFrame,
]:
    """Loads and validates required datasets from FIS export directory."""
    required = [
        "route.geoparquet",
        "routejunction.geoparquet",
        "berth.geoparquet",
        "cardropoff.geoparquet",
        "fairwayclassification.geoparquet",
    ]
    for fname in required:
        fpath = export_dir / fname
        if not fpath.exists():
            raise FileNotFoundError(f"Required input file missing: {fpath}")

    logger.info("Loading FIS datasets from %s", export_dir)
    routes = gpd.read_parquet(export_dir / "route.geoparquet")
    rjs = gpd.read_parquet(export_dir / "routejunction.geoparquet")
    berths = gpd.read_parquet(export_dir / "berth.geoparquet")
    cardropoffs = gpd.read_parquet(export_dir / "cardropoff.geoparquet")
    fc = gpd.read_parquet(export_dir / "fairwayclassification.geoparquet")

    return routes, rjs, berths, cardropoffs, fc


def build_network_classification_map(fc: gpd.GeoDataFrame) -> dict[int, str]:
    """Builds a mapping from RouteId to its highest fairway classification."""
    type_map: dict[int, str] = {}
    for rid, grp in fc.groupby("RouteId"):
        types = set(grp["TypeDescription"].dropna().unique())
        if "Hoofdtransportas" in types:
            type_map[int(rid)] = "Hoofdtransportas"
        elif "Hoofdvaarweg" in types:
            type_map[int(rid)] = "Hoofdvaarweg"
        elif "Overige vaarweg" in types:
            type_map[int(rid)] = "Overige vaarweg"
        else:
            type_map[int(rid)] = "Overig"
    return type_map


def classify_and_calculate_capacity(
    berths: gpd.GeoDataFrame, cardropoffs: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Classifies berths into overnight and kegel berths, calculating berth length and capacity."""
    df = berths.copy()

    cardropoff_berth_ids = set(cardropoffs["BerthId"].dropna().astype(int).unique())
    has_car_dropoff_fac = df["Facility"].astype(str).str.contains("CAR_DROPOFF")
    df["has_cardropoff"] = (
        df["Id"].astype(int).isin(cardropoff_berth_ids) | has_car_dropoff_fac
    )

    is_fac_overnight = df["Facility"].astype(str).str.contains("OVERNIGHT_ACCOMODATION")
    is_waiting = df["Category"].isin(["WAITING_AREA"])
    is_loading = df["Category"].isin(["LOADING_AND_UNLOADING"])
    is_long_stay = df["MaximumStayDurationH"].fillna(0) >= 12.0

    df["is_overnight"] = (is_fac_overnight & ~is_waiting) | (
        is_long_stay & ~is_waiting & ~is_loading
    )

    is_sig_kegel = df["Signalling"].isin(["ONE_BLUE_LIGHT", "TWO_BLUE_LIGHTS"])
    is_name_kegel = df["Name"].astype(str).str.lower().str.contains("kegel")
    df["is_kegel"] = is_sig_kegel | is_name_kegel

    def determine_kegels(row: pd.Series) -> int:
        sig = str(row.get("Signalling", ""))
        name = str(row.get("Name", "")).lower()
        if "TWO_BLUE_LIGHTS" in sig or "2 kegel" in name:
            return 2
        if "ONE_BLUE_LIGHT" in sig or "1 kegel" in name:
            return 1
        if row.get("is_kegel"):
            return 1
        return 0

    df["kegel_count"] = df.apply(determine_kegels, axis=1)

    def determine_capacity(row: pd.Series) -> int:
        num_rows_val = row.get("NumberOfRows")
        rows = int(num_rows_val) if pd.notna(num_rows_val) and num_rows_val > 0 else 1

        length_val = row.get("Length")
        if pd.notna(length_val) and float(length_val) > 0:
            len_m = float(length_val)
            cap = int(np.floor((len_m + CLEARANCE_SPACING_M) / SLOT_LENGTH_M)) * rows
            return max(1, cap)

        mooring_fac = row.get("NumberOfMooringFacilities")
        if pd.notna(mooring_fac) and float(mooring_fac) > 0:
            return int(float(mooring_fac))

        return 1

    df["capacity_ships"] = df.apply(determine_capacity, axis=1)

    # Effective berth length in meters (default to nominal vessel length if not registered)
    df["effective_length_m"] = df["Length"].fillna(NOMINAL_SHIP_LENGTH_M).astype(float)

    logger.info(
        "Classified %d berths: %d overnight (total length %.0fm, capacity %d ships), %d kegel (total length %.0fm)",
        len(df),
        df["is_overnight"].sum(),
        df.loc[df["is_overnight"], "effective_length_m"].sum(),
        df.loc[df["is_overnight"], "capacity_ships"].sum(),
        df["is_kegel"].sum(),
        df.loc[df["is_kegel"], "effective_length_m"].sum(),
    )
    return df


def build_route_network_graph(
    routes: gpd.GeoDataFrame,
    rjs: gpd.GeoDataFrame,
    berths: gpd.GeoDataFrame,
    type_map: dict[int, str],
) -> tuple[nx.Graph, list[dict]]:
    """Constructs a network graph from the simplified route network, with berths and junctions as nodes."""
    logger.info("Constructing NetworkX route graph...")
    G = nx.Graph()
    route_segments: list[dict] = []

    rj_by_route = rjs.groupby("RouteId")
    berth_by_route = berths.groupby("RouteId")

    for _, route in routes.iterrows():
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
        km_span_total = abs(full_km_end - full_km_begin)

        points: list[tuple[float, str, str, dict]] = [
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

        for i in range(len(points) - 1):
            km_a, id_a, type_a, attrs_a = points[i]
            km_b, id_b, type_b, attrs_b = points[i + 1]

            if id_a not in G:
                G.add_node(id_a, type=type_a, km=km_a, route_id=route_id, **attrs_a)
            else:
                for k, v in attrs_a.items():
                    if v:
                        G.nodes[id_a][k] = v

            if id_b not in G:
                G.add_node(id_b, type=type_b, km=km_b, route_id=route_id, **attrs_b)
            else:
                for k, v in attrs_b.items():
                    if v:
                        G.nodes[id_b][k] = v

            dist_km = abs(km_b - km_a)

            seg_geom = None
            if full_geom is not None and km_span_total > 1e-4 and full_geom.length > 0:
                f_a = max(0.0, min(1.0, (km_a - full_km_begin) / km_span_total))
                f_b = max(0.0, min(1.0, (km_b - full_km_begin) / km_span_total))
                if abs(f_b - f_a) > 1e-6:
                    seg_geom = substring(
                        full_geom,
                        min(f_a, f_b) * full_geom.length,
                        max(f_a, f_b) * full_geom.length,
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

    logger.info(
        "Built route graph: %d nodes, %d edges, %d route segments",
        G.number_of_nodes(),
        G.number_of_edges(),
        len(route_segments),
    )
    return G, route_segments


def calc_segment_furthest_distance(d_u: float, d_v: float, L: float) -> float:
    """Calculates the furthest distance along an edge (u, v) of length L to the nearest facility.

    Conform metric graph theory:
    Any point x in [0, L] has distance min(d_u + x, d_v + L - x).
    The maximum occurs where d_u + x = d_v + L - x => x = (d_v - d_u + L) / 2.
    If within [0, L], the peak distance is (d_u + d_v + L) / 2.
    """
    if d_u == float("inf") and d_v == float("inf"):
        return float("inf")
    if d_u == float("inf"):
        return d_v + L
    if d_v == float("inf"):
        return d_u + L
    if abs(d_u - d_v) <= L:
        return (d_u + d_v + L) / 2.0
    return max(d_u, d_v)


def calc_segment_compliant_length(
    d_u: float, d_v: float, L: float, threshold_radius: float
) -> float:
    """Calculates the exact continuous length along edge (u, v) within threshold_radius of a facility."""
    if L <= 0.0:
        return 0.0
    reach_u = max(0.0, threshold_radius - d_u) if d_u != float("inf") else 0.0
    reach_v = max(0.0, threshold_radius - d_v) if d_v != float("inf") else 0.0
    return min(L, reach_u + reach_v)


def run_spatial_distance_analysis(
    G: nx.Graph, route_segments: list[dict]
) -> gpd.GeoDataFrame:
    """Calculates network distances and continuous coverage from all segments to nearest berths."""
    overnight_nodes = [n for n, d in G.nodes(data=True) if d.get("is_overnight")]
    kegel_nodes = [n for n, d in G.nodes(data=True) if d.get("is_kegel")]

    if not overnight_nodes:
        raise ValueError(
            "No overnight berth nodes found in the network graph. Cannot calculate distances."
        )
    if not kegel_nodes:
        raise ValueError(
            "No kegel berth nodes found in the network graph. Cannot calculate distances."
        )

    logger.info(
        "Running Dijkstra from %d overnight nodes and %d kegel nodes",
        len(overnight_nodes),
        len(kegel_nodes),
    )

    dist_overnight = nx.multi_source_dijkstra_path_length(
        G, overnight_nodes, weight="weight"
    )
    dist_kegel = nx.multi_source_dijkstra_path_length(G, kegel_nodes, weight="weight")

    records = []
    for seg in route_segments:
        u = seg["source"]
        v = seg["target"]
        d_u_ov = dist_overnight.get(u, float("inf"))
        d_v_ov = dist_overnight.get(v, float("inf"))
        d_u_kg = dist_kegel.get(u, float("inf"))
        d_v_kg = dist_kegel.get(v, float("inf"))
        seg_len = seg["length_km"]

        max_dist_ov = calc_segment_furthest_distance(d_u_ov, d_v_ov, seg_len)
        max_dist_kg = calc_segment_furthest_distance(d_u_kg, d_v_kg, seg_len)

        # RVW 2020: 30 km overnight spacing => radius <= 15 km; 60 km kegel spacing => radius <= 30 km
        comp_len_ov = calc_segment_compliant_length(d_u_ov, d_v_ov, seg_len, 15.0)
        comp_len_kg = calc_segment_compliant_length(d_u_kg, d_v_kg, seg_len, 30.0)

        records.append(
            {
                **seg,
                "dist_to_overnight_km": round(max_dist_ov, 2),
                "compliant_30km_overnight_km": round(comp_len_ov, 3),
                "is_fully_compliant_overnight": max_dist_ov <= 15.0,
                "dist_to_kegel_km": round(max_dist_kg, 2),
                "compliant_60km_kegel_km": round(comp_len_kg, 3),
                "is_fully_compliant_kegel": max_dist_kg <= 30.0,
            }
        )

    gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
    return gdf


def cluster_berths_and_check_facilities(berths: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Clusters nearby berths by route into locations and checks autoafzetplaats requirement (RVW § 6.6)."""
    overnight = berths[berths["is_overnight"]].copy()

    clusters: list[dict] = []
    for route_id, group in overnight.groupby("RouteId"):
        group = group.sort_values("RouteKmBegin")
        curr_cluster: list[pd.Series] = []
        cluster_start_km = None

        for _, row in group.iterrows():
            km = row["RouteKmBegin"]
            if (
                cluster_start_km is None
                or abs(km - cluster_start_km) <= MAX_CLUSTER_SPAN_KM
            ):
                if cluster_start_km is None:
                    cluster_start_km = km
                curr_cluster.append(row)
            else:
                clusters.append(_aggregate_cluster(curr_cluster))
                curr_cluster = [row]
                cluster_start_km = km
        if curr_cluster:
            clusters.append(_aggregate_cluster(curr_cluster))

    cluster_df = pd.DataFrame(clusters)
    if cluster_df.empty:
        return gpd.GeoDataFrame(crs="EPSG:4326")

    cluster_df["req_cardropoff"] = (
        cluster_df["total_capacity_ships"] >= CAR_DROPOFF_MIN_CAPACITY_SHIPS
    )
    cluster_df["cardropoff_compliant"] = (
        ~cluster_df["req_cardropoff"] | cluster_df["has_cardropoff"]
    )

    return gpd.GeoDataFrame(cluster_df, geometry="geometry", crs="EPSG:4326")


def _aggregate_cluster(rows: list[pd.Series]) -> dict:
    """Helper to aggregate a list of berth rows into a cluster dictionary."""
    df = pd.DataFrame(rows)
    names = ", ".join(df["Name"].dropna().unique()[:3])
    total_cap = df["capacity_ships"].sum()
    total_len = df["effective_length_m"].sum()
    has_cardrop = df["has_cardropoff"].any()
    kegel_cap = df[df["is_kegel"]]["capacity_ships"].sum()
    kegel_len = df[df["is_kegel"]]["effective_length_m"].sum()

    geom = df.iloc[0]["geometry"]

    return {
        "route_id": df.iloc[0]["RouteId"],
        "route_km_begin": df["RouteKmBegin"].min(),
        "route_km_end": df["RouteKmBegin"].max(),
        "cluster_name": names,
        "num_berth_records": len(df),
        "total_capacity_ships": int(total_cap),
        "total_length_m": float(total_len),
        "has_cardropoff": bool(has_cardrop),
        "kegel_capacity_ships": int(kegel_cap),
        "kegel_length_m": float(kegel_len),
        "geometry": geom,
    }


def compute_route_capacity_and_intensity(
    routes: gpd.GeoDataFrame,
    berths: gpd.GeoDataFrame,
    gdf_segments: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Aggregates berth capacity, coverage, and maximum carrying capacity up to the ship-length step per route.

    Conform RVW 2020 § 6.4.2:
    - Stop probability per passage: P_stop = L_route / DAILY_SAILING_DISTANCE_KM (168 km).
    - Available berth-meter nights per year: L_kade * 365.
    - Max carrying capacity in ship length kilometers per year:
      (L_kade / 1000) * (DAILY_SAILING_DISTANCE_KM / L_route) * 365.
    Split strictly between regular overnight berths and kegel berths.
    """
    logger.info("Computing route-level capacity and maximum carrying capacity...")

    ov_berths = (
        berths[berths["is_overnight"]]
        .groupby("RouteId")
        .agg(
            num_overnight_berths=("Id", "count"),
            overnight_berth_length_m=("effective_length_m", "sum"),
            overnight_capacity_ships=("capacity_ships", "sum"),
            has_cardropoff=("has_cardropoff", "any"),
        )
        .reset_index()
    )

    kg_berths = (
        berths[berths["is_kegel"]]
        .groupby("RouteId")
        .agg(
            num_kegel_berths=("Id", "count"),
            kegel_berth_length_m=("effective_length_m", "sum"),
            kegel_capacity_ships=("capacity_ships", "sum"),
        )
        .reset_index()
    )

    route_cov = (
        gdf_segments.groupby("route_id")
        .agg(
            total_length_km=("length_km", "sum"),
            overnight_compliant_km=("compliant_30km_overnight_km", "sum"),
            max_dist_to_overnight_km=("dist_to_overnight_km", "max"),
            kegel_compliant_km=("compliant_60km_kegel_km", "sum"),
            max_dist_to_kegel_km=("dist_to_kegel_km", "max"),
            netwerk_type=("netwerk_type", "first"),
            route_name=("route_name", "first"),
            route_code=("route_code", "first"),
        )
        .reset_index()
    )

    df = route_cov.merge(
        ov_berths.rename(columns={"RouteId": "route_id"}), on="route_id", how="left"
    )
    df = df.merge(
        kg_berths.rename(columns={"RouteId": "route_id"}), on="route_id", how="left"
    )

    df["num_overnight_berths"] = df["num_overnight_berths"].fillna(0).astype(int)
    df["overnight_berth_length_m"] = df["overnight_berth_length_m"].fillna(0.0).round(1)
    df["overnight_capacity_ships"] = (
        df["overnight_capacity_ships"].fillna(0).astype(int)
    )
    df["has_cardropoff"] = df["has_cardropoff"].fillna(False).astype(bool)

    df["num_kegel_berths"] = df["num_kegel_berths"].fillna(0).astype(int)
    df["kegel_berth_length_m"] = df["kegel_berth_length_m"].fillna(0.0).round(1)
    df["kegel_capacity_ships"] = df["kegel_capacity_ships"].fillna(0).astype(int)

    # Coverage percentages
    df["overnight_coverage_pct"] = np.where(
        df["total_length_km"] > 0,
        (df["overnight_compliant_km"] / df["total_length_km"] * 100.0).round(1),
        0.0,
    )
    df["kegel_coverage_pct"] = np.where(
        df["total_length_km"] > 0,
        (df["kegel_compliant_km"] / df["total_length_km"] * 100.0).round(1),
        0.0,
    )

    # Stop probability per passage (RVW § 6.4.2)
    df["p_overnight_stop"] = np.where(
        df["total_length_km"] > 0,
        (df["total_length_km"] / DAILY_SAILING_DISTANCE_KM).round(4),
        0.0,
    )

    # Available meter-nights per year
    df["overnight_meter_nights_yr"] = (df["overnight_berth_length_m"] * 365.0).round(1)
    df["kegel_meter_nights_yr"] = (df["kegel_berth_length_m"] * 365.0).round(1)

    # Maximum annual carrying capacity in ship-length kilometers per year
    # (L_kade / 1000) * (DAILY_SAILING_DISTANCE_KM / L_route) * 365
    df["max_capacity_ship_length_km_yr"] = np.where(
        df["total_length_km"] > 0,
        (
            (df["overnight_berth_length_m"] / 1000.0)
            * (DAILY_SAILING_DISTANCE_KM / df["total_length_km"])
            * 365.0
        ).round(1),
        0.0,
    )

    df["max_kegel_ship_length_km_yr"] = np.where(
        df["total_length_km"] > 0,
        (
            (df["kegel_berth_length_m"] / 1000.0)
            * (DAILY_SAILING_DISTANCE_KM / df["total_length_km"])
            * 365.0
        ).round(1),
        0.0,
    )

    routes_geom = routes.set_index("Id")["geometry"]
    df["geometry"] = df["route_id"].map(routes_geom)

    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    return gdf


def generate_markdown_summary(
    gdf_segments: gpd.GeoDataFrame,
    gdf_clusters: gpd.GeoDataFrame,
    gdf_routes: gpd.GeoDataFrame,
    berths: gpd.GeoDataFrame,
    output_path: Path,
) -> None:
    """Generates a structured markdown report of the spatial berth analysis."""
    netwerk_stats: dict[str, dict] = {}
    for ntype in [
        "Hoofdtransportas",
        "Hoofdvaarweg",
        "Overige vaarweg",
        "Niet geclassificeerd / Zee",
    ]:
        grp = gdf_segments[gdf_segments["netwerk_type"] == ntype]
        tot = grp["length_km"].sum()
        ov = grp["compliant_30km_overnight_km"].sum()
        kg = grp["compliant_60km_kegel_km"].sum()
        netwerk_stats[ntype] = {
            "type": ntype,
            "tot_km": tot,
            "ov_km": ov,
            "ov_pct": (ov / tot * 100.0) if tot > 0 else 0.0,
            "kg_km": kg,
            "kg_pct": (kg / tot * 100.0) if tot > 0 else 0.0,
        }

    hvwn_tot = sum(
        netwerk_stats.get(t, {}).get("tot_km", 0.0)
        for t in ["Hoofdtransportas", "Hoofdvaarweg"]
    )
    hvwn_ov = sum(
        netwerk_stats.get(t, {}).get("ov_km", 0.0)
        for t in ["Hoofdtransportas", "Hoofdvaarweg"]
    )
    hvwn_kg = sum(
        netwerk_stats.get(t, {}).get("kg_km", 0.0)
        for t in ["Hoofdtransportas", "Hoofdvaarweg"]
    )
    hvwn_ov_pct = (hvwn_ov / hvwn_tot * 100.0) if hvwn_tot > 0 else 0.0
    hvwn_kg_pct = (hvwn_kg / hvwn_tot * 100.0) if hvwn_tot > 0 else 0.0

    large_clusters = (
        gdf_clusters[gdf_clusters["req_cardropoff"]]
        if not gdf_clusters.empty
        else pd.DataFrame()
    )
    missing_cardrop = (
        large_clusters[~large_clusters["cardropoff_compliant"]]
        if not large_clusters.empty
        else pd.DataFrame()
    )

    hta_stat = netwerk_stats.get(
        "Hoofdtransportas",
        {"tot_km": 0.0, "ov_km": 0.0, "ov_pct": 0.0, "kg_km": 0.0, "kg_pct": 0.0},
    )
    hvw_stat = netwerk_stats.get(
        "Hoofdvaarweg",
        {"tot_km": 0.0, "ov_km": 0.0, "ov_pct": 0.0, "kg_km": 0.0, "kg_pct": 0.0},
    )
    ovg_stat = netwerk_stats.get(
        "Overige vaarweg",
        {"tot_km": 0.0, "ov_km": 0.0, "ov_pct": 0.0, "kg_km": 0.0, "kg_pct": 0.0},
    )
    unc_stat = netwerk_stats.get(
        "Niet geclassificeerd / Zee",
        {"tot_km": 0.0, "ov_km": 0.0, "ov_pct": 0.0, "kg_km": 0.0, "kg_pct": 0.0},
    )

    md = f"""# Ruimtelijke Analyse Ligplaatscapaciteit en Maximale Draagkracht (RVW 2020 & IMA 2021)

## 1. Samenvatting Netwerkdekking per Netwerkcategorie

Conform de **Richtlijnen Vaarwegen 2020 (§ 6.3 - 6.4)** en de **Achtergrondrapportage Vaarwegen IMA 2021 (§ 2.3)** gelden de volgende ruimtelijke normen:
* **Overnachtingshavens**: maximaal **30 km** tussenruimte (elke locatie ligt binnen $\\le 15$ km van een ligplaats).
* **Kegelligplaatsen**: maximaal **60 km** tussenruimte (elke locatie ligt binnen $\\le 30$ km van een kegelligplaats).

| Netwerkcategorie | Totale lengte (km) | Overnachten $\\le 30$ km | Overnachten (%) | Kegel $\\le 60$ km | Kegel (%) |
|---|---|---|---|---|---|
| **Hoofdtransportassen (HTA)** | {hta_stat["tot_km"]:,.1f} km | {hta_stat["ov_km"]:,.1f} km | **{hta_stat["ov_pct"]:.1f}%** | {hta_stat["kg_km"]:,.1f} km | **{hta_stat["kg_pct"]:.1f}%** |
| **Hoofdvaarwegen (HVW)** | {hvw_stat["tot_km"]:,.1f} km | {hvw_stat["ov_km"]:,.1f} km | **{hvw_stat["ov_pct"]:.1f}%** | {hvw_stat["kg_km"]:,.1f} km | **{hvw_stat["kg_pct"]:.1f}%** |
| **Totaal HVWN (HTA + HVW)** | **{hvwn_tot:,.1f} km** | **{hvwn_ov:,.1f} km** | **{hvwn_ov_pct:.1f}%** | **{hvwn_kg:,.1f} km** | **{hvwn_kg_pct:.1f}%** |
| Overige vaarwegen | {ovg_stat["tot_km"]:,.1f} km | {ovg_stat["ov_km"]:,.1f} km | {ovg_stat["ov_pct"]:.1f}% | {ovg_stat["kg_km"]:,.1f} km | {ovg_stat["kg_pct"]:.1f}% |
| Niet geclassificeerd (kust/zee) | {unc_stat["tot_km"]:,.1f} km | {unc_stat["ov_km"]:,.1f} km | {unc_stat["ov_pct"]:.1f}% | {unc_stat["kg_km"]:,.1f} km | {unc_stat["kg_pct"]:.1f}% |

---

## 2. Ligplaatscapaciteit en Aanbod

* **Totaal aantal ligplaatsrecords in FIS**: {len(berths):,}
* **Aangemerkt als overnachtingsligplaats**: {berths["is_overnight"].sum():,} records
* **Totale kadelengte overnachten**: **{berths.loc[berths["is_overnight"], "effective_length_m"].sum():,.1f} m**
* **Totale kadecapaciteit overnachten**: **{berths.loc[berths["is_overnight"], "effective_length_m"].sum() * 365.0:,.0f} meter-nachten per jaar**
* **Kegelligplaatsen (gevaarlijke stoffen)**: {berths["is_kegel"].sum():,} records ({berths.loc[berths["is_kegel"], "effective_length_m"].sum():,.1f} m kadelengte)
* **Aantal geclusterde overnachtingslocaties**: {len(gdf_clusters):,} locaties

---

## 3. Autoafzetplaatsen Toetsing (RVW § 6.6)
* **Grote overnachtingslocaties (capaciteit $\\ge 15$ schepen)**: {len(large_clusters)} locaties
* **Waarvan mét geregistreerde autoafzetplaats**: {len(large_clusters) - len(missing_cardrop)} locaties
* **Ontbrekende autoafzetplaatsen**: {len(missing_cardrop)} locaties

"""
    if not missing_cardrop.empty:
        md += (
            "### Grote overnachtingslocaties zonder geregistreerde autoafzetplaats:\n\n"
        )
        md += "| Route ID | Locatie / Naam | Capaciteit (schepen) | Kadelengte (m) |\n|---|---|---|---|\n"
        for _, r in missing_cardrop.iterrows():
            md += f"| {r['route_id']} | {r['cluster_name']} | {r['total_capacity_ships']} | {r['total_length_m']:.0f}m |\n"

    # Specific gaps on Hoofdtransportassen and Hoofdvaarwegen
    md += "\n---\n\n## 4. Knelpunten op Hoofdtransportassen en Hoofdvaarwegen (Gap > 30 km)\n\n"
    hvwn_grp = gdf_segments[
        gdf_segments["netwerk_type"].isin(["Hoofdtransportas", "Hoofdvaarweg"])
    ]
    hvwn_gaps = (
        hvwn_grp[~hvwn_grp["is_fully_compliant_overnight"]]
        .groupby(["route_id", "route_name", "netwerk_type"])
        .agg(gap_km=("length_km", "sum"), max_dist=("dist_to_overnight_km", "max"))
        .reset_index()
        .sort_values("gap_km", ascending=False)
        .head(15)
    )
    md += "| Route ID | Routenaam | Netwerktype | Trajectlengte zonder volledige dekking (km) | Max. afstand tot ligplaats (km) |\n|---|---|---|---|---|\n"
    for _, r in hvwn_gaps.iterrows():
        md += f"| {r['route_id']} | {r['route_name']} | {r['netwerk_type']} | {r['gap_km']:.1f} km | {r['max_dist']:.1f} km |\n"

    # Section 5: Maximum Carrying Capacity (Draagkracht) per Route
    md += "\n---\n\n## 5. Maximale Draagkracht per Route (RVW 2020 § 6.4.2)\n\n"
    md += (
        "Berekend tot de stap waar scheepslengte nodig is. Toont per route de beschikbare kadelengte en de maximale "
        "jaardraagkracht in scheepslengte-kilometers per jaar (zowel regulier overnachten als kegel):\n\n"
    )
    md += "| Code | Routenaam | Categorie | Lengte (km) | Overnachting Kade (m) | Max. Draagkracht Overnachten (km schip/jr) | Kegel Kade (m) | Max. Draagkracht Kegel (km schip/jr) | Dekking Overnacht (%) |\n"
    md += "|---|---|---|---|---|---|---|---|---|\n"

    top_cap = gdf_routes.sort_values(
        "max_capacity_ship_length_km_yr", ascending=False
    ).head(20)
    for _, r in top_cap.iterrows():
        ov_kade_str = (
            f"{r['overnight_berth_length_m']:,.0f} m"
            if r["overnight_berth_length_m"] > 0
            else "-"
        )
        ov_draag_str = (
            f"{r['max_capacity_ship_length_km_yr']:,.1f} km"
            if r["max_capacity_ship_length_km_yr"] > 0
            else "-"
        )
        kg_kade_str = (
            f"{r['kegel_berth_length_m']:,.0f} m"
            if r["kegel_berth_length_m"] > 0
            else "-"
        )
        kg_draag_str = (
            f"{r['max_kegel_ship_length_km_yr']:,.1f} km"
            if r["max_kegel_ship_length_km_yr"] > 0
            else "-"
        )
        md += (
            f"| {r['route_code']} | {r['route_name']} | {r['netwerk_type']} | {r['total_length_km']:.1f} km | "
            f"{ov_kade_str} | {ov_draag_str} | {kg_kade_str} | {kg_draag_str} | {r['overnight_coverage_pct']:.1f}% |\n"
        )

    output_path.write_text(md, encoding="utf-8")
    logger.info("Summary report written to %s", output_path)


@click.command()
@click.option(
    "--export-dir",
    type=click.Path(exists=True, path_type=Path),
    default=Path("output/fis-export"),
    help="Directory containing FIS exported parquet files.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("output/berth-analysis"),
    help="Directory to save analysis outputs.",
)
def main(export_dir: Path, output_dir: Path) -> None:
    """Runs spatial capacity and distance analysis for berths across the route network."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    routes, rjs, berths, cardropoffs, fc = load_input_datasets(export_dir)
    type_map = build_network_classification_map(fc)

    classified_berths = classify_and_calculate_capacity(berths, cardropoffs)
    classified_berths.to_parquet(output_dir / "berths_analyzed.geoparquet")

    gdf_clusters = cluster_berths_and_check_facilities(classified_berths)
    if not gdf_clusters.empty:
        gdf_clusters.to_file(output_dir / "berth_clusters.geojson", driver="GeoJSON")

    G, route_segments = build_route_network_graph(
        routes, rjs, classified_berths, type_map
    )
    gdf_segments = run_spatial_distance_analysis(G, route_segments)

    gdf_routes = compute_route_capacity_and_intensity(
        routes, classified_berths, gdf_segments
    )

    logger.info("Exporting route compliance and intensity outputs...")
    # 1. Primary GeoParquet exports
    gdf_routes.to_parquet(output_dir / "route_capacity_and_intensity.geoparquet")
    gdf_segments.to_parquet(output_dir / "route_segments_compliance.geoparquet")

    # 2. Supplementary CSV and GeoJSON exports
    gdf_routes.drop(columns=["geometry"], errors="ignore").to_csv(
        output_dir / "route_capacity_and_intensity.csv", index=False
    )
    gdf_routes.to_file(
        output_dir / "route_capacity_and_intensity.geojson", driver="GeoJSON"
    )
    gdf_segments.to_file(
        output_dir / "route_segments_compliance.geojson", driver="GeoJSON"
    )

    generate_markdown_summary(
        gdf_segments,
        gdf_clusters,
        gdf_routes,
        classified_berths,
        output_dir / "berth_spatial_report.md",
    )
    logger.info("Analysis complete! Output saved in %s", output_dir)


if __name__ == "__main__":
    main()
