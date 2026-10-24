"""Spatial capacity and network distance analysis for berths (ligplaatsen).

Analyzes berth capacity, kegel (hazardous goods) berths, and car drop-off facilities
against the spatial spacing criteria from Richtlijnen Vaarwegen 2020 (RVW 2020 § 6.3-6.4)
and the Integrale Mobiliteitsanalyse (IMA 2021 Achtergrondrapportage Vaarwegen § 2.3):
1. Overnight berth spacing: <= 30 km (<= 15 km to nearest berth along fairway).
2. Kegel berth spacing: <= 60 km (<= 30 km to nearest kegel berth along fairway).
3. Car drop-off facility: Required for overnight clusters with >= 15 berths.
4. Route-level capacity and intensity comparison (trips, tonnage, and ship-length meters).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
import click
import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
from shapely.ops import substring

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

NOMINAL_SHIP_LENGTH_M = 110.0  # CEMT Va standard vessel
CLEARANCE_SPACING_M = 10.0  # RVW 2020 p. 146 spacing s for class V+
SLOT_LENGTH_M = NOMINAL_SHIP_LENGTH_M + CLEARANCE_SPACING_M  # 120.0 m


def load_input_datasets(
    export_dir: Path,
) -> tuple[
    gpd.GeoDataFrame,
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
        "section.geoparquet",
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
    sections = gpd.read_parquet(export_dir / "section.geoparquet")

    return routes, rjs, berths, cardropoffs, fc, sections


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
    """Classifies berths into overnight, kegel, and calculates physical capacity."""
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
        if "TWO_BLUE_LIGHTS" in sig or "2 kegel" in name or "2-kegel" in name:
            return 2
        elif (
            "ONE_BLUE_LIGHT" in sig
            or "1 kegel" in name
            or "1-kegel" in name
            or "kegel" in name
        ):
            return 1
        return 0

    df["kegel_count"] = df.apply(determine_kegels, axis=1)

    rows = df["NumberOfRows"].fillna(1).astype(float)
    rows = rows.where(rows > 0, 1.0)

    length = df["Length"].fillna(0).astype(float)
    derived_cap = np.maximum(1, np.floor(length / SLOT_LENGTH_M) * rows)

    explicit_cap = df["NumberOfMooringFacilities"].astype(float)
    df["capacity_ships"] = np.where(
        explicit_cap.notna() & (explicit_cap > 0), explicit_cap, derived_cap
    ).astype(int)

    logger.info(
        "Classified %d berths: %d overnight (total capacity %d ships), %d kegel",
        len(df),
        df["is_overnight"].sum(),
        df.loc[df["is_overnight"], "capacity_ships"].sum(),
        df["is_kegel"].sum(),
    )
    return df


def build_route_network_graph(
    routes: gpd.GeoDataFrame,
    rjs: gpd.GeoDataFrame,
    berths: gpd.GeoDataFrame,
    type_map: dict[int, str],
) -> tuple[nx.Graph, list[dict]]:
    """Builds the route-level NetworkX graph with berths and junctions spliced in."""
    G = nx.Graph()
    route_segments: list[dict] = []

    routes_by_id = routes.set_index("Id")

    for route_id, junc_group in rjs.groupby("RouteId"):
        if route_id not in routes_by_id.index:
            continue
        route_row = routes_by_id.loc[route_id]
        route_name = route_row["Name"]
        route_code = route_row["Code"]
        netwerk_type = type_map.get(int(route_id), "Niet geclassificeerd / Zee")
        full_geom = route_row.geometry
        full_km_begin = float(route_row["RouteKmBegin"])
        full_km_end = float(route_row["RouteKmEnd"])
        km_span_total = full_km_end - full_km_begin

        points: list[tuple[float, str, str, dict]] = []

        for _, rj in junc_group.iterrows():
            km = float(rj["RouteKmBegin"])
            junc_id = f"junc_{rj['SectionJunctionId']}"
            points.append(
                (km, junc_id, "junction", {"junc_id": rj["SectionJunctionId"]})
            )

        route_berths = berths[berths["RouteId"] == route_id]
        for _, b in route_berths.iterrows():
            km = float(b["RouteKmBegin"])
            b_id = f"berth_{b['Id']}"
            points.append(
                (
                    km,
                    b_id,
                    "berth",
                    {
                        "berth_id": b["Id"],
                        "name": b["Name"],
                        "is_overnight": b["is_overnight"],
                        "is_kegel": b["is_kegel"],
                        "capacity": b["capacity_ships"],
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
            if id_b not in G:
                G.add_node(id_b, type=type_b, km=km_b, route_id=route_id, **attrs_b)

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
            G.add_edge(id_a, id_b, **edge_data)

            route_segments.append(
                {
                    "source": id_a,
                    "target": id_b,
                    **edge_data,
                }
            )

    logger.info(
        "Built route graph: %d nodes, %d edges",
        G.number_of_nodes(),
        G.number_of_edges(),
    )
    return G, route_segments


def run_spatial_distance_analysis(
    G: nx.Graph, route_segments: list[dict]
) -> gpd.GeoDataFrame:
    """Calculates network distances from all segments to nearest overnight and kegel berths."""
    overnight_nodes = [n for n, d in G.nodes(data=True) if d.get("is_overnight")]
    kegel_nodes = [n for n, d in G.nodes(data=True) if d.get("is_kegel")]

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
        dist_mid_ov = min(d_u_ov, d_v_ov) + 0.5 * seg_len
        dist_mid_kg = min(d_u_kg, d_v_kg) + 0.5 * seg_len

        compliant_30km_overnight = dist_mid_ov <= 15.0
        compliant_60km_kegel = dist_mid_kg <= 30.0

        records.append(
            {
                **seg,
                "dist_to_overnight_km": round(dist_mid_ov, 2),
                "dist_to_kegel_km": round(dist_mid_kg, 2),
                "compliant_30km_overnight": compliant_30km_overnight,
                "compliant_60km_kegel": compliant_60km_kegel,
            }
        )

    gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
    return gdf


def cluster_berths_and_check_facilities(berths: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Clusters nearby berths by route into locations and checks autoafzetplaats requirement."""
    overnight = berths[berths["is_overnight"]].copy()

    clusters: list[dict] = []
    for route_id, group in overnight.groupby("RouteId"):
        group = group.sort_values("RouteKmBegin")
        curr_cluster: list[pd.Series] = []
        curr_km = None

        for _, row in group.iterrows():
            km = row["RouteKmBegin"]
            if curr_km is None or abs(km - curr_km) <= 0.5:
                curr_cluster.append(row)
                curr_km = km
            else:
                clusters.append(_aggregate_cluster(curr_cluster))
                curr_cluster = [row]
                curr_km = km
        if curr_cluster:
            clusters.append(_aggregate_cluster(curr_cluster))

    cluster_df = pd.DataFrame(clusters)
    if cluster_df.empty:
        return gpd.GeoDataFrame(crs="EPSG:4326")

    cluster_df["req_cardropoff"] = cluster_df["total_capacity_ships"] >= 15
    cluster_df["cardropoff_compliant"] = (
        ~cluster_df["req_cardropoff"] | cluster_df["has_cardropoff"]
    )

    return gpd.GeoDataFrame(cluster_df, geometry="geometry", crs="EPSG:4326")


def _aggregate_cluster(rows: list[pd.Series]) -> dict:
    """Helper to aggregate a list of berth rows into a cluster dictionary."""
    df = pd.DataFrame(rows)
    names = ", ".join(df["Name"].dropna().unique()[:3])
    total_cap = df["capacity_ships"].sum()
    total_len = df["Length"].fillna(0).sum()
    has_cardrop = df["has_cardropoff"].any()
    kegel_cap = df[df["is_kegel"]]["capacity_ships"].sum()

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
        "geometry": geom,
    }


def compute_route_capacity_and_intensity(
    routes: gpd.GeoDataFrame,
    sections: gpd.GeoDataFrame,
    berths: gpd.GeoDataFrame,
    gdf_segments: gpd.GeoDataFrame,
    intensity_path: Path | None = None,
    dtv_path: Path | None = None,
) -> gpd.GeoDataFrame:
    """Aggregates berth capacity, coverage, and maximum traffic intensity (trips, tonnage, ship-meters) per route."""
    logger.info("Computing route-level capacity and intensity comparison dataset...")

    ov_berths = (
        berths[berths["is_overnight"]]
        .groupby("RouteId")
        .agg(
            num_overnight_berths=("Id", "count"),
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
            kegel_capacity_ships=("capacity_ships", "sum"),
        )
        .reset_index()
    )

    route_cov = (
        gdf_segments.groupby("route_id")
        .agg(
            total_length_km=("length_km", "sum"),
            overnight_compliant_km=(
                "length_km",
                lambda x: x[
                    gdf_segments.loc[x.index, "compliant_30km_overnight"]
                ].sum(),
            ),
            max_dist_to_overnight_km=("dist_to_overnight_km", "max"),
            kegel_compliant_km=(
                "length_km",
                lambda x: x[gdf_segments.loc[x.index, "compliant_60km_kegel"]].sum(),
            ),
            max_dist_to_kegel_km=("dist_to_kegel_km", "max"),
            netwerk_type=("netwerk_type", "first"),
            route_name=("route_name", "first"),
            route_code=("route_code", "first"),
        )
        .reset_index()
    )

    route_int = pd.DataFrame(
        columns=[
            "RouteId",
            "max_trip_count",
            "max_ship_length_km",
            "max_cargo_weight_ton",
            "avg_trip_count",
            "avg_ship_length_m",
        ]
    )

    if intensity_path is not None and intensity_path.exists():
        logger.info("Loading intensity data from %s", intensity_path)
        intensity = gpd.read_parquet(intensity_path)

        tons_ref = np.array(
            [
                118.0,
                312.0,
                490.0,
                677.0,
                995.0,
                1272.0,
                1722.0,
                2286.0,
                3315.0,
                4636.0,
                6923.0,
            ]
        )
        lengths_ref = np.array(
            [28.0, 39.0, 55.0, 70.0, 85.0, 85.0, 105.0, 110.0, 135.0, 105.0, 185.0]
        )

        if dtv_path is not None and dtv_path.exists():
            with open(dtv_path, encoding="utf-8") as f:
                dtv = json.load(f)
            df_dtv = (
                pd.DataFrame(dtv)
                .dropna(subset=["Length [m]", "Load Weight average [ton]"])
                .sort_values("Load Weight average [ton]")
            )
            tons_ref = df_dtv["Load Weight average [ton]"].values
            lengths_ref = df_dtv["Length [m]"].values

        intensity["avg_cargo_ton"] = (
            intensity["cargo_weight_kg"] / 1000.0
        ) / intensity["trip_count"]
        intensity["est_ship_length_m"] = np.interp(
            intensity["avg_cargo_ton"].fillna(1000.0), tons_ref, lengths_ref
        )
        intensity["ship_length_km"] = (
            intensity["trip_count"] * intensity["est_ship_length_m"]
        ) / 1000.0

        intensity["section_id"] = pd.to_numeric(intensity["id"], errors="coerce")
        merged_sec = intensity.merge(
            sections[["Id", "RouteId"]],
            left_on="section_id",
            right_on="Id",
            how="inner",
        )
        route_int = (
            merged_sec.groupby("RouteId")
            .agg(
                max_trip_count=("trip_count", "max"),
                max_ship_length_km=("ship_length_km", "max"),
                max_cargo_weight_ton=("cargo_weight_kg", lambda x: x.max() / 1000.0),
                avg_trip_count=("trip_count", "mean"),
                avg_ship_length_m=("est_ship_length_m", "mean"),
            )
            .reset_index()
        )
        logger.info(
            "Mapped intensity and ship-length meters for %d routes", len(route_int)
        )

    df = route_cov.merge(
        route_int.rename(columns={"RouteId": "route_id"}), on="route_id", how="left"
    )
    df = df.merge(
        ov_berths.rename(columns={"RouteId": "route_id"}), on="route_id", how="left"
    )
    df = df.merge(
        kg_berths.rename(columns={"RouteId": "route_id"}), on="route_id", how="left"
    )

    df["overnight_coverage_pct"] = (
        df["overnight_compliant_km"] / df["total_length_km"] * 100.0
    ).round(1)
    df["kegel_coverage_pct"] = (
        df["kegel_compliant_km"] / df["total_length_km"] * 100.0
    ).round(1)

    df["max_trip_count"] = df["max_trip_count"].fillna(0).astype(int)
    df["max_ship_length_km"] = df["max_ship_length_km"].fillna(0.0).round(1)
    df["max_cargo_weight_ton"] = df["max_cargo_weight_ton"].fillna(0.0).round(1)
    df["avg_trip_count"] = df["avg_trip_count"].fillna(0.0).round(1)
    df["avg_ship_length_m"] = df["avg_ship_length_m"].fillna(0.0).round(1)

    df["num_overnight_berths"] = df["num_overnight_berths"].fillna(0).astype(int)
    df["overnight_capacity_ships"] = (
        df["overnight_capacity_ships"].fillna(0).astype(int)
    )
    df["has_cardropoff"] = df["has_cardropoff"].fillna(False).astype(bool)

    df["num_kegel_berths"] = df["num_kegel_berths"].fillna(0).astype(int)
    df["kegel_capacity_ships"] = df["kegel_capacity_ships"].fillna(0).astype(int)

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
    netwerk_stats: list[dict] = []
    for ntype in [
        "Hoofdtransportas",
        "Hoofdvaarweg",
        "Overige vaarweg",
        "Niet geclassificeerd / Zee",
    ]:
        grp = gdf_segments[gdf_segments["netwerk_type"] == ntype]
        if grp.empty:
            continue
        tot = grp["length_km"].sum()
        ov = grp[grp["compliant_30km_overnight"]]["length_km"].sum()
        kg = grp[grp["compliant_60km_kegel"]]["length_km"].sum()
        netwerk_stats.append(
            {
                "type": ntype,
                "tot_km": tot,
                "ov_km": ov,
                "ov_pct": (ov / tot * 100.0) if tot > 0 else 0.0,
                "kg_km": kg,
                "kg_pct": (kg / tot * 100.0) if tot > 0 else 0.0,
            }
        )

    hvwn_grp = gdf_segments[
        gdf_segments["netwerk_type"].isin(["Hoofdtransportas", "Hoofdvaarweg"])
    ]
    hvwn_tot = hvwn_grp["length_km"].sum()
    hvwn_ov = hvwn_grp[hvwn_grp["compliant_30km_overnight"]]["length_km"].sum()
    hvwn_kg = hvwn_grp[hvwn_grp["compliant_60km_kegel"]]["length_km"].sum()

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

    md = f"""# Ruimtelijke Analyse Ligplaatscapaciteit (Richtlijnen Vaarwegen 2020 & IMA 2021)

## 1. Samenvatting Netwerkdekking per Netwerkcategorie

Conform de **Richtlijnen Vaarwegen 2020 (§ 6.3 - 6.4)** en de **Achtergrondrapportage Vaarwegen IMA 2021 (§ 2.3)** gelden de volgende normen:
* **Overnachtingshavens**: maximaal **30 km** tussenruimte (elke locatie ligt binnen $\\le 15$ km van een ligplaats).
* **Kegelligplaatsen**: maximaal **60 km** tussenruimte (elke locatie ligt binnen $\\le 30$ km van een kegelligplaats).

| Netwerkcategorie | Totale lengte (km) | Overnachten $\\le 30$ km | Overnachten (%) | Kegel $\\le 60$ km | Kegel (%) |
|---|---|---|---|---|---|
| **Hoofdtransportassen (HTA)** | {netwerk_stats[0]["tot_km"]:,.1f} km | {netwerk_stats[0]["ov_km"]:,.1f} km | **{netwerk_stats[0]["ov_pct"]:.1f}%** | {netwerk_stats[0]["kg_km"]:,.1f} km | **{netwerk_stats[0]["kg_pct"]:.1f}%** |
| **Hoofdvaarwegen (HVW)** | {netwerk_stats[1]["tot_km"]:,.1f} km | {netwerk_stats[1]["ov_km"]:,.1f} km | **{netwerk_stats[1]["ov_pct"]:.1f}%** | {netwerk_stats[1]["kg_km"]:,.1f} km | **{netwerk_stats[1]["kg_pct"]:.1f}%** |
| **Totaal HVWN (HTA + HVW)** | **{hvwn_tot:,.1f} km** | **{hvwn_ov:,.1f} km** | **{(hvwn_ov / hvwn_tot * 100):.1f}%** | **{hvwn_kg:,.1f} km** | **{(hvwn_kg / hvwn_tot * 100):.1f}%** |
| Overige vaarwegen | {netwerk_stats[2]["tot_km"]:,.1f} km | {netwerk_stats[2]["ov_km"]:,.1f} km | {netwerk_stats[2]["ov_pct"]:.1f}% | {netwerk_stats[2]["kg_km"]:,.1f} km | {netwerk_stats[2]["kg_pct"]:.1f}% |
| Niet geclassificeerd (kust/zee) | {netwerk_stats[3]["tot_km"]:,.1f} km | {netwerk_stats[3]["ov_km"]:,.1f} km | {netwerk_stats[3]["ov_pct"]:.1f}% | {netwerk_stats[3]["kg_km"]:,.1f} km | {netwerk_stats[3]["kg_pct"]:.1f}% |

---

## 2. Ligplaatscapaciteit en Aanbod

* **Totaal aantal ligplaatsrecords in FIS**: {len(berths):,}
* **Aangemerkt als overnachtingsligplaats**: {berths["is_overnight"].sum():,} records
* **Totale overnachtingscapaciteit**: **{berths.loc[berths["is_overnight"], "capacity_ships"].sum():,} scheepsplaatsen** (CEMT Va $110\\text{{m}} + 10\\text{{m}}$ tussenruimte)
* **Kegelligplaatsen (blauw licht)**: {berths["is_kegel"].sum():,} records ({berths.loc[berths["is_kegel"], "capacity_ships"].sum():,} scheepsplaatsen)
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
    hvwn_gaps = (
        hvwn_grp[~hvwn_grp["compliant_30km_overnight"]]
        .groupby(["route_id", "route_name", "netwerk_type"])
        .agg(gap_km=("length_km", "sum"), max_dist=("dist_to_overnight_km", "max"))
        .reset_index()
        .sort_values("gap_km", ascending=False)
        .head(15)
    )
    md += "| Route ID | Routenaam | Netwerktype | Trajectlengte zonder dekking (km) | Max. afstand tot ligplaats (km) |\n|---|---|---|---|---|\n"
    for _, r in hvwn_gaps.iterrows():
        md += f"| {r['route_id']} | {r['route_name']} | {r['netwerk_type']} | {r['gap_km']:.1f} km | {r['max_dist']:.1f} km |\n"

    # Section 5: Comparison of Capacity vs Maximum Traffic Intensity per Route
    md += "\n---\n\n## 5. Vergelijking Ligplaatscapaciteit en Maximale Scheepvaartintensiteit per Route\n\n"
    md += (
        "Onderstaande tabel toont de drukste corridors en routes, gerangschikt op de **maximale scheepsintensiteit** "
        "(passages/jaar én totale **scheepslengte-kilometers/jaar** op het drukste baanvak van de route), gekoppeld aan de lokale ligplaatscapaciteit en dekking:\n\n"
    )
    md += "| Code | Routenaam | Categorie | Max. Scheepslengte (km/jr) | Max. Intensiteit (reizen/jr) | Max. Tonnage (ton/jr) | Dekking (%) | Capaciteit (schepen) |\n"
    md += "|---|---|---|---|---|---|---|---|\n"

    top_int = gdf_routes.sort_values("max_trip_count", ascending=False).head(20)
    for _, r in top_int.iterrows():
        trips_str = f"{r['max_trip_count']:,}" if r["max_trip_count"] > 0 else "-"
        len_str = (
            f"{r['max_ship_length_km']:,.1f} km" if r["max_ship_length_km"] > 0 else "-"
        )
        ton_str = (
            f"{r['max_cargo_weight_ton']:,.0f}"
            if r["max_cargo_weight_ton"] > 0
            else "-"
        )
        md += (
            f"| {r['route_code']} | {r['route_name']} | {r['netwerk_type']} | "
            f"{len_str} | {trips_str} | {ton_str} | {r['overnight_coverage_pct']:.1f}% | {r['overnight_capacity_ships']} |\n"
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
    "--intensity-path",
    type=click.Path(path_type=Path),
    default=Path("output/ivs-assignment/year=2025/intensity.geoparquet"),
    help="Path to IVS traffic intensity parquet file.",
)
@click.option(
    "--dtv-path",
    type=click.Path(path_type=Path),
    default=Path("reference/DTV_shiptypes_database.json"),
    help="Path to DTV shiptypes database JSON.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("output/berth-analysis"),
    help="Directory to save analysis outputs.",
)
def main(
    export_dir: Path, intensity_path: Path, dtv_path: Path, output_dir: Path
) -> None:
    """Runs spatial capacity and distance analysis for berths across the route network."""
    output_dir.mkdir(parents=True, exist_ok=True)

    routes, rjs, berths, cardropoffs, fc, sections = load_input_datasets(export_dir)

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

    # 4. Compute route-level dataset joining capacity and intensity (including ship-length meters)
    gdf_routes = compute_route_capacity_and_intensity(
        routes,
        sections,
        classified_berths,
        gdf_segments,
        intensity_path=intensity_path if intensity_path.exists() else None,
        dtv_path=dtv_path if dtv_path.exists() else None,
    )

    logger.info("Exporting route compliance and intensity outputs...")
    gdf_segments.to_parquet(output_dir / "route_segments_compliance.geoparquet")
    gdf_segments.to_file(
        output_dir / "route_segments_compliance.geojson", driver="GeoJSON"
    )

    gdf_routes.to_parquet(output_dir / "route_capacity_and_intensity.geoparquet")
    gdf_routes.drop(columns=["geometry"], errors="ignore").to_csv(
        output_dir / "route_capacity_and_intensity.csv", index=False
    )
    gdf_routes.to_file(
        output_dir / "route_capacity_and_intensity.geojson", driver="GeoJSON"
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
