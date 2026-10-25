"""Spatial capacity and network distance analysis for berths (ligplaatsen).

Analyzes berth capacity (discretized for 85 m, 110 m, and 135 m vessel scenarios),
kegel (hazardous goods) berths, and car drop-off facilities against the spatial
spacing criteria from Richtlijnen Vaarwegen 2020 (RVW 2020 § 6.3-6.6) and the
Integrale Mobiliteitsanalyse (IMA 2021 Achtergrondrapportage Vaarwegen § 2.3).

Usage:
    uv run python -m scripts.berth_analysis.analyze_spatial_capacity
"""

from __future__ import annotations

import logging
from pathlib import Path

import click
import geopandas as gpd
import jinja2
import networkx as nx
import numpy as np
import pandas as pd

from fis.utils import load_schema, normalize_attributes, stringify_id
from scripts.berth_analysis.route_network_graph import build_route_network_graph

logger = logging.getLogger(__name__)

# RVW 2020 § 6.3 - 6.6 constants
SHIP_LENGTH_85_M = 85.0  # CEMT IV Johann Welker
SHIP_LENGTH_110_M = 110.0  # CEMT Va Groot Rijnschip
SHIP_LENGTH_135_M = 135.0  # CEMT Vb Verlengd Groot Rijnschip
CLEARANCE_SPACING_M = 10.0  # RVW 2020 p. 146 longitudinal spacing s
SHIP_SCENARIOS = (
    ("85m", SHIP_LENGTH_85_M),
    ("110m", SHIP_LENGTH_110_M),
    ("135m", SHIP_LENGTH_135_M),
)

# RVW 2020 § 6.3 p. 144 (~12 km/h network average) & Binnenvaartwet A1 (14h/day)
NORMAL_DAY_SAILING_HOURS = 14.0
AVERAGE_SAILING_SPEED_KMH = 12.0
DAILY_SAILING_DISTANCE_KM = (
    NORMAL_DAY_SAILING_HOURS * AVERAGE_SAILING_SPEED_KMH
)  # 168.0 km

OVERNIGHT_RADIUS_KM = 15.0  # Max 30 km spacing between overnight ports
KEGEL_RADIUS_KM = 30.0  # Max 60 km spacing between kegel berths
MAX_CLUSTER_SPAN_KM = 0.5  # Max cluster span along fairway (500 m)
CAR_DROPOFF_MIN_CAPACITY_SHIPS = 15  # RVW § 6.6 threshold

HVWN_TYPES = ("Hoofdtransportas", "Hoofdvaarweg")
NETWORK_CATEGORIES = {
    "Hoofdtransportas": "Hoofdtransportassen (HTA)",
    "Hoofdvaarweg": "Hoofdvaarwegen (HVW)",
    "Overige vaarweg": "Overige vaarwegen",
    "Overig": "Overig",
    "Niet geclassificeerd / Zee": "Niet geclassificeerd (kust/zee)",
}
ID_COLUMNS = ("id", "route_id", "berth_id", "section_junction_id")


def _read_normalized(
    export_dir: Path, name: str, section: str, schema: dict
) -> gpd.GeoDataFrame:
    """Reads a GeoParquet dataset, normalizes column names to snake_case, and standardizes IDs."""
    fpath = export_dir / f"{name}.geoparquet"
    if not fpath.exists():
        raise FileNotFoundError(f"Required input file missing: {fpath}")

    gdf = gpd.read_parquet(fpath).drop(columns=["Geometry"], errors="ignore")
    df = normalize_attributes(gdf, section, schema)
    for col in ID_COLUMNS:
        if col in df.columns:
            df[col] = df[col].map(stringify_id)
    return gpd.GeoDataFrame(df, geometry="geometry", crs=gdf.crs)


def load_input_datasets(
    export_dir: Path,
) -> tuple[
    gpd.GeoDataFrame,
    gpd.GeoDataFrame,
    gpd.GeoDataFrame,
    gpd.GeoDataFrame,
    gpd.GeoDataFrame,
]:
    """Loads and normalizes required FIS datasets from export_dir."""
    logger.info("Loading FIS datasets from %s", export_dir)
    schema = load_schema()
    return (
        _read_normalized(export_dir, "route", "routes", schema),
        _read_normalized(export_dir, "routejunction", "routejunctions", schema),
        _read_normalized(export_dir, "berth", "berths", schema),
        _read_normalized(export_dir, "cardropoff", "cardropoffs", schema),
        _read_normalized(
            export_dir, "fairwayclassification", "fairwayclassifications", schema
        ),
    )


def build_network_classification_map(fc: gpd.GeoDataFrame) -> dict[str, str]:
    """Maps each route_id to its highest fairway network classification."""
    type_map: dict[str, str] = {}
    for route_key, grp in fc.groupby("route_id"):
        types = set(grp["type_description"].dropna().unique())
        for candidate in ("Hoofdtransportas", "Hoofdvaarweg", "Overige vaarweg"):
            if candidate in types:
                type_map[route_key] = candidate
                break
        else:
            type_map[route_key] = "Overig"
    return type_map


def determine_kegels(
    signalling: str | None = None,
    name: str | None = None,
) -> int:
    """Determines the allowed number of cones (0, 1, or 2) from signalling and name."""
    sig = str(signalling or "")
    n = str(name or "").lower()
    if any(neg in n for neg in ("geen kegel", "0 kegel", "0-kegel")):
        return 0
    if "TWO_BLUE_LIGHTS" in sig or any(
        p in n for p in ("2 kegel", "2-kegel", "2kegel", "twee kegel")
    ):
        return 2
    if "ONE_BLUE_LIGHT" in sig or "kegel" in n:
        return 1
    return 0


def calc_berth_ship_slots(
    length: pd.Series,
    rows: pd.Series,
    max_ship_length: pd.Series,
    ship_length_m: float,
    spacing_m: float = CLEARANCE_SPACING_M,
) -> pd.Series:
    """Computes discrete ship slots for a reference ship length where allowed by maximal_ship_length."""
    allowed = max_ship_length.isna() | (max_ship_length >= ship_length_m)
    slots = np.floor((length + spacing_m) / (ship_length_m + spacing_m)) * rows
    ship_slots = pd.Series(
        np.where(allowed & (length > 0), slots, 0.0), index=length.index
    )
    return ship_slots


def classify_berths(
    berths: gpd.GeoDataFrame, cardropoffs: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Classifies berths into overnight, kegel (hazardous cargo), and car drop-off categories.

    Because the raw FIS attribute `OVERNIGHT_ACCOMODATION` in `facility` is only
    populated on a subset of overnight ports, a berth is classified as an
    overnight berth (`is_overnight`) if it is not excluded and meets at least
    one of four inclusion criteria:
    - `ov_by_facility`: `facility` explicitly contains `OVERNIGHT_ACCOMODATION`.
    - `ov_by_stay`: `maximum_stay_duration_h` explicitly permits stays >= 12 hours.
    - `ov_by_category`: `category` designates commercial inland vessels
      (`ALL_VESSELS`, `PUSH_TOW_VESSELS`, or `NON_PUSH_TOW_VESSELS`).
    - `ov_by_long_quay`: general quay (`category == "OTHER"`) of at least 100 m
      length with no maximum stay duration restriction.

    Hard exclusions (`not_excl`) disqualify:
    - Short-stay berths (`maximum_stay_duration_h < 12.0`).
    - Lock/bridge waiting areas (`category == "WAITING_AREA"`).
    - Transshipment/loading quays (`category == "LOADING_AND_UNLOADING"`).
    """
    df = berths.copy()

    # 1. Car drop-off availability: linked either via the separate cardropoffs
    # dataset (`berth_id`) or via the `CAR_DROPOFF` flag in `facility`.
    cardropoff_berth_ids = set(cardropoffs["berth_id"].dropna().unique())
    has_car_dropoff_fac = df["facility"].astype(str).str.contains("CAR_DROPOFF")
    df["has_cardropoff"] = df["id"].isin(cardropoff_berth_ids) | has_car_dropoff_fac

    # 2. Hard exclusions for overnight stay: short-stay (< 12h), waiting areas at
    # locks/bridges, and loading/unloading quays cannot serve as overnight ports.
    stay = df["maximum_stay_duration_h"]
    stay_ok = stay.isna() | (stay >= 12.0)
    not_excl = stay_ok & ~df["category"].isin(["WAITING_AREA", "LOADING_AND_UNLOADING"])

    # 3. Four inclusion criteria for overnight berths (combined with `not_excl`):
    df["ov_by_facility"] = (
        df["facility"].astype(str).str.contains("OVERNIGHT_ACCOMODATION") & not_excl
    )
    df["ov_by_stay"] = (stay >= 12.0) & not_excl
    df["ov_by_category"] = (
        df["category"].isin(["ALL_VESSELS", "PUSH_TOW_VESSELS", "NON_PUSH_TOW_VESSELS"])
        & not_excl
    )
    df["ov_by_long_quay"] = (
        (df["length"].fillna(0.0) >= 100.0)
        & (df["category"] == "OTHER")
        & stay.isna()
        & not_excl
    )
    df["is_overnight"] = (
        df["ov_by_facility"]
        | df["ov_by_stay"]
        | df["ov_by_category"]
        | df["ov_by_long_quay"]
    )

    # 4. Kegel (ADN hazardous cargo) classification from blue-light signalling or berth name.
    df["kegel_count"] = [
        determine_kegels(sig, nm)
        for sig, nm in zip(
            df.get("signalling", pd.Series(index=df.index)),
            df.get("name", pd.Series(index=df.index)),
        )
    ]
    df["is_kegel"] = df["kegel_count"] > 0
    return df


def calculate_berth_capacities(berths: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Computes effective quay length and ship capacities for 85 m, 110 m, and 135 m vessel scenarios."""
    df = berths.copy()
    rows = pd.to_numeric(df.get("number_of_rows"), errors="coerce")
    rows = rows.where(rows > 0, 1.0).fillna(1.0)

    length = pd.to_numeric(df.get("length"), errors="coerce")
    length = length.where(length > 0, 0.0).fillna(0.0)

    max_ship_len = pd.to_numeric(
        df.get("maximal_ship_length", pd.Series(np.nan, index=df.index)),
        errors="coerce",
    )

    df["effective_length_m"] = length * rows
    for suffix, ship_length_m in SHIP_SCENARIOS:
        df[f"capacity_ships_{suffix}"] = calc_berth_ship_slots(
            length, rows, max_ship_len, ship_length_m
        )

    logger.info(
        "Classified %d berths: %d overnight (%.0fm, %.0f ships @85m, %.0f ships @110m, %.0f ships @135m), %d kegel (%.0fm)",
        len(df),
        df["is_overnight"].sum(),
        df.loc[df["is_overnight"], "effective_length_m"].sum(),
        df.loc[df["is_overnight"], "capacity_ships_85m"].sum(),
        df.loc[df["is_overnight"], "capacity_ships_110m"].sum(),
        df.loc[df["is_overnight"], "capacity_ships_135m"].sum(),
        df["is_kegel"].sum(),
        df.loc[df["is_kegel"], "effective_length_m"].sum(),
    )
    return df


def classify_and_calculate_capacity(
    berths: gpd.GeoDataFrame, cardropoffs: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Classifies berths and computes effective length and 110m/135m ship capacities."""
    classified = classify_berths(berths, cardropoffs)
    berths_with_capacity = calculate_berth_capacities(classified)
    return berths_with_capacity


def calc_segment_furthest_distance(d_u: float, d_v: float, L: float) -> float:
    """Returns the furthest distance to the nearest berth for any point on segment [u, v]."""
    furthest_distance = min((d_u + d_v + L) / 2.0, d_u + L, d_v + L)
    return furthest_distance


def calc_segment_compliant_length(d_u: float, d_v: float, L: float, r: float) -> float:
    """Returns the length (km) of segment [u, v] within distance r of a berth."""
    compliant_length = min(L, max(0.0, r - d_u) + max(0.0, r - d_v))
    return compliant_length


def run_spatial_distance_analysis(
    G: nx.Graph,
    route_segments: list[dict],
) -> gpd.GeoDataFrame:
    """Runs multi-source Dijkstra for overnight and kegel berths across all route segments."""
    berth_kinds = {
        "overnight": (OVERNIGHT_RADIUS_KM, "30km"),
        "kegel": (KEGEL_RADIUS_KM, "60km"),
    }
    distances_by_kind: dict[str, dict[str, float]] = {}

    for kind in berth_kinds:
        sources = [n for n, d in G.nodes(data=True) if d.get(f"is_{kind}", False)]
        if kind == "overnight" and not sources:
            raise ValueError(
                "No overnight berths identified in network graph! Check classification logic."
            )
        distances_by_kind[kind] = (
            nx.multi_source_dijkstra_path_length(G, sources=sources, weight="weight")
            if sources
            else {}
        )

    records: list[dict] = []
    for seg in route_segments:
        u, v, seg_len = seg["source"], seg["target"], seg["length_km"]
        rec = {**seg, "is_hvwn": seg.get("network_type") in HVWN_TYPES}

        for kind, (radius_km, norm_label) in berth_kinds.items():
            dists = distances_by_kind[kind]
            d_u = dists.get(u, float("inf"))
            d_v = dists.get(v, float("inf"))
            max_dist = calc_segment_furthest_distance(d_u, d_v, seg_len)
            comp_len = calc_segment_compliant_length(d_u, d_v, seg_len, radius_km)
            reachable = not np.isinf(max_dist)

            rec[f"is_reachable_{kind}"] = reachable
            rec[f"dist_to_{kind}_km"] = max_dist if reachable else np.nan
            rec[f"compliant_{norm_label}_{kind}_km"] = comp_len
            rec[f"is_fully_compliant_{kind}"] = reachable and max_dist <= radius_km

        records.append(rec)

    return gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")


def _aggregate_cluster(rows: list[pd.Series]) -> dict:
    """Aggregates a list of nearby berth rows into a single cluster record."""
    df = pd.DataFrame(rows)
    return {
        "route_id": df.iloc[0]["route_id"],
        "route_km_begin": float(df["route_km"].min()),
        "route_km_end": float(df["route_km"].max()),
        "cluster_name": ", ".join(df["name"].dropna().astype(str).unique()[:3]),
        "num_berth_records": len(df),
        "total_capacity_ships_85m": float(df["capacity_ships_85m"].sum()),
        "total_capacity_ships_110m": float(df["capacity_ships_110m"].sum()),
        "total_capacity_ships_135m": float(df["capacity_ships_135m"].sum()),
        "total_length_m": float(df["effective_length_m"].sum()),
        "has_cardropoff": bool(df["has_cardropoff"].any()),
        "geometry": df.iloc[0]["geometry"],
    }


def cluster_berths_and_check_facilities(
    berths: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Clusters nearby overnight berths by route and evaluates RVW § 6.6 car drop-off compliance."""
    overnight = berths[berths["is_overnight"]].copy()

    clusters: list[dict] = []
    for _, group in overnight.groupby("route_id"):
        group = group.sort_values("route_km")
        curr_cluster: list[pd.Series] = []
        cluster_start_km: float | None = None

        for _, row in group.iterrows():
            km = float(row["route_km"])
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

    if not clusters:
        return gpd.GeoDataFrame(crs="EPSG:4326")

    cluster_df = pd.DataFrame(clusters)
    for suffix, _ in SHIP_SCENARIOS:
        req_col = f"req_cardropoff_{suffix}"
        comp_col = f"cardropoff_compliant_{suffix}"
        cluster_df[req_col] = (
            cluster_df[f"total_capacity_ships_{suffix}"]
            >= CAR_DROPOFF_MIN_CAPACITY_SHIPS
        )
        cluster_df[comp_col] = ~cluster_df[req_col] | cluster_df["has_cardropoff"]

    return gpd.GeoDataFrame(cluster_df, geometry="geometry", crs="EPSG:4326")


def _aggregate_berths_by_route(berths: gpd.GeoDataFrame, kind: str) -> pd.DataFrame:
    """Aggregates berth counts, lengths, and 85m/110m/135m ship capacities per route for a berth kind."""
    sub = berths[berths[f"is_{kind}"]]
    agg_spec: dict[str, tuple[str, str]] = {
        f"num_{kind}_berths": ("id", "count"),
        f"{kind}_berth_length_m": ("effective_length_m", "sum"),
        **{
            f"{kind}_capacity_ships_{suffix}": (f"capacity_ships_{suffix}", "sum")
            for suffix, _ in SHIP_SCENARIOS
        },
    }
    if kind == "overnight":
        agg_spec["has_cardropoff"] = ("has_cardropoff", "any")
    return sub.groupby("route_id").agg(**agg_spec).reset_index()


def _aggregate_catchment_by_route(
    G: nx.Graph,
    gdf_segments: gpd.GeoDataFrame,
    berths: gpd.GeoDataFrame,
    kind: str,
    radius_km: float,
) -> pd.DataFrame:
    """Aggregates reachable berth capacity within network distance radius_km of each route."""
    node_routes: dict[str, set[str]] = {}
    for s, t, rid in zip(
        gdf_segments["source"], gdf_segments["target"], gdf_segments["route_id"]
    ):
        node_routes.setdefault(s, set()).add(rid)
        node_routes.setdefault(t, set()).add(rid)

    sub = berths[berths[f"is_{kind}"]].set_index("id")
    route_to_berths: dict[str, set[str]] = {}
    for bid in sub.index:
        b_node = f"berth_{bid}"
        if b_node not in G:
            continue
        dists = nx.single_source_dijkstra_path_length(
            G, b_node, cutoff=radius_km, weight="weight"
        )
        for node in dists:
            for rid in node_routes.get(node, ()):
                route_to_berths.setdefault(rid, set()).add(bid)

    rows = []
    for rid, bids in route_to_berths.items():
        b_slice = sub.loc[list(bids)]
        rows.append(
            {
                "route_id": rid,
                f"catchment_{kind}_berths": len(b_slice),
                f"catchment_{kind}_berth_length_m": float(
                    b_slice["effective_length_m"].sum()
                ),
                **{
                    f"catchment_{kind}_capacity_ships_{suffix}": float(
                        b_slice[f"capacity_ships_{suffix}"].sum()
                    )
                    for suffix, _ in SHIP_SCENARIOS
                },
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "route_id",
                f"catchment_{kind}_berths",
                f"catchment_{kind}_berth_length_m",
                *(
                    f"catchment_{kind}_capacity_ships_{suffix}"
                    for suffix, _ in SHIP_SCENARIOS
                ),
            ]
        )
    return pd.DataFrame(rows)


def compute_route_carrying_capacity(
    routes: gpd.GeoDataFrame,
    berths: gpd.GeoDataFrame,
    gdf_segments: gpd.GeoDataFrame,
    G: nx.Graph | None = None,
) -> gpd.GeoDataFrame:
    """Computes route-level compliance, local and catchment ship capacities, and annual carrying capacity."""
    logger.info("Computing route-level capacity and maximum carrying capacity...")

    df = (
        gdf_segments.groupby("route_id")
        .agg(
            total_length_km=("length_km", "sum"),
            overnight_compliant_km=("compliant_30km_overnight_km", "sum"),
            kegel_compliant_km=("compliant_60km_kegel_km", "sum"),
            network_type=("network_type", "first"),
            route_name=("route_name", "first"),
            route_code=("route_code", "first"),
        )
        .reset_index()
    )

    df["expected_overnight_stops"] = np.where(
        df["total_length_km"] > 0,
        df["total_length_km"] / DAILY_SAILING_DISTANCE_KM,
        0.0,
    )

    kind_radii = {
        "overnight": OVERNIGHT_RADIUS_KM,
        "kegel": KEGEL_RADIUS_KM,
    }
    for kind, radius_km in kind_radii.items():
        df = df.merge(
            _aggregate_berths_by_route(berths, kind), on="route_id", how="left"
        )
        df[f"num_{kind}_berths"] = df[f"num_{kind}_berths"].fillna(0).astype(int)
        for col in (
            f"{kind}_berth_length_m",
            *(f"{kind}_capacity_ships_{suffix}" for suffix, _ in SHIP_SCENARIOS),
        ):
            df[col] = df[col].fillna(0.0)

        df[f"{kind}_coverage_pct"] = np.where(
            df["total_length_km"] > 0,
            df[f"{kind}_compliant_km"] / df["total_length_km"] * 100.0,
            0.0,
        )
        df[f"{kind}_meter_nights_yr"] = df[f"{kind}_berth_length_m"] * 365.0
        df[f"max_{kind}_ship_length_km_yr"] = np.where(
            df["expected_overnight_stops"] > 0,
            (df[f"{kind}_berth_length_m"] / 1000.0)
            / df["expected_overnight_stops"]
            * 365.0,
            0.0,
        )
        for suffix, _ in SHIP_SCENARIOS:
            df[f"max_{kind}_passages_{suffix}_yr"] = np.where(
                df["expected_overnight_stops"] > 0,
                df[f"{kind}_capacity_ships_{suffix}"]
                * 365.0
                / df["expected_overnight_stops"],
                0.0,
            )

        if G is not None and "source" in gdf_segments.columns:
            catch_df = _aggregate_catchment_by_route(
                G, gdf_segments, berths, kind, radius_km
            )
            df = df.merge(catch_df, on="route_id", how="left")
        else:
            df[f"catchment_{kind}_berths"] = df[f"num_{kind}_berths"]
            df[f"catchment_{kind}_berth_length_m"] = df[f"{kind}_berth_length_m"]
            for suffix, _ in SHIP_SCENARIOS:
                df[f"catchment_{kind}_capacity_ships_{suffix}"] = df[
                    f"{kind}_capacity_ships_{suffix}"
                ]

        df[f"catchment_{kind}_berths"] = (
            df[f"catchment_{kind}_berths"].fillna(0).astype(int)
        )
        for col in (
            f"catchment_{kind}_berth_length_m",
            *(
                f"catchment_{kind}_capacity_ships_{suffix}"
                for suffix, _ in SHIP_SCENARIOS
            ),
        ):
            df[col] = df[col].fillna(0.0)

        df[f"catchment_max_{kind}_ship_length_km_yr"] = np.where(
            df["expected_overnight_stops"] > 0,
            (df[f"catchment_{kind}_berth_length_m"] / 1000.0)
            / df["expected_overnight_stops"]
            * 365.0,
            0.0,
        )

    df["has_cardropoff"] = df["has_cardropoff"].fillna(False).astype(bool)
    df["is_hvwn"] = df["network_type"].isin(HVWN_TYPES)
    routes_geom = routes.set_index("id")["geometry"]
    df["geometry"] = df["route_id"].map(routes_geom)

    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")


def fmt_num(
    val: float | int | None,
    decimals: int = 0,
    unit: str = "",
    zero_dash: bool = False,
) -> str:
    """Formats a number with given decimals and unit, returning '-' for missing (or <= 0 if zero_dash)."""
    if val is None or pd.isna(val) or np.isinf(val):
        return "-"
    fval = float(val)
    if zero_dash and fval <= 0:
        return "-"
    return f"{fval:,.{decimals}f}{unit}"


def fmt_pct(val: float | None) -> str:
    """Formats a percentage with 1 decimal place, returning '-' when missing."""
    if val is None or pd.isna(val) or np.isinf(val):
        return "-"
    return f"{float(val):.1f}%"


def fmt_dist(val: float | None) -> str:
    """Formats a distance in km, or returns a fallback label when unreachable."""
    if val is None or pd.isna(val) or np.isinf(val):
        return "Geen ligplaats bereikbaar"
    return f"{float(val):.1f} km"


def summarize_segments(
    segments: gpd.GeoDataFrame, label: str, bold: bool = False
) -> dict:
    """Summarizes total and compliant lengths/percentages for a subset of route segments."""
    tot = float(segments["length_km"].sum())
    ov = float(segments["compliant_30km_overnight_km"].sum())
    kg = float(segments["compliant_60km_kegel_km"].sum())
    return {
        "label": label,
        "bold": bold,
        "tot_km": tot,
        "ov_km": ov,
        "ov_pct": (ov / tot * 100.0) if tot > 0 else 0.0,
        "kg_km": kg,
        "kg_pct": (kg / tot * 100.0) if tot > 0 else 0.0,
    }


def calculate_report_metrics(
    gdf_segments: gpd.GeoDataFrame,
    gdf_clusters: gpd.GeoDataFrame,
    gdf_routes: gpd.GeoDataFrame,
    berths: gpd.GeoDataFrame,
) -> dict:
    """Computes summary metrics and table rows for the spatial report."""
    network_rows = [
        summarize_segments(
            gdf_segments[gdf_segments["network_type"] == "Hoofdtransportas"],
            NETWORK_CATEGORIES["Hoofdtransportas"],
        ),
        summarize_segments(
            gdf_segments[gdf_segments["network_type"] == "Hoofdvaarweg"],
            NETWORK_CATEGORIES["Hoofdvaarweg"],
        ),
        summarize_segments(
            gdf_segments[gdf_segments["is_hvwn"]],
            "Totaal HVWN (HTA + HVW)",
            bold=True,
        ),
    ]
    for cat in ("Overige vaarweg", "Overig", "Niet geclassificeerd / Zee"):
        sub = gdf_segments[gdf_segments["network_type"] == cat]
        if not sub.empty or cat != "Overig":
            network_rows.append(summarize_segments(sub, NETWORK_CATEGORIES[cat]))

    cluster_stats: dict[str, int] = {}
    for suffix, _ in SHIP_SCENARIOS:
        large = (
            gdf_clusters[gdf_clusters[f"req_cardropoff_{suffix}"]]
            if not gdf_clusters.empty
            else pd.DataFrame()
        )
        missing = (
            large[~large[f"cardropoff_compliant_{suffix}"]]
            if not large.empty
            else pd.DataFrame()
        )
        cluster_stats[f"large_clusters_{suffix}_count"] = len(large)
        cluster_stats[f"cardropoff_ok_{suffix}_count"] = len(large) - len(missing)
        cluster_stats[f"missing_cardrop_{suffix}_count"] = len(missing)

    missing_cardrop_df = (
        gdf_clusters[
            gdf_clusters["req_cardropoff_85m"]
            & ~gdf_clusters["cardropoff_compliant_85m"]
        ]
        if not gdf_clusters.empty
        else pd.DataFrame()
    )

    hvwn_seg = gdf_segments[gdf_segments["is_hvwn"]]
    hvwn_gaps_df = (
        hvwn_seg[~hvwn_seg["is_fully_compliant_overnight"]]
        .groupby(["route_id", "route_name", "network_type"])
        .agg(gap_km=("length_km", "sum"), max_dist=("dist_to_overnight_km", "max"))
        .reset_index()
        .sort_values("gap_km", ascending=False)
        .head(15)
    )

    top_cap_df = (
        gdf_routes[gdf_routes["is_hvwn"]]
        .sort_values(
            ["max_overnight_ship_length_km_yr", "catchment_overnight_berth_length_m"],
            ascending=False,
        )
        .head(20)
    )
    zero_local_hvwn_df = (
        gdf_routes[gdf_routes["is_hvwn"] & (gdf_routes["num_overnight_berths"] == 0)]
        .sort_values("catchment_overnight_berth_length_m", ascending=False)
        .head(15)
    )

    ov_mask = berths["is_overnight"]
    kg_mask = berths["is_kegel"]
    ov_len_m = float(berths.loc[ov_mask, "effective_length_m"].sum())

    return {
        "network_rows": network_rows,
        "total_berths": len(berths),
        "overnight_berths_count": int(ov_mask.sum()),
        "ov_explicit_count": int(berths["ov_by_facility"].sum()),
        "ov_stay_12h_count": int(berths["ov_by_stay"].sum()),
        "ov_commercial_count": int(berths["ov_by_category"].sum()),
        "ov_long_quay_count": int(berths["ov_by_long_quay"].sum()),
        "overnight_kadelengte_m": ov_len_m,
        "overnight_meter_nachten": ov_len_m * 365.0,
        "overnight_ships_85m": float(berths.loc[ov_mask, "capacity_ships_85m"].sum()),
        "overnight_ships_110m": float(berths.loc[ov_mask, "capacity_ships_110m"].sum()),
        "overnight_ships_135m": float(berths.loc[ov_mask, "capacity_ships_135m"].sum()),
        "kegel_berths_count": int(kg_mask.sum()),
        "kegel_kadelengte_m": float(berths.loc[kg_mask, "effective_length_m"].sum()),
        "kegel_ships_85m": float(berths.loc[kg_mask, "capacity_ships_85m"].sum()),
        "kegel_ships_110m": float(berths.loc[kg_mask, "capacity_ships_110m"].sum()),
        "kegel_ships_135m": float(berths.loc[kg_mask, "capacity_ships_135m"].sum()),
        "total_clusters": len(gdf_clusters),
        **cluster_stats,
        "missing_cardrop": missing_cardrop_df.to_dict(orient="records"),
        "hvwn_gaps": hvwn_gaps_df.to_dict(orient="records"),
        "top_cap": top_cap_df.drop(columns=["geometry"], errors="ignore").to_dict(
            orient="records"
        ),
        "zero_local_hvwn": zero_local_hvwn_df.drop(
            columns=["geometry"], errors="ignore"
        ).to_dict(orient="records"),
        "hvwn_total_count": int(gdf_routes["is_hvwn"].sum()),
        "hvwn_zero_local_count": int(
            (gdf_routes["is_hvwn"] & (gdf_routes["num_overnight_berths"] == 0)).sum()
        ),
        "hvwn_zero_catchment_count": int(
            (
                gdf_routes["is_hvwn"] & (gdf_routes["catchment_overnight_berths"] == 0)
            ).sum()
        ),
    }


def generate_markdown_summary(
    gdf_segments: gpd.GeoDataFrame,
    gdf_clusters: gpd.GeoDataFrame,
    gdf_routes: gpd.GeoDataFrame,
    berths: gpd.GeoDataFrame,
    output_path: Path,
) -> None:
    """Renders the spatial berth analysis markdown report using Jinja2."""
    template_dir = Path(__file__).parent / "templates"
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(template_dir),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["fmt_num"] = fmt_num
    env.filters["fmt_pct"] = fmt_pct
    env.filters["fmt_dist"] = fmt_dist

    template = env.get_template("berth_spatial_report.md.j2")
    context = calculate_report_metrics(gdf_segments, gdf_clusters, gdf_routes, berths)
    output_path.write_text(template.render(**context), encoding="utf-8")
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
        routes, rjs, classified_berths, type_map, expected_parallel_edges=1
    )
    gdf_segments = run_spatial_distance_analysis(G, route_segments)
    gdf_routes = compute_route_carrying_capacity(
        routes, classified_berths, gdf_segments, G=G
    )

    logger.info("Exporting route compliance and carrying capacity outputs...")
    gdf_hvwn = gdf_routes[gdf_routes["is_hvwn"]].copy()
    gdf_hvwn.to_parquet(output_dir / "route_carrying_capacity.geoparquet")
    gdf_hvwn.drop(columns=["geometry"], errors="ignore").to_csv(
        output_dir / "route_carrying_capacity.csv", index=False
    )
    gdf_hvwn.to_file(output_dir / "route_carrying_capacity.geojson", driver="GeoJSON")
    gdf_routes.to_parquet(output_dir / "route_carrying_capacity_all.geoparquet")

    gdf_segments_hvwn = gdf_segments[gdf_segments["is_hvwn"]].copy()
    gdf_segments_hvwn.to_parquet(output_dir / "route_segments_compliance.geoparquet")
    gdf_segments_hvwn.to_file(
        output_dir / "route_segments_compliance.geojson", driver="GeoJSON"
    )
    gdf_segments.to_parquet(output_dir / "route_segments_compliance_all.geoparquet")

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
