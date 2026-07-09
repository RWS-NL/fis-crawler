"""Boven/beneden (upstream/downstream) determination and gate orientation for lock complexes.

See docs/werkwijze_sluiscontrole.md §3.4 for the definition this implements:
boven/beneden is a fixed, per-complex designation based on streefpeil (target
level, m NAP) and position in the water system — not an instantaneous water-level
comparison. The side connected (via the fairway graph) to the higher streefpeil is
"boven"; the lower is "beneden". Rivers and tidal reaches typically carry no
streefpeil at all, which is expected and handled explicitly (not treated as a bug).
"""

import logging
import geopandas as gpd
import pandas as pd
from fis.utils import load_lock_bridge_mappings

logger = logging.getLogger(__name__)

# Tolerance (m) below which two streefpeil values are considered equal/ambiguous.
STREEFPEIL_TOLERANCE_M = 0.05


def sjoin_nearest_value(
    points_gdf, source_gdf, value_cols, max_distance=500, id_col="id"
):
    """Nearest-join one or more value columns from ``source_gdf`` onto ``points_gdf``.

    Small reusable helper factoring out the ``sjoin_nearest(...).drop_duplicates(...)``
    idiom duplicated across the lock validation script (fairwaydepth.ReferenceLevel,
    aimedwaterlevel deviations, aimedlevel.Value-on-centroid fallback).
    """
    joined = gpd.sjoin_nearest(
        points_gdf[[id_col, "geometry"]],
        source_gdf[value_cols + ["geometry"]],
        how="left",
        max_distance=max_distance,
    ).drop_duplicates(subset=[id_col])
    return joined[[id_col] + value_cols]


def _normalize_id(val):
    """Best-effort normalize an id-like value (int/float/str) to a comparable form."""
    if val is None:
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return val


def _edge_route_id(edge_data):
    return _normalize_id(edge_data.get("route_id", edge_data.get("RouteId")))


def walk_to_streefpeil(
    graph, start_junction_id, max_hops=5, route_id=None, stop_nodes=None
):
    """Breadth-first search from a junction node for the nearest streefpeil_nap.

    When ``route_id`` is given, the walk stays on edges belonging to the lock's
    own FIS route and does not cross into side branches/harbours on a different
    route. Verified necessary empirically (Sluis Belfeld/Sambeek): an
    unconstrained walk from a lock's own junction can reach a nearby harbour or
    side canal with its own (unrelated) streefpeil within 1-2 hops, before ever
    reaching the correct pand boundary further along the lock's own route — the
    walk must NOT silently fall back to an unconstrained search in that case,
    since that fallback is exactly what produces the wrong value; if nothing is
    found on the lock's own route within ``max_hops``, the result is genuinely
    unresolved.

    ``stop_nodes`` (typically the start/end junctions of every OTHER lock
    complex on the same route) are treated as barriers: an edge leading into a
    stop node is still checked for streefpeil_nap, but the walk does not
    continue past it. Verified necessary empirically: without this, the walk
    can cross straight through a neighbouring lock's own pand and pick up ITS
    streefpeil instead of stopping at the calling lock's own boundary (e.g.
    Sluis Maasbracht silently resolving to Sluis Born's values, Sluis Sambeek
    to Sluis Belfeld's, both ~13-16 route-km away — well past any real pand).

    Returns (value, hops) of the first edge carrying a ``streefpeil_nap``
    attribute, or (None, None) if none is found within ``max_hops``.
    """
    if (
        graph is None
        or start_junction_id is None
        or not graph.has_node(start_junction_id)
    ):
        return None, None

    stop_nodes = stop_nodes or set()
    visited = {start_junction_id}
    frontier = [start_junction_id]
    for hop in range(max_hops):
        next_frontier = []
        for node in frontier:
            for nbr in graph.neighbors(node):
                edge_data = graph.get_edge_data(node, nbr) or {}
                if route_id is not None and _edge_route_id(edge_data) != route_id:
                    continue
                value = edge_data.get("streefpeil_nap")
                if value is not None:
                    return value, hop + 1
                if nbr in stop_nodes:
                    # Edge into another lock's own junction checked above; do not
                    # traverse past it into that lock's pand.
                    continue
                if nbr not in visited:
                    visited.add(nbr)
                    next_frontier.append(nbr)
        frontier = next_frontier
        if not frontier:
            break
    return None, None


def resolve_boven_beneden(
    fairway_data,
    graph,
    route_id=None,
    max_hops=5,
    other_lock_junctions=None,
    lock_name=None,
):
    """Determine boven/beneden for one lock complex using the fis-graph topology.

    ``fairway_data`` is the dict produced by ``fis.lock.core._resolve_fairway_data``,
    which already carries ``start_junction_id``/``end_junction_id`` — the two real
    graph junctions bordering the complex's own fairway. ``split_point`` corresponds
    to the start_junction side, ``merge_point`` to the end_junction side (see
    ``_resolve_fairway_data``: geometry_before spans start->lock, geometry_after
    spans lock->end).

    ``route_id`` (the lock's own RouteId, distinct from fairway_id — a single FIS
    route strings together many short fairway segments) constrains the graph walk
    to the lock's own route so it does not wander into a nearby harbour/side canal
    with an unrelated streefpeil (see ``walk_to_streefpeil``).

    ``other_lock_junctions`` (start/end junctions of every OTHER lock complex,
    normalized ints) bounds the walk so it stops at a neighbouring lock's own
    boundary instead of crossing through its pand — see ``walk_to_streefpeil``.

    ``lock_name``: if it matches MULTI_RIVER_JUNCTION_LOCKS (a confluence of two
    rivers, e.g. Weurt/Heumen — not a simple 2-sided boven/beneden case), the
    graph walk is skipped entirely and the result is marked
    source="multi_river_junction", so the manual table is authoritative for
    these locks in the actual schematization output, not just in the separate
    cross-validation report.

    Returns a dict with split_side/merge_side ("boven"/"beneden"/None),
    split_streefpeil_nap/merge_streefpeil_nap (float m NAP or None), and a
    ``source`` explaining how the result was reached.
    """
    empty = {
        "split_side": None,
        "merge_side": None,
        "split_streefpeil_nap": None,
        "merge_streefpeil_nap": None,
        "source": "no_graph",
    }
    if lock_name and any(
        key in lock_name.lower() for key in MULTI_RIVER_JUNCTION_LOCKS
    ):
        return {**empty, "source": "multi_river_junction"}
    if graph is None:
        return empty

    start_j = fairway_data.get("start_junction_id")
    end_j = fairway_data.get("end_junction_id")
    start_j = int(start_j) if start_j is not None else None
    end_j = int(end_j) if end_j is not None else None
    route_id = _normalize_id(route_id)
    stop_nodes = (other_lock_junctions or set()) - {start_j, end_j}

    val_start, _ = walk_to_streefpeil(
        graph, start_j, max_hops, route_id=route_id, stop_nodes=stop_nodes
    )
    val_end, _ = walk_to_streefpeil(
        graph, end_j, max_hops, route_id=route_id, stop_nodes=stop_nodes
    )

    if val_start is None and val_end is None:
        return {**empty, "source": "no_streefpeil_found"}

    if val_start is None or val_end is None:
        # Only one side carries a streefpeil (canal-to-river/tidal case, e.g. Weurt,
        # Eefde): the regulated (streefpeil) side is boven by definition; the other
        # side is beneden by definition, not by measured value.
        if val_start is not None:
            return {
                "split_side": "boven",
                "merge_side": "beneden",
                "split_streefpeil_nap": val_start,
                "merge_streefpeil_nap": None,
                "source": "single_side_aimedlevel",
            }
        return {
            "split_side": "beneden",
            "merge_side": "boven",
            "split_streefpeil_nap": None,
            "merge_streefpeil_nap": val_end,
            "source": "single_side_aimedlevel",
        }

    if abs(val_start - val_end) < STREEFPEIL_TOLERANCE_M:
        return {
            "split_side": None,
            "merge_side": None,
            "split_streefpeil_nap": val_start,
            "merge_streefpeil_nap": val_end,
            "source": "ambiguous",
        }

    if val_start > val_end:
        split_side, merge_side = "boven", "beneden"
    else:
        split_side, merge_side = "beneden", "boven"

    return {
        "split_side": split_side,
        "merge_side": merge_side,
        "split_streefpeil_nap": val_start,
        "merge_streefpeil_nap": val_end,
        "source": "resolved",
    }


# Load manual waterway levels from the config TOML file
_mappings = load_lock_bridge_mappings()
MANUAL_WATERWAY_LEVELS = {
    k: {
        "levels": (
            v["waterway_hoog"],
            v["peil_hoog"],
            v["waterway_laag"],
            v["peil_laag"],
        ),
        "isrs_codes": v.get("isrs_codes", []),
    }
    for k, v in _mappings["manual_waterway_levels"].items()
}

# Build a lookup to map chamber ISRS codes to their parent lock complex ISRS codes
_chambers = gpd.read_parquet("output/fis-export/chamber.geoparquet")
_locks = gpd.read_parquet("output/fis-export/lock.geoparquet")
_lock_isrs_map = _locks.set_index("Id")["Code"].to_dict()
CHAMBER_TO_COMPLEX_ISRS = {}
for _, _row in _chambers.iterrows():
    _c_code = _row.get("Code")
    _p_id = _row.get("ParentId") or _row.get("ParentLockId")
    if _c_code and _p_id:
        _p_isrs = _lock_isrs_map.get(_p_id)
        if _p_isrs:
            CHAMBER_TO_COMPLEX_ISRS[str(_c_code).strip()] = str(_p_isrs).strip()

# Locks that are structurally not a simple 2-sided boven/beneden case.
MULTI_RIVER_JUNCTION_LOCKS = {"weurt", "heumen"}


def get_waterway_levels(isrs_code):
    """Return (waterway_hoog, peil_hoog, waterway_laag, peil_laag) for a lock by its ISRS code.

    Strictly matches against MANUAL_WATERWAY_LEVELS using exact ISRS complex code matching.
    If a chamber ISRS code is passed, it is automatically resolved to its parent complex code.
    """
    target = str(isrs_code).strip()
    complex_isrs = CHAMBER_TO_COMPLEX_ISRS.get(target, target)

    for key, cfg in MANUAL_WATERWAY_LEVELS.items():
        if complex_isrs in cfg["isrs_codes"]:
            return cfg["levels"]

    raise KeyError(
        f"ISRS code '{target}' (resolved complex: '{complex_isrs}') not found in manual waterway levels configuration."
    )


def _determine_validation_category(
    key: str,
    source: str | None,
    auto_boven: float | None,
    auto_beneden: float | None,
    peil_hoog: float | None,
    peil_laag: float | None,
    tolerance: float,
) -> str:
    """Determine the validation category for a given automatic and manual level match.

    Categorizes comparison as MATCH, SIDE_MISMATCH, VALUE_MISMATCH, PARTIAL, or UNRESOLVED.
    """
    if key in MULTI_RIVER_JUNCTION_LOCKS or source in (
        None,
        "no_graph",
        "no_streefpeil_found",
        "ambiguous",
    ):
        return "UNRESOLVED"

    if source == "single_side_aimedlevel":
        if auto_boven is not None and peil_hoog is not None:
            return (
                "PARTIAL"
                if abs(auto_boven - peil_hoog) <= tolerance
                else "VALUE_MISMATCH"
            )
        if auto_beneden is not None and peil_laag is not None:
            return (
                "PARTIAL"
                if abs(auto_beneden - peil_laag) <= tolerance
                else "VALUE_MISMATCH"
            )
        return "PARTIAL"

    if auto_boven is None or auto_beneden is None:
        return "PARTIAL"

    if peil_hoog is None or peil_laag is None:
        return "UNRESOLVED"

    boven_ok = abs(auto_boven - peil_hoog) <= tolerance
    beneden_ok = abs(auto_beneden - peil_laag) <= tolerance
    if boven_ok and beneden_ok:
        return "MATCH"

    swapped_ok = (
        abs(auto_boven - peil_laag) <= tolerance
        and abs(auto_beneden - peil_hoog) <= tolerance
    )
    if swapped_ok:
        return "SIDE_MISMATCH"

    return "VALUE_MISMATCH"


def cross_validate_manual_levels(nodes_gdf, lock_gdf, tolerance=0.1):
    """Compare automatically resolved boven/beneden streefpeil against the manual table.

    This function helps validate whether the graph-based water level resolution matches
    our manually curated list of waterway streefpeilen.
    """
    rows = []
    lock_names = lock_gdf[["id", "name", "isrs_code"]].dropna(subset=["name"])

    # Map each lock to its matched key in MANUAL_WATERWAY_LEVELS using exact ISRS code matching
    lock_to_key = {}
    for _, lock_row in lock_names.iterrows():
        lock_isrs = lock_row.get("isrs_code")
        if lock_isrs:
            lock_isrs_str = str(lock_isrs).strip()
            for key, cfg in MANUAL_WATERWAY_LEVELS.items():
                if lock_isrs_str in cfg["isrs_codes"]:
                    lock_to_key[str(lock_row["id"])] = key
                    break

    for key, cfg in MANUAL_WATERWAY_LEVELS.items():
        wway_hoog, peil_hoog, wway_laag, peil_laag = cfg["levels"]
        matched_ids = [lk for lk, k in lock_to_key.items() if k == key]
        matches = lock_names[lock_names["id"].astype(str).isin(matched_ids)]

        if matches.empty:
            rows.append(
                {
                    "sluis_key": key,
                    "lock_id": None,
                    "lock_name": None,
                    "category": "NO_LOCK_MATCH",
                    "manual_peil_hoog": peil_hoog,
                    "manual_peil_laag": peil_laag,
                    "auto_boven_nap": None,
                    "auto_beneden_nap": None,
                    "source": None,
                }
            )
            continue

        for _, lock_row in matches.iterrows():
            lock_id = str(lock_row["id"])
            complex_nodes = nodes_gdf[
                (nodes_gdf["lock_id"].astype(str) == lock_id)
                & (nodes_gdf["node_type"].isin(["lock_split", "lock_merge"]))
            ]

            auto_boven_nap = None
            auto_beneden_nap = None
            source = None

            for _, node_row in complex_nodes.iterrows():
                side = node_row.get("side")
                nap = node_row.get("streefpeil_nap")
                source = node_row.get("streefpeil_source") or source
                if side == "boven":
                    auto_boven_nap = nap
                elif side == "beneden":
                    auto_beneden_nap = nap

            category = _determine_validation_category(
                key,
                source,
                auto_boven_nap,
                auto_beneden_nap,
                peil_hoog,
                peil_laag,
                tolerance,
            )

            rows.append(
                {
                    "sluis_key": key,
                    "lock_id": lock_id,
                    "lock_name": lock_row["name"],
                    "category": category,
                    "manual_peil_hoog": peil_hoog,
                    "manual_peil_laag": peil_laag,
                    "auto_boven_nap": auto_boven_nap,
                    "auto_beneden_nap": auto_beneden_nap,
                    "source": source,
                }
            )

    return pd.DataFrame(rows)
