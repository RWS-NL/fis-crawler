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


# Manually curated waterway names + streefpeil (m NAP) per side, for locks where
# the automatic method is structurally unable to resolve one or both sides (tidal
# reaches, free-flowing rivers, or multi-river junctions like Weurt/Heumen).
MANUAL_WATERWAY_LEVELS = {
    "belfeld": ("Maas (bovenstrooms)", 14.1, "Maas (benedenstrooms)", 10.8),
    "born": (
        "Julianakanaal (bovenstrooms)",
        44.7,
        "Julianakanaal (benedenstrooms)",
        32.6,
    ),
    "eefde": ("Twentekanaal", 10.0, "Gelderse IJssel", 3.0),
    "gaarkeuken": (
        "Van Starkenborghkanaal (oost)",
        -0.93,
        "Prinses Margrietkanaal (west)",
        -0.52,
    ),
    "hansweert": ("Kanaal door Zuid-Beveland", 0.0, "Westerschelde", 0.0),
    "heel": (
        "Julianakanaal / Kanaal Wessem-Nederweert",
        28.65,
        "Maasplassen Heel (stuwpeil Linne)",
        20.8,
    ),
    "wood": (
        "IJsselmeer",
        0.0,
        "Markermeer",
        -0.2,
    ),  # Note: matches houtrib via 'wood' sub-key in get_waterway_levels? Or houtrib? Let's check original.
    "houtrib": ("IJsselmeer", 0.0, "Markermeer", -0.2),
    "krammer": ("Volkerakpeil", 0.0, "Krammer / Oosterschelde", 0.0),
    "kreekrak": (
        "Antwerpen kanaalpeil",
        1.8,
        "Schelde-Rijnverbinding (Volkerakpeil)",
        0.0,
    ),
    "maasbracht": (
        "Julianakanaal (bovenstrooms)",
        32.6,
        "Julianakanaal (benedenstrooms)",
        20.8,
    ),
    "oranje": ("Markermeer", -0.2, "Binnen-IJ / Noordzeekanaal", -0.4),
    "bernhard": ("Waal (stuwpeil Hagestein/rivier)", 3.0, "Amsterdam-Rijnkanaal", -0.4),
    "beatrix": (
        "Lek (stuwpeil Hagestein)",
        3.0,
        "Lekkanaal / Amsterdam-Rijnkanaal",
        -0.4,
    ),
    "irene": ("Lek (stuwpeil Hagestein)", 3.0, "Amsterdam-Rijnkanaal", -0.4),
    "margriet": ("IJsselmeer", -0.1, "Friese Boezem", -0.52),
    "sambeek": ("Maas (bovenstrooms)", 10.8, "Maas (benedenstrooms)", 8.6),
    "weurt": ("Maas-Waalkanaal", 7.95, "Waal (rivier)", 5.0),
    "stevin": ("IJsselmeer", -0.1, "Waddenzee (tij)", 0.0),
    "terneuzen": ("Kanaal Gent-Terneuzen", 2.1, "Westerschelde (tij)", 0.0),
    "volkerak": ("Hollandsch Diep", 0.0, "Volkerak (Volkerakpeil)", 0.0),
}

# Locks that are structurally not a simple 2-sided boven/beneden case.
MULTI_RIVER_JUNCTION_LOCKS = {"weurt", "heumen"}


def get_waterway_levels(sluis_name):
    """Return (waterway_hoog, peil_hoog, waterway_laag, peil_laag) for a lock name.

    Substring match against MANUAL_WATERWAY_LEVELS, case-insensitive.
    """
    s = sluis_name.lower().strip()
    for key, value in MANUAL_WATERWAY_LEVELS.items():
        if key in s:
            return value
    return "Onbekende waterweg", None, "Onbekende waterweg", None


def cross_validate_manual_levels(nodes_gdf, lock_gdf, tolerance=0.1):
    """Compare automatically resolved boven/beneden streefpeil against the manual table."""
    rows = []
    lock_names = lock_gdf[["id", "name"]].dropna(subset=["name"])

    for key, (
        wway_hoog,
        peil_hoog,
        wway_laag,
        peil_laag,
    ) in MANUAL_WATERWAY_LEVELS.items():
        matches = lock_names[lock_names["name"].str.lower().str.contains(key)]
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
            split = complex_nodes[complex_nodes["node_type"] == "lock_split"]
            merge = complex_nodes[complex_nodes["node_type"] == "lock_merge"]

            auto_boven_nap = None
            auto_beneden_nap = None
            source = None
            for side_df in (split, merge):
                if side_df.empty:
                    continue
                side = side_df.iloc[0].get("side")
                nap = side_df.iloc[0].get("streefpeil_nap")
                source = side_df.iloc[0].get("streefpeil_source") or source
                if side == "boven":
                    auto_boven_nap = nap
                elif side == "beneden":
                    auto_beneden_nap = nap

            if key in MULTI_RIVER_JUNCTION_LOCKS or source == "no_graph":
                category = "UNRESOLVED"
            elif source in (None, "no_streefpeil_found", "ambiguous"):
                category = "UNRESOLVED"
            elif source == "single_side_aimedlevel":
                if auto_boven_nap is not None and peil_hoog is not None:
                    category = (
                        "PARTIAL"
                        if abs(auto_boven_nap - peil_hoog) <= tolerance
                        else "VALUE_MISMATCH"
                    )
                elif auto_beneden_nap is not None and peil_laag is not None:
                    category = (
                        "PARTIAL"
                        if abs(auto_beneden_nap - peil_laag) <= tolerance
                        else "VALUE_MISMATCH"
                    )
                else:
                    category = "PARTIAL"
            elif auto_boven_nap is None or auto_beneden_nap is None:
                category = "PARTIAL"
            elif peil_hoog is None or peil_laag is None:
                category = "UNRESOLVED"
            else:
                boven_ok = abs(auto_boven_nap - peil_hoog) <= tolerance
                beneden_ok = abs(auto_beneden_nap - peil_laag) <= tolerance
                swapped_ok = (
                    abs(auto_boven_nap - peil_laag) <= tolerance
                    and abs(auto_beneden_nap - peil_hoog) <= tolerance
                )
                if boven_ok and beneden_ok:
                    category = "MATCH"
                elif swapped_ok:
                    category = "SIDE_MISMATCH"
                else:
                    category = "VALUE_MISMATCH"

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
