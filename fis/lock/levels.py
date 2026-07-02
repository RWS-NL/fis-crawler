"""Boven/beneden (upstream/downstream) determination for lock complexes.

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


def _match_sections_to_aimedlevel(sections_gdf, aimedlevel_gdf):
    """Route/km overlap match: normalized ``sections_gdf`` (snake_case columns,
    e.g. from ``fis.lock.core.load_data``) against raw FIS ``aimedlevel_gdf``
    (CamelCase columns, straight from the export). Returns a dict of
    section id -> streefpeil Value (m NAP) for sections with an overlapping
    aimedlevel segment on the same RouteId.
    """
    required_section_cols = {"id", "route_id", "route_km_begin", "route_km_end"}
    required_aimed_cols = {"RouteId", "RouteKmBegin", "RouteKmEnd", "Value"}
    if not required_section_cols.issubset(sections_gdf.columns):
        logger.warning(
            "sections_gdf missing route/km columns; cannot match aimedlevel."
        )
        return {}
    if not required_aimed_cols.issubset(aimedlevel_gdf.columns):
        logger.warning("aimedlevel_gdf missing expected columns; cannot match.")
        return {}

    sections = sections_gdf.dropna(
        subset=["route_id", "route_km_begin", "route_km_end"]
    )
    aimed = aimedlevel_gdf.dropna(
        subset=["RouteId", "RouteKmBegin", "RouteKmEnd", "Value"]
    )
    aimed_by_route = aimed.groupby("RouteId")

    value_by_section = {}
    for section in sections.itertuples():
        route_id = section.route_id
        if route_id not in aimed_by_route.groups:
            continue
        s_begin = min(section.route_km_begin, section.route_km_end)
        s_end = max(section.route_km_begin, section.route_km_end)
        for aimed_row in aimed_by_route.get_group(route_id).itertuples():
            a_begin = min(aimed_row.RouteKmBegin, aimed_row.RouteKmEnd)
            a_end = max(aimed_row.RouteKmBegin, aimed_row.RouteKmEnd)
            if not (s_end < a_begin or a_end < s_begin):
                value_by_section[section.id] = aimed_row.Value
                break

    return value_by_section


def enrich_edges_with_streefpeil(graph, sections_gdf, aimedlevel_gdf):
    """Project ``aimedlevel`` (streefpeil, m NAP) onto fis-graph edges.

    Sets a ``streefpeil_nap`` attribute on every edge whose underlying FIS section
    overlaps (by RouteId/RouteKm range) an aimedlevel segment.

    Mutates ``graph`` in place and also returns it.
    """
    if graph is None or aimedlevel_gdf is None or aimedlevel_gdf.empty:
        return graph

    value_by_section = _match_sections_to_aimedlevel(sections_gdf, aimedlevel_gdf)
    if not value_by_section:
        logger.warning(
            "aimedlevel route-km matching produced no values; graph not enriched."
        )
        return graph

    section_lookup = sections_gdf.dropna(
        subset=["start_junction_id", "end_junction_id"]
    )[["id", "start_junction_id", "end_junction_id"]]
    edge_to_section = {}
    for row in section_lookup.itertuples():
        try:
            u, v = int(row.start_junction_id), int(row.end_junction_id)
        except (TypeError, ValueError):
            continue
        edge_to_section[(u, v)] = row.id
        edge_to_section[(v, u)] = row.id

    enriched = 0
    for u, v, data in graph.edges(data=True):
        section_id = edge_to_section.get((u, v))
        if section_id is None or section_id not in value_by_section:
            continue
        data["streefpeil_nap"] = value_by_section[section_id]
        enriched += 1

    logger.info(
        "Enriched %d / %d graph edges with streefpeil_nap",
        enriched,
        graph.number_of_edges(),
    )
    return graph


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


def walk_to_streefpeil(graph, start_junction_id, max_hops=5, route_id=None):
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

    Returns (value, hops) of the first edge carrying a ``streefpeil_nap``
    attribute, or (None, None) if none is found within ``max_hops``.
    """
    if (
        graph is None
        or start_junction_id is None
        or not graph.has_node(start_junction_id)
    ):
        return None, None

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
                if nbr not in visited:
                    visited.add(nbr)
                    next_frontier.append(nbr)
        frontier = next_frontier
        if not frontier:
            break
    return None, None


def resolve_boven_beneden(fairway_data, graph, route_id=None, max_hops=5):
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
    if graph is None:
        return empty

    start_j = fairway_data.get("start_junction_id")
    end_j = fairway_data.get("end_junction_id")
    # start_junction_id/end_junction_id are stringified elsewhere in the pipeline;
    # graph node ids are ints (junction ids as loaded from the FIS section export).
    start_j = int(start_j) if start_j is not None else None
    end_j = int(end_j) if end_j is not None else None
    route_id = _normalize_id(route_id)

    val_start, _ = walk_to_streefpeil(graph, start_j, max_hops, route_id=route_id)
    val_end, _ = walk_to_streefpeil(graph, end_j, max_hops, route_id=route_id)

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
# reaches, free-flowing rivers, or multi-river junctions like Weurt/Heumen — see
# docs/werkwijze_sluiscontrole.md §3.4/§5). Relocated from the former
# scripts/lock_validation/validate_lock_dimensions.py::get_waterway_levels()
# if/elif chain so there is a single source of truth.
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

# Locks that are structurally not a simple 2-sided boven/beneden case (e.g. a canal
# meeting the confluence of two rivers). Marked explicitly so the automatic method
# does not silently guess — see docs/werkwijze_sluiscontrole.md §3.4 "Weurt/Heumen".
MULTI_RIVER_JUNCTION_LOCKS = {"weurt", "heumen"}


def get_waterway_levels(sluis_name):
    """Return (waterway_hoog, peil_hoog, waterway_laag, peil_laag) for a lock name.

    Substring match against MANUAL_WATERWAY_LEVELS, case-insensitive. Kept for
    backward compatibility with callers that only know the lock's display name;
    ``resolve_boven_beneden`` should be preferred where the fis-graph is available.
    """
    s = sluis_name.lower().strip()
    for key, value in MANUAL_WATERWAY_LEVELS.items():
        if key in s:
            return value
    return "Onbekende waterweg", None, "Onbekende waterweg", None


def cross_validate_manual_levels(nodes_gdf, lock_gdf, tolerance=0.1):
    """Compare automatically resolved boven/beneden streefpeil against the manual table.

    For every lock name known to MANUAL_WATERWAY_LEVELS, finds the matching lock
    complex(es) in ``lock_gdf`` and compares the ``lock_split``/``lock_merge`` node
    attributes (from ``nodes_gdf``) against the hand-curated values. Returns a
    DataFrame with one row per matched complex, classified into:
      MATCH / SIDE_MISMATCH / VALUE_MISMATCH / PARTIAL / UNRESOLVED / NO_LOCK_MATCH
    """
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
