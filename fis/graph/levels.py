"""Network-wide level enrichment for the waterway graph."""

import logging

logger = logging.getLogger(__name__)


def _match_sections_to_aimedlevel(sections_gdf, aimedlevel_gdf):
    """Route/km overlap match: normalized ``sections_gdf`` (snake_case columns)
    against raw FIS ``aimedlevel_gdf`` (CamelCase columns, straight from export).
    Returns a dict of section id -> streefpeil Value (m NAP) for sections with
    an overlapping aimedlevel segment on the same RouteId.
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
        best_overlap_km = -1.0
        best_value = None
        for aimed_row in aimed_by_route.get_group(route_id).itertuples():
            a_begin = min(aimed_row.RouteKmBegin, aimed_row.RouteKmEnd)
            a_end = max(aimed_row.RouteKmBegin, aimed_row.RouteKmEnd)
            overlap_km = min(s_end, a_end) - max(s_begin, a_begin)
            if overlap_km >= 0 and overlap_km > best_overlap_km:
                best_overlap_km = overlap_km
                best_value = aimed_row.Value
        if best_value is not None:
            value_by_section[section.id] = best_value

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
