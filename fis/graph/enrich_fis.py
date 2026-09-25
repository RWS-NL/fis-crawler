"""FIS graph enrichment functions.

Adds attributes from maximumdimensions, navigability, navigationspeed,
fairwaydepth, fairwaytype, and tidalarea to FIS graph edges.
"""

import logging
import pathlib
from collections import defaultdict
from typing import Optional

import geopandas as gpd
import networkx as nx
import pandas as pd
import pyproj
from pyproj import Geod
from shapely.geometry import LineString, Point
from shapely.ops import transform

from fis import settings, utils
from fis.splicer import FairwaySplicer, StructureCut
from fis.utils import normalize_attributes, stringify_id

logger = logging.getLogger(__name__)


def load_fis_node_enrichments(export_dir: pathlib.Path) -> dict[str, gpd.GeoDataFrame]:
    """Load all FIS enrichment datasets (used for both edges and nodes).

    Args:
        export_dir: Path to fis-export directory.

    Returns:
        Dict of dataset name to GeoDataFrame.
    """
    datasets = {}
    required = ["section", "routejunction"]
    optional = [
        "maximumdimensions",
        "navigability",
        "navigationspeed",
        "fairwaydepth",
        "fairwaytype",
        "tidalarea",
        "fairwayclassification",
        "fairwaystatus",
        "mgdtrajectory",
        "fairway",
        "route",
        "vinharbour",
        "aimedlevel",
        "aimedwaterlevel",
        "officiallevel",
    ]

    # Load required datasets
    for name in required:
        path = export_dir / f"{name}.geoparquet"
        if not path.exists():
            raise FileNotFoundError(
                f"Required FIS dataset '{name}.geoparquet' not found in {export_dir}. "
                "Ensure you have run the crawl-fis step."
            )
        datasets[name] = gpd.read_parquet(path)
        logger.info("Loaded required dataset %s: %d records", name, len(datasets[name]))

    # Load optional datasets. Following the dataset file naming conventions (see
    # NAMING_CONVENTIONS.md §3.2), spatial datasets are exported as '.geoparquet'
    # and non-spatial/tabular datasets as '.parquet'. We probe for '.geoparquet'
    # first and fall back to '.parquet'. If neither representation exists on disk,
    # the optional dataset is absent from the crawl and we log a warning and skip.
    for name in optional:
        path = export_dir / f"{name}.geoparquet"
        is_geo = True
        if not path.exists():
            path = export_dir / f"{name}.parquet"
            is_geo = False
        if not path.exists():
            logger.warning("Optional FIS dataset missing: %s", name)
            continue

        if is_geo:
            df = gpd.read_parquet(path)
            if "geometry" in df.columns and "Geometry" in df.columns:
                df = df.drop(columns=["Geometry"])
            datasets[name] = df
        else:
            datasets[name] = pd.read_parquet(path)
        if name == "vinharbour":
            datasets[name] = normalize_attributes(datasets[name], "harbours")
        logger.info("Loaded optional dataset %s: %d records", name, len(datasets[name]))

    return datasets


def match_by_geometry(
    sections: gpd.GeoDataFrame,
    data: Optional[gpd.GeoDataFrame],
    columns: list[str],
    prefix: str,
) -> pd.DataFrame:
    """Match data to sections by exact geometry WKT.

    Args:
        sections: Sections GeoDataFrame with Id column.
        data: Optional Data GeoDataFrame to match.
        columns: Columns to extract from data.
        prefix: Prefix to add to column names.

    Returns:
        DataFrame indexed by section Id with prefixed columns.
    """
    if data is None:
        return pd.DataFrame(index=sections["Id"])

    if data.empty:
        raise ValueError(f"Data provided for {prefix} geometry matching is empty.")

    available = [c for c in columns if c in data.columns]
    if not available:
        return pd.DataFrame(index=sections["Id"])

    # Use geometry WKT as join key
    sections = sections.copy()
    sections["_geom_key"] = sections.geometry.apply(lambda g: g.wkt)

    data = data.copy()
    data["_geom_key"] = data.geometry.apply(lambda g: g.wkt)

    # Select and deduplicate
    data_select = data[["_geom_key"] + available].drop_duplicates("_geom_key")
    data_select = data_select.rename(columns={c: f"{prefix}{c}" for c in available})

    # Join
    result = (
        sections[["Id", "_geom_key"]]
        .merge(data_select, on="_geom_key", how="left")
        .drop(columns=["_geom_key"])
        .set_index("Id")
    )

    matched = result.notna().any(axis=1).sum()
    logger.info("Matched %d sections by geometry for %s", matched, prefix)

    return result


def match_by_route_km(
    sections: gpd.GeoDataFrame,
    data: Optional[gpd.GeoDataFrame],
    columns: list[str],
    prefix: str,
) -> pd.DataFrame:
    """Match data to sections by RouteId and overlapping km ranges.

    Uses range overlap: section [km_begin, km_end] overlaps data [km_begin, km_end]
    when they share the same RouteId.

    Args:
        sections: Sections with RouteId, RouteKmBegin, RouteKmEnd.
        data: Optional Data with same columns.
        columns: Columns to extract.
        prefix: Prefix for output columns.

    Returns:
        DataFrame indexed by section Id with prefixed columns.
    """
    if data is None:
        return pd.DataFrame(index=sections["Id"])

    if data.empty:
        raise ValueError(f"Data provided for {prefix} route/km matching is empty.")

    # Check required columns
    required = ["RouteId", "RouteKmBegin", "RouteKmEnd"]
    for col in required:
        if col not in sections.columns or col not in data.columns:
            logger.warning("Missing %s column for route/km matching", col)
            return pd.DataFrame(index=sections["Id"])

    available = [c for c in columns if c in data.columns]
    if not available:
        return pd.DataFrame(index=sections["Id"])

    # Build section index
    sections = sections.copy()
    sections = sections.dropna(subset=["RouteId", "RouteKmBegin", "RouteKmEnd"])

    data = data.copy()
    data = data.dropna(subset=["RouteId", "RouteKmBegin", "RouteKmEnd"])

    # Group data by RouteId for efficient lookup
    data_by_route = data.groupby("RouteId")

    results = []
    for _, section in sections.iterrows():
        section_id = section["Id"]
        route_id = section["RouteId"]
        s_begin = min(section["RouteKmBegin"], section["RouteKmEnd"])
        s_end = max(section["RouteKmBegin"], section["RouteKmEnd"])

        if route_id not in data_by_route.groups:
            continue

        route_data = data_by_route.get_group(route_id)

        # Find overlapping records
        for _, row in route_data.iterrows():
            d_begin = min(row["RouteKmBegin"], row["RouteKmEnd"])
            d_end = max(row["RouteKmBegin"], row["RouteKmEnd"])

            # Overlap check: ranges overlap if not (s_end < d_begin or d_end < s_begin)
            if not (s_end < d_begin or d_end < s_begin):
                result_row = {"Id": section_id}
                for col in available:
                    result_row[f"{prefix}{col}"] = row[col]
                results.append(result_row)
                break  # Take first match

    if not results:
        return pd.DataFrame(index=sections["Id"])

    result_df = pd.DataFrame(results).drop_duplicates("Id").set_index("Id")

    # Reindex to include all section IDs
    all_ids = sections["Id"].unique()
    result_df = result_df.reindex(all_ids)

    matched = result_df.notna().any(axis=1).sum()
    logger.info("Matched %d sections by route/km for %s", matched, prefix)

    return result_df


def build_fis_edge_enrichments(datasets: dict[str, gpd.GeoDataFrame]) -> pd.DataFrame:
    """Build enrichment lookup by joining all datasets to sections.

    Args:
        datasets: Dict of dataset name to GeoDataFrame.

    Returns:
        DataFrame indexed by section Id with all enrichment columns.
    """
    sections = datasets["section"]

    # Geometry-based matching
    maxdim_cols = [
        "GeneralDepth",
        "GeneralLength",
        "GeneralWidth",
        "GeneralHeight",
        "SeaFairingDepth",
        "SeaFairingLength",
        "SeaFairingWidth",
        "SeaFairingHeight",
        "PushedDepth",
        "PushedLength",
        "PushedWidth",
        "CoupledDepth",
        "CoupledLength",
        "CoupledWidth",
        "WidePushedDepth",
        "WidePushedLength",
        "WidePushedWidth",
        "WidePushedHeight",
        "Note",
    ]
    maxdim_df = match_by_route_km(
        sections, datasets.get("maximumdimensions"), maxdim_cols, "dim_"
    )

    nav_cols = ["Classification", "Code", "Description"]
    nav_df = match_by_route_km(sections, datasets.get("navigability"), nav_cols, "nav_")
    # Add cemt_class alias
    if "nav_Code" in nav_df.columns:
        nav_df["cemt_class"] = nav_df["nav_Code"]

    # Route/km-based matching
    speed_cols = [
        "Speed",
        "MaxSpeedUp",
        "MaxSpeedDown",
        "CalibratedSpeedUp",
        "CalibratedSpeedDown",
        "CalibratedSpeedConvoyUp",
        "CalibratedSpeedConvoyDown",
        "MaxSpeedConvoyUp",
        "MaxSpeedConvoyDown",
        "SpeedConvoy",
    ]
    speed_df = match_by_route_km(
        sections, datasets.get("navigationspeed"), speed_cols, "speed_"
    )

    depth_cols = ["MinimalDepthLowerLimit", "MinimalDepthUpperLimit", "ReferenceLevel"]
    depth_df = match_by_route_km(
        sections, datasets.get("fairwaydepth"), depth_cols, "depth_"
    )

    type_cols = ["CharacterTypeCode"]
    type_df = match_by_route_km(
        sections, datasets.get("fairwaytype"), type_cols, "type_"
    )

    # Tidal area - just mark as boolean
    tidal_df = match_by_route_km(
        sections, datasets.get("tidalarea"), ["Name"], "tidal_"
    )
    if "tidal_Name" in tidal_df.columns:
        tidal_df["is_tidal"] = tidal_df["tidal_Name"].notna()
        tidal_df = tidal_df.drop(columns=["tidal_Name"])

    # Fairway classification (HTA/HVW)
    fwc_cols = ["TypeDescription", "Type"]
    fwc_df = match_by_route_km(
        sections, datasets.get("fairwayclassification"), fwc_cols, "fwc_"
    )

    # Fairway status
    status_cols = ["TrajectCode", "StatusCode", "StatusDescription", "Note"]
    status_df = match_by_route_km(
        sections, datasets.get("fairwaystatus"), status_cols, "status_"
    )

    # MGD Trajectory
    mgd_cols = ["FromTo"]
    mgd_df = match_by_route_km(
        sections, datasets.get("mgdtrajectory"), mgd_cols, "mgd_"
    )

    # Fairway number (join by FairwayId)
    fairway = datasets.get("fairway")
    if (
        fairway is not None
        and not fairway.empty
        and {"Id", "FairwayNumber"}.issubset(fairway.columns)
        and "FairwayId" in sections.columns
    ):
        fairway_df = (
            sections[["Id", "FairwayId"]]
            .merge(
                fairway[["Id", "FairwayNumber"]].rename(columns={"Id": "FairwayId"}),
                on="FairwayId",
                how="left",
            )
            .set_index("Id")[["FairwayNumber"]]
        )
    else:
        fairway_df = pd.DataFrame(index=sections["Id"], columns=["FairwayNumber"])

    # Route code and WaterName (join by RouteId)
    route = datasets.get("route")
    if (
        route is not None
        and not route.empty
        and {"Id", "Code", "WaterName"}.issubset(route.columns)
        and "RouteId" in sections.columns
    ):
        route_df = (
            sections[["Id", "RouteId"]]
            .merge(
                route[["Id", "Code", "WaterName"]].rename(columns={"Id": "RouteId"}),
                on="RouteId",
                how="left",
            )
            .set_index("Id")[["Code", "WaterName"]]
        )
    else:
        route_df = pd.DataFrame(index=sections["Id"], columns=["Code", "WaterName"])

    # Aimed Level (streefpeil)
    aimedlevel = datasets.get("aimedlevel")
    officiallevel = datasets.get("officiallevel")
    if aimedlevel is not None and not aimedlevel.empty:
        if officiallevel is not None and not officiallevel.empty:
            ol_rename = officiallevel[["Id", "Name"]].rename(
                columns={"Id": "OfficialLevelId", "Name": "OfficialLevelName"}
            )
            aimedlevel = aimedlevel.copy()
            aimedlevel["OfficialLevelId"] = aimedlevel["OfficialLevelId"].apply(
                utils.stringify_id
            )
            ol_rename["OfficialLevelId"] = ol_rename["OfficialLevelId"].apply(
                utils.stringify_id
            )
            aimedlevel = aimedlevel.merge(ol_rename, on="OfficialLevelId", how="left")
            datasets["aimedlevel"] = aimedlevel

    aimed_cols = ["Value", "OfficialLevelName"]
    aimed_df = match_by_route_km(
        sections, datasets.get("aimedlevel"), aimed_cols, "aimed_"
    )

    # Aimed Water Level deviations and average level
    aimedwater_cols = [
        "MaximumNegativeDeviation",
        "MaximumPositiveDeviation",
        "AverageLevel",
    ]
    aimedwater_df = match_by_route_km(
        sections, datasets.get("aimedwaterlevel"), aimedwater_cols, "aimedwater_"
    )

    # Combine all enrichment
    enrichment = pd.concat(
        [
            maxdim_df,
            nav_df,
            speed_df,
            depth_df,
            type_df,
            tidal_df,
            fwc_df,
            status_df,
            mgd_df,
            fairway_df,
            route_df,
            aimed_df,
            aimedwater_df,
        ],
        axis=1,
    )

    # Map enrichment columns to canonical names early
    schema = utils.load_schema()
    mappings = schema.get("attributes", {}).get("edges", {})

    # Create mapping for enrichment columns (which have prefixes)
    # This ensures dim_GeneralWidth -> dim_width, speed_Speed -> maxspeed, etc.
    rename_map = {}
    for col in enrichment.columns:
        if col in mappings:
            rename_map[col] = mappings[col]

    if rename_map:
        # If multiple source columns map to same canonical name, we might lose data
        # but the schema expects unique canonical names.
        enrichment = enrichment.rename(columns=rename_map)

    # Summary stats
    for prefix, desc in [
        ("dim_", "dimensions"),
        ("cemt_", "CEMT"),
        ("maxspeed", "speed"),
        ("depth_", "depth"),
        ("fairway_type", "type"),
        ("is_tidal", "tidal"),
        ("fwc_", "fairway_classification"),
        ("status_", "status"),
        ("mgd_", "MGD"),
        ("fairway_number", "fairway_number"),
        ("route_code", "route_code"),
        ("water_name", "water_name"),
        ("aimed", "aimed levels"),
    ]:
        cols = [c for c in enrichment.columns if c.startswith(prefix)]
        if cols:
            count = enrichment[cols].notna().any(axis=1).sum()
            logger.info("Total sections with %s: %d", desc, count)

    return enrichment


def enrich_fis_graph(
    graph: nx.Graph,
    sections: gpd.GeoDataFrame,
    edge_enrichments: pd.DataFrame,
    node_enrichments: Optional[dict[str, gpd.GeoDataFrame]] = None,
) -> nx.Graph:
    """Add enrichment attributes to FIS graph edges and nodes.

    Args:
        graph: FIS networkx graph (nodes are junction IDs).
        sections: Sections GeoDataFrame with junction ID columns.
        edge_enrichments: DataFrame indexed by section Id with enrichment attrs.
        node_enrichments: Optional dict of all FIS datasets for node enrichment.

    Returns:
        Graph with enriched attributes.
    """
    # 1. Enrich Edges
    # Build edge → section mapping
    section_lookup = (
        sections[["Id", "StartJunctionId", "EndJunctionId"]]
        .dropna(subset=["StartJunctionId", "EndJunctionId"])
        .assign(
            start=lambda df: df["StartJunctionId"].astype(int),
            end=lambda df: df["EndJunctionId"].astype(int),
        )
    )

    edge_to_section = {
        **{(row.start, row.end): row.Id for row in section_lookup.itertuples()},
        **{(row.end, row.start): row.Id for row in section_lookup.itertuples()},
    }

    logger.info(
        "Built edge-to-section mapping with %d entries", len(edge_to_section) // 2
    )

    # Apply edge enrichment
    enriched_edges_count = 0
    for u, v, data in graph.edges(data=True):
        section_id = edge_to_section.get((u, v))
        if section_id is None or section_id not in edge_enrichments.index:
            continue

        attrs = edge_enrichments.loc[section_id].dropna().to_dict()
        data.update(attrs)
        if attrs:
            enriched_edges_count += 1

    logger.info("Enriched %d / %d edges", enriched_edges_count, graph.number_of_edges())

    # 2. Enrich Nodes (Locode / ISRS)
    if node_enrichments is None:
        return graph

    # We use routejunction to map sectionjunctions to locodes
    route_junc = node_enrichments.get("routejunction")
    if route_junc is None:
        logger.warning(
            "Node enrichment requested but 'routejunction' dataset is missing; skipping."
        )
        return graph

    enriched_nodes_count = 0

    logger.info(
        "Enriching nodes using routejunction dataset, records: %d",
        len(route_junc),
    )
    # Map section_junction_id -> first locode found
    # Ensure SectionJunctionId is integer for matching with graph nodes
    node_locode_map = (
        route_junc.dropna(subset=["SectionJunctionId", "Code"])
        .assign(sid=lambda df: df["SectionJunctionId"].astype(int))
        .groupby("sid")["Code"]
        .first()
        .to_dict()
    )

    for node_id in graph.nodes():
        # node_id in graph is the junction Id (int)
        locode = node_locode_map.get(node_id)
        if not locode:
            continue

        graph.nodes[node_id]["locode"] = locode
        enriched_nodes_count += 1

    logger.info(
        "Enriched %d / %d nodes with locode",
        enriched_nodes_count,
        graph.number_of_nodes(),
    )

    # 3. Integrate Harbours as Nodes & Edges
    graph = integrate_harbours(graph, node_enrichments, sections=sections)

    return graph


def integrate_harbours(
    graph: nx.Graph,
    datasets: dict,
    sections: Optional[gpd.GeoDataFrame] = None,
) -> nx.Graph:
    """Integrates harbours from vinharbour as spliced nodes and access edges in the graph.

    Uses ID-based matching via FairwaySectionId / section_id and FairwaySplicer to project
    harbours onto the corresponding fairway section in RD New (EPSG:28992). Inserts connection
    nodes on the fairway, splices fairway edges cleanly, and adds short harbour_access edges.
    """
    harbours = datasets.get("vinharbour")
    if harbours is None or harbours.empty:
        logger.warning(
            "vinharbour dataset not found or empty; skipping harbour integration."
        )
        return graph

    if "section_id" not in harbours.columns:
        if "geometry" in harbours.columns and "Geometry" in harbours.columns:
            harbours = harbours.drop(columns=["Geometry"])
        harbours = normalize_attributes(harbours, "harbours")

    if sections is None:
        sections = datasets.get("section")

    project_to_rd = pyproj.Transformer.from_crs(
        "EPSG:4326", settings.PROJECTED_CRS, always_xy=True
    ).transform
    project_to_4326 = pyproj.Transformer.from_crs(
        settings.PROJECTED_CRS, "EPSG:4326", always_xy=True
    ).transform
    geod = Geod(ellps="WGS84")

    # Map section_id -> edge in graph
    edge_by_sec_id: dict[str, tuple] = {}
    for u, v, d in graph.edges(data=True):
        sid = d.get("id", d.get("Id"))
        if sid is not None:
            edge_by_sec_id[stringify_id(sid)] = tuple(sorted([u, v]))

    sec_by_id = {}
    if sections is not None:
        for _, r in sections.iterrows():
            sid = r.get("id", r.get("Id"))
            if sid is not None:
                sec_by_id[stringify_id(sid)] = r

    # Group harbours by graph edge
    harbours_by_edge = defaultdict(list)
    for _, h in harbours.iterrows():
        raw_id = h.get("id", h.get("Id"))
        if raw_id is None:
            continue
        sec_id = stringify_id(h.get("section_id", h.get("FairwaySectionId")))
        if not sec_id:
            logger.warning("Harbour %s has no section_id; skipping.", raw_id)
            continue

        edge_key = edge_by_sec_id.get(sec_id)
        if edge_key is None and sec_by_id:
            sec = sec_by_id.get(sec_id)
            if sec is not None:
                sj = sec.get("start_junction_id", sec.get("StartJunctionId"))
                ej = sec.get("end_junction_id", sec.get("EndJunctionId"))
                if sj is not None and ej is not None:
                    cand_u, cand_v = None, None
                    for cand in (
                        sj,
                        stringify_id(sj),
                        int(sj) if str(sj).isdigit() else None,
                    ):
                        if cand is not None and graph.has_node(cand):
                            cand_u = cand
                            break
                    for cand in (
                        ej,
                        stringify_id(ej),
                        int(ej) if str(ej).isdigit() else None,
                    ):
                        if cand is not None and graph.has_node(cand):
                            cand_v = cand
                            break
                    if (
                        cand_u is not None
                        and cand_v is not None
                        and graph.has_edge(cand_u, cand_v)
                    ):
                        edge_key = tuple(sorted([cand_u, cand_v]))

        if edge_key is None:
            logger.warning(
                "Harbour %s references section_id '%s' which was not found in the graph; skipping.",
                raw_id,
                sec_id,
            )
            continue

        harbours_by_edge[edge_key].append(h)

    harbour_nodes_added = 0
    harbour_edges_added = 0

    for (u, v), h_list in harbours_by_edge.items():
        edge_attrs = dict(graph[u][v])
        line_4326 = edge_attrs.get("geometry")
        if line_4326 is None or line_4326.geom_type != "LineString":
            continue

        # Orient line_4326 from u to v
        u_geom = graph.nodes[u].get("geometry")
        if u_geom and hasattr(u_geom, "x"):
            p_u = Point(u_geom.x, u_geom.y)
            if p_u.distance(Point(line_4326.coords[0])) > p_u.distance(
                Point(line_4326.coords[-1])
            ):
                u, v = v, u

        line_rd = transform(project_to_rd, line_4326)

        h_projs = []
        for h in h_list:
            h_geom = h.get("geometry")
            if not h_geom:
                continue
            if h_geom.geom_type != "Point":
                h_geom = h_geom.centroid
            h_rd = transform(project_to_rd, h_geom)
            p_dist = max(0.0, min(line_rd.length, line_rd.project(h_rd)))
            h_projs.append((h, h_geom, p_dist))

        if not h_projs:
            continue

        # Cluster harbours within 1m along the fairway line
        h_projs.sort(key=lambda x: x[2])
        clusters = []
        for h, h_geom, p_dist in h_projs:
            if clusters and abs(p_dist - clusters[-1]["dist"]) < 1.0:
                clusters[-1]["harbours"].append((h, h_geom))
                continue
            h_id_str = stringify_id(h.get("id", h.get("Id")))
            clusters.append(
                {
                    "dist": p_dist,
                    "harbours": [(h, h_geom)],
                    "conn_id": f"harbour_{h_id_str}_connection",
                }
            )

        cuts = [
            StructureCut(
                id=cl["conn_id"],
                geometry=line_rd.interpolate(cl["dist"]),
                projected_distance=cl["dist"],
                buffer_distance=0.0,
            )
            for cl in clusters
        ]

        splicer = FairwaySplicer(line_rd)
        segments = splicer.splice(cuts)

        graph.remove_edge(u, v)

        for seg in segments:
            seg_geom_4326 = transform(project_to_4326, seg.geometry)
            su = u if seg.source_structure_id is None else seg.source_structure_id
            sv = v if seg.target_structure_id is None else seg.target_structure_id
            attrs = edge_attrs.copy()
            attrs["geometry"] = seg_geom_4326
            attrs["length_m"] = geod.geometry_length(seg_geom_4326)
            graph.add_edge(su, sv, **attrs)

        for cl in clusters:
            conn_id = cl["conn_id"]
            conn_pt_rd = line_rd.interpolate(cl["dist"])
            conn_pt_4326 = transform(project_to_4326, conn_pt_rd)
            graph.add_node(
                conn_id,
                node_id=conn_id,
                node_type="junction",
                feature_type="node",
                geometry=conn_pt_4326,
            )

            for h, h_geom in cl["harbours"]:
                raw_id = stringify_id(h.get("id"))
                hid = f"harbour_{raw_id}"
                h_name = str(h.get("name") or "Unnamed Harbour").strip()
                locode = str(
                    h.get("locode")
                    or h.get("un_location_code")
                    or h.get("UnLocationCode")
                    or ""
                )
                isrs_id = str(h.get("isrs_id") or h.get("code") or h.get("Code") or "")
                vin_code = str(h.get("vin_code") or h.get("VinCode") or "")
                city = str(h.get("city") or h.get("City") or "")

                if len(locode) < 5 and len(isrs_id) >= 5:
                    locode = isrs_id[:5]

                graph.add_node(
                    hid,
                    node_id=hid,
                    node_type="harbour",
                    name=h_name,
                    locode=locode,
                    isrs_id=isrs_id,
                    vin_code=vin_code,
                    city=city,
                    geometry=h_geom,
                )
                harbour_nodes_added += 1

                access_line = LineString([h_geom, conn_pt_4326])
                graph.add_edge(
                    hid,
                    conn_id,
                    geometry=access_line,
                    length_m=geod.geometry_length(access_line),
                    segment_type="harbour_access",
                    data_source="vinharbour",
                    name=f"Access to {h_name}",
                )
                harbour_edges_added += 1

    logger.info(
        "Integrated %d harbour nodes and %d harbour access edges into the graph.",
        harbour_nodes_added,
        harbour_edges_added,
    )
    return graph
