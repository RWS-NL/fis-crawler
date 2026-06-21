"""FIS graph enrichment functions.

Adds attributes from maximumdimensions, navigability, navigationspeed,
fairwaydepth, fairwaytype, and tidalarea to FIS graph edges.
"""

import logging
import pathlib
from typing import Optional

import geopandas as gpd
import networkx as nx
import pandas as pd

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

    # Load optional datasets
    for name in optional:
        path = export_dir / f"{name}.geoparquet"
        if not path.exists():
            logger.warning("Optional FIS dataset missing: %s.geoparquet", name)
            continue

        datasets[name] = gpd.read_parquet(path)
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
    maxdim_df = match_by_geometry(
        sections, datasets.get("maximumdimensions"), maxdim_cols, "dim_"
    )

    nav_cols = ["Classification", "Code", "Description"]
    nav_df = match_by_geometry(sections, datasets.get("navigability"), nav_cols, "nav_")
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
        ],
        axis=1,
    )

    # Map enrichment columns to canonical names early
    from fis import utils

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
    graph = integrate_harbours(graph, node_enrichments)

    return graph


def integrate_harbours(graph, datasets):
    """Integrates harbours from the vinharbour dataset as nodes and access edges in the graph."""
    harbours = datasets.get("vinharbour")
    if harbours is None or harbours.empty:
        logger.warning(
            "vinharbour dataset not found or empty; skipping harbour integration."
        )
        return graph

    import numpy as np
    from scipy.spatial import KDTree
    from shapely.geometry import LineString
    from pyproj import Geod

    def is_valid(val):
        if val is None:
            return False
        if isinstance(val, float) and np.isnan(val):
            return False
        if str(val) == "nan":
            return False
        return True

    geod = Geod(ellps="WGS84")

    # Step 1: Map routejunction Code -> SectionJunctionId
    route_junc = datasets.get("routejunction")
    rj_code_map = {}
    if route_junc is not None:
        for _, r in route_junc.iterrows():
            c = r.get("Code")
            sjid = r.get("SectionJunctionId")
            if is_valid(c) and is_valid(sjid):
                rj_code_map[str(c).strip().upper()] = int(sjid)

    # Step 2: Prepare KDTree of all junction nodes in the graph for fallback snapping
    junction_nodes = []
    junction_coords = []
    for n_id, n_data in graph.nodes(data=True):
        if isinstance(n_id, (int, float)) or (isinstance(n_id, str) and n_id.isdigit()):
            geom = n_data.get("geometry")
            if geom and hasattr(geom, "x") and hasattr(geom, "y"):
                junction_nodes.append(int(n_id))
                junction_coords.append((geom.x, geom.y))

    tree = KDTree(np.array(junction_coords)) if junction_coords else None

    # Step 3: Add each harbour and link to the graph
    harbour_nodes_added = 0
    harbour_edges_added = 0

    for _, row in harbours.iterrows():
        raw_id = row.get("Id")
        if not is_valid(raw_id):
            continue

        h_id = f"harbour_{raw_id}"
        h_name = row.get("Name", "Unnamed Harbour")
        h_geom = row.get("geometry")

        if not h_geom:
            continue

        # Ensure it's a Point
        if h_geom.geom_type != "Point":
            h_geom = h_geom.centroid

        h_code = row.get("Code")
        h_code_str = str(h_code).strip().upper() if is_valid(h_code) else ""

        # Extract locode: prefer UnLocationCode if present and valid, otherwise first 5 chars of Code
        h_locode = row.get("UnLocationCode")
        if not is_valid(h_locode) or len(str(h_locode).strip()) < 5:
            if len(h_code_str) >= 5:
                h_locode = h_code_str[:5]
            else:
                h_locode = ""
        else:
            h_locode = str(h_locode).strip().upper()

        # Add node
        graph.add_node(
            h_id,
            node_id=h_id,
            node_type="harbour",
            name=h_name,
            locode=h_locode,
            isrs_id=h_code_str,
            vin_code=str(row.get("VinCode", "")),
            city=str(row.get("City", "")),
            geometry=h_geom,
        )
        harbour_nodes_added += 1

        # Link to target junction node
        target_node_id = None
        if h_code_str and h_code_str in rj_code_map:
            candidate = rj_code_map[h_code_str]
            if graph.has_node(candidate):
                target_node_id = candidate

        # Fallback to geometric snapping
        if target_node_id is None and tree is not None:
            dist, idx = tree.query((h_geom.x, h_geom.y))
            candidate = junction_nodes[idx]
            if graph.has_node(candidate):
                target_node_id = candidate

        if target_node_id is not None:
            target_geom = graph.nodes[target_node_id].get("geometry")
            if target_geom and hasattr(target_geom, "x"):
                access_line = LineString([h_geom, target_geom])
                graph.add_edge(
                    h_id,
                    target_node_id,
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
