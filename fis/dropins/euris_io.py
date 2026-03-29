import logging
import pathlib
from typing import List, Dict, Tuple

import pandas as pd
import geopandas as gpd
from fis.utils import normalize_attributes

logger = logging.getLogger(__name__)


def load_dropins_with_explicit_linking(
    export_dir: pathlib.Path, bbox=None
) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict], pd.DataFrame, pd.DataFrame]:
    """
    Loads structure and network data from a directory of GeoJSON files,
    using explicit foreign keys (locode, sectionref) to group and link.

    This loading strategy is optimized for data sources like EURIS where
    topological and grouping relationships are predefined in the dataset.

    Mapping Strategy:
    1. Units: Dimensions ending in '_cm' after normalization are converted to meters.
    2. Grouping: Uses mappings from schema.toml (e.g. slslocode -> parent_id).
    3. Fairway Linking: Explicit 'section_id' links to FairwaySection id.
    4. Geometries: Point geometries from chambers/bridges are preserved as 'topological_anchor'.
    """

    def read_euris_gdf(stype, schema_section=None):
        files = list(export_dir.glob(f"{stype}_*.geojson"))
        if not files:
            return gpd.GeoDataFrame(columns=["geometry"], crs="EPSG:4326")
        gdfs = []
        for f in files:
            try:
                gdf = gpd.read_file(f)
                if not gdf.empty:
                    gdfs.append(gdf)
            except Exception as e:
                logger.error(f"Error reading {f.name}: {e}")
        if not gdfs:
            return gpd.GeoDataFrame(columns=["geometry"], crs="EPSG:4326")

        combined = pd.concat(gdfs, ignore_index=True)
        if schema_section:
            combined = normalize_attributes(combined, schema_section)
            # Handle CM to Meter conversion for any column ending in _cm
            for col in combined.columns:
                if col.endswith("_cm"):
                    target_col = col.replace("_cm", "")
                    combined[target_col] = combined[col] / 100.0
        return combined

    logger.info("Loading structures with explicit linking...")
    lock_complexes_gdf = read_euris_gdf("LockComplex", "locks")
    lock_chambers_gdf = read_euris_gdf("LockChamber", "chambers")
    lock_chamber_areas_gdf = read_euris_gdf("LockChamberArea")  # Just for geometry

    bridge_areas_gdf = read_euris_gdf("BridgeArea", "bridges")
    bridge_openings_gdf = read_euris_gdf("BridgeOpening", "openings")

    terminals_gdf = read_euris_gdf("Terminal", "berths")
    berths_gdf = read_euris_gdf("Berth", "berths")
    sections_gdf = read_euris_gdf("FairwaySection", "sections")
    nodes_gdf = read_euris_gdf("Node", "nodes")

    if bbox:
        import shapely.geometry

        bbox_poly = shapely.geometry.box(*bbox)

        def filter_gdf(gdf):
            if gdf.empty:
                return gdf
            mask = gdf.intersects(bbox_poly)
            return gdf[mask].copy()

        lock_complexes_gdf = filter_gdf(lock_complexes_gdf)
        lock_chambers_gdf = filter_gdf(lock_chambers_gdf)
        lock_chamber_areas_gdf = filter_gdf(lock_chamber_areas_gdf)
        bridge_areas_gdf = filter_gdf(bridge_areas_gdf)
        bridge_openings_gdf = filter_gdf(bridge_openings_gdf)
        terminals_gdf = filter_gdf(terminals_gdf)
        berths_gdf = filter_gdf(berths_gdf)
        sections_gdf = filter_gdf(sections_gdf)
        nodes_gdf = filter_gdf(nodes_gdf)

    # Associate nodes with sections
    if not sections_gdf.empty and not nodes_gdf.empty:
        logger.info("Associating nodes with sections...")
        # node_id construction logic from fis.graph.euris
        # In this context, we already normalized nodes_gdf, so locode might be 'id'
        # but normalize_attributes might have converted 'locode' to 'id'
        # Let's use the raw column if available or normalized one
        id_col = "id" if "id" in nodes_gdf.columns else "locode"
        nodes_gdf["countrycode"] = nodes_gdf[id_col].apply(
            lambda x: x[:2] if x else "XX"
        )

        # objectcode might have been normalized to snake_case if not in schema.toml nodes section
        obj_col = "objectcode" if "objectcode" in nodes_gdf.columns else "object_code"
        nodes_gdf["node_id"] = nodes_gdf.apply(
            lambda row: f"{row['countrycode']}_{row[obj_col]}", axis=1
        )

        node_section = sections_gdf[["id"]].merge(
            nodes_gdf[["section_id", "node_id"]], left_on="id", right_on="section_id"
        )[["section_id", "node_id"]]

        if not node_section.empty:
            left_df = node_section.groupby("section_id").first()
            right_df = node_section.groupby("section_id").last()

            edge_nodes = pd.merge(
                left_df,
                right_df,
                left_index=True,
                right_index=True,
                suffixes=["_from", "_to"],
            )
            sections_gdf = sections_gdf.merge(
                edge_nodes.reset_index(),
                left_on="id",
                right_on="section_id",
                how="left",
            )
            sections_gdf["StartJunctionId"] = sections_gdf["node_id_from"]
            sections_gdf["EndJunctionId"] = sections_gdf["node_id_to"]

    # 1. Group Locks
    logger.info("Grouping Locks (Explicit)...")
    lock_complexes = []
    # Create sections map for geometry lookup
    sections_map = (
        sections_gdf.set_index("id")["geometry"].to_dict()
        if not sections_gdf.empty
        else {}
    )

    for _, complex_row in lock_complexes_gdf.iterrows():
        cid = complex_row["id"]
        complex_dict = complex_row.to_dict()

        # Link to sections
        sref = complex_row.get("section_id")
        if sref and pd.notna(sref):
            complex_dict["sections"] = [
                {
                    "id": sref,
                    "relation": "overlap",
                    "geometry": sections_map.get(sref).wkt
                    if sref in sections_map and sections_map.get(sref)
                    else None,
                }
            ]

        # Topological anchor (use the complex point itself)
        complex_dict["topological_anchor"] = complex_row.geometry.wkt
        complex_dict["geometry"] = complex_row.geometry.wkt

        # Chambers
        chambers = []
        relevant_chambers = lock_chambers_gdf[lock_chambers_gdf["parent_id"] == cid]
        for _, chamber_row in relevant_chambers.iterrows():
            chamber_dict = chamber_row.to_dict()

            # Preserve the Point geometry for splicing (Topological Anchor)
            chamber_dict["topological_anchor"] = chamber_row.geometry.wkt

            # Match Area for visualization
            # Use raw 'locode' for area matching if available, otherwise 'id'
            area_match_id = chamber_row.get("id")
            if not lock_chamber_areas_gdf.empty:
                # lock_chamber_areas_gdf wasn't normalized, so it has 'locode'
                areas = lock_chamber_areas_gdf[
                    lock_chamber_areas_gdf["locode"] == area_match_id
                ]
                if not areas.empty:
                    chamber_dict["geometry"] = areas.iloc[0].geometry.wkt
                else:
                    chamber_dict["geometry"] = chamber_row.geometry.wkt
            else:
                chamber_dict["geometry"] = chamber_row.geometry.wkt

            chambers.append(chamber_dict)

        complex_dict["locks"] = [{"chambers": chambers}]
        lock_complexes.append(complex_dict)

    # 2. Group Bridges
    logger.info("Grouping Bridges (Explicit)...")
    bridge_complexes = []

    for _, area_row in bridge_areas_gdf.iterrows():
        bid = area_row["id"]
        bridge_dict = area_row.to_dict()

        sref = area_row.get("section_id")
        if sref and pd.notna(sref):
            bridge_dict["sections"] = [
                {
                    "id": sref,
                    "relation": "overlap",
                    "geometry": sections_map.get(sref).wkt
                    if sref in sections_map and sections_map.get(sref)
                    else None,
                }
            ]

        bridge_dict["topological_anchor"] = area_row.geometry.wkt
        bridge_dict["geometry"] = area_row.geometry.wkt

        openings = []
        relevant_openings = bridge_openings_gdf[bridge_openings_gdf["parent_id"] == bid]
        for _, opening_row in relevant_openings.iterrows():
            opening_dict = opening_row.to_dict()
            opening_dict["topological_anchor"] = opening_row.geometry.wkt
            opening_dict["geometry"] = opening_row.geometry.wkt
            openings.append(opening_dict)

        bridge_dict["openings"] = openings
        bridge_complexes.append(bridge_dict)

    # 3. Terminals and Berths
    logger.info("Preparing Terminals and Berths (Explicit)...")
    terminals_list = []
    for _, row in terminals_gdf.iterrows():
        term_dict = row.to_dict()
        # Already normalized: locode -> id, sectionref -> section_id
        term_dict["FairwaySectionId"] = row.get("section_id")
        if "geometry" in term_dict and hasattr(term_dict["geometry"], "wkt"):
            term_dict["geometry"] = term_dict["geometry"].wkt
        terminals_list.append(term_dict)

    berths_list = []
    for _, row in berths_gdf.iterrows():
        berth_dict = row.to_dict()
        berth_dict["FairwaySectionId"] = row.get("section_id")
        if "geometry" in berth_dict and hasattr(berth_dict["geometry"], "wkt"):
            berth_dict["geometry"] = berth_dict["geometry"].wkt
        berths_list.append(berth_dict)

    return (
        lock_complexes,
        bridge_complexes,
        terminals_list,
        berths_list,
        sections_gdf,
        pd.DataFrame(),  # openings handled in bridge_complexes
    )
