import logging
from typing import List, Dict, Set, Tuple, Optional, Any

import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, mapping
from tqdm import tqdm
from pyproj import Geod

from fis.splicer import FairwaySplicer, StructureCut
from fis import settings, utils

logger = logging.getLogger(__name__)
geod = Geod(ellps="WGS84")


def splice_fairways(
    sections: pd.DataFrame,
    dropins_by_section: Dict[Any, List[Dict]],
    embedded_bridges: Dict[str, Dict],
    mode: str = "detailed",
) -> List[Dict]:
    """
    Iterates over all fairway sections and splices them into sub-segments
    based on the structures (drop-ins) that lie upon them.

    Approach divergence handling:
    1. Attribute consistency: Splicing logic handles both 'length' (FIS) and
       'dim_length' (EURIS) attributes via field-agnostic lookups.
    2. Splicing Geometry: For structures provided as Points (common in EURIS),
       the splicer projections handle Point geometries to derive cut distances.
    3. Hierarchy: Embedded bridge logic is shared and applied to any source
       where a bridge is spatially part of a lock complex.
    """
    # Harmonize junction ID column naming between legacy (camelCase) and
    # normalized (snake_case) schemas so downstream code can rely on either.
    # This supports:
    # - EURIS/legacy sources that provide StartJunctionId/EndJunctionId
    # - Normalized inputs (e.g. via schema.toml) that use start_junction_id /
    #   end_junction_id
    if sections is not None:
        # Work on a shallow copy if we are going to add columns, to avoid
        # mutating caller-owned DataFrames unexpectedly.
        needs_alias = any(
            name not in sections.columns
            for name in (
                "StartJunctionId",
                "EndJunctionId",
                "start_junction_id",
                "end_junction_id",
            )
        )
        if needs_alias:
            sections = sections.copy()

        # Create legacy camelCase columns from normalized snake_case when needed.
        if (
            "StartJunctionId" not in sections.columns
            and "start_junction_id" in sections.columns
        ):
            sections["StartJunctionId"] = sections["start_junction_id"]
        if (
            "EndJunctionId" not in sections.columns
            and "end_junction_id" in sections.columns
        ):
            sections["EndJunctionId"] = sections["end_junction_id"]

        # And vice versa: ensure normalized columns exist when only legacy ones
        # are provided (helps any newer code that expects snake_case).
        if (
            "start_junction_id" not in sections.columns
            and "StartJunctionId" in sections.columns
        ):
            sections["start_junction_id"] = sections["StartJunctionId"]
        if (
            "end_junction_id" not in sections.columns
            and "EndJunctionId" in sections.columns
        ):
            sections["end_junction_id"] = sections["EndJunctionId"]

    all_features = []

    sections_gdf = _prepare_sections_gdf(sections)
    embedded_ids = {str(k) for k in embedded_bridges.keys()}

    for _, sec in tqdm(
        sections_gdf.iterrows(),
        total=len(sections_gdf),
        desc="Splicing fairways",
        mininterval=2.0,
    ):
        line_geom = sec.geometry
        if not line_geom or line_geom.is_empty:
            continue

        sid = utils.stringify_id(sec["id"])
        dropins_on_sec = dropins_by_section.get(sid, [])

        visible_dropins = [
            d
            for d in dropins_on_sec
            if mode == "simplified" or not _is_embedded(d, embedded_ids)
        ]

        if not visible_dropins:
            _handle_clear_section(all_features, sec)
            continue

        _slice_section_with_dropins(
            all_features, sec, visible_dropins, dropins_on_sec, mode=mode
        )
    return all_features


def _is_embedded(dropin: Dict, embedded_ids: Set[str]) -> bool:
    if dropin["type"] != "bridge":
        return False
    # If it's a bridge, we expect it to have openings
    for op in dropin["obj"]["openings"]:
        if str(op["id"]) in embedded_ids:
            return True
    return False


def _prepare_sections_gdf(sections: pd.DataFrame) -> gpd.GeoDataFrame:
    if sections is not None and "geometry" in sections.columns:
        sections = sections.copy()
        sections["geometry"] = sections["geometry"].apply(
            lambda x: wkt.loads(x) if isinstance(x, str) else x
        )
    return gpd.GeoDataFrame(sections, geometry="geometry", crs="EPSG:4326")


def _handle_clear_section(all_features, sec):
    sid = utils.stringify_id(sec["id"])
    fairway_id = utils.stringify_id(sec.get("fairway_id"))
    name = sec.get("Name", sec.get("FairwayName"))
    start_junc = sec.get("StartJunctionId")
    end_junc = sec.get("EndJunctionId")
    line_geom = sec.geometry

    source_id = utils.stringify_id(start_junc)
    target_id = utils.stringify_id(end_junc)

    all_features.append(
        {
            "type": "Feature",
            "geometry": mapping(line_geom),
            "properties": {
                "id": f"fairway_segment_section_{sid}",
                "feature_type": "fairway_segment",
                "segment_type": "clear",
                "section_id": sid,
                "fairway_id": fairway_id,
                "name": name,
                "source_node": source_id,
                "target_node": target_id,
                "length_m": geod.geometry_length(line_geom),
            },
        }
    )
    _yield_junction_nodes(all_features, line_geom, True, True, start_junc, end_junc)


def _slice_section_with_dropins(
    all_features, sec, visible_dropins, original_dropins_on_sec, mode="detailed"
):
    line_geom = sec.geometry
    line_rd_series = gpd.GeoSeries([line_geom], crs="EPSG:4326")
    utm_crs = line_rd_series.estimate_utm_crs()
    line_rd = line_rd_series.to_crs(utm_crs).iloc[0]

    splicer = FairwaySplicer(line_rd)
    cuts = _generate_structure_cuts(line_rd, visible_dropins, utm_crs, mode=mode)
    segments = splicer.splice(cuts)

    _handle_consumed_junctions(
        all_features,
        sec,
        cuts,
        line_geom,
        utm_crs,
        splicer.total_length,
        original_dropins_on_sec,
    )

    _generate_spliced_features(
        all_features, sec, segments, original_dropins_on_sec, utm_crs
    )


def _handle_consumed_junctions(
    all_features, sec, cuts, line_geom, utm_crs, total_length, original_dropins_on_sec
):
    """
    Generate connecting edges if the structures consumed the start or end junctions.
    """
    if not cuts:
        return

    sec_id = utils.stringify_id(sec["id"])

    # Start junction
    if sec.get("StartJunctionId") and pd.notna(sec.get("StartJunctionId")):
        first_cut = min(cuts, key=lambda c: c.projected_distance - c.buffer_before)
        if first_cut.projected_distance - first_cut.buffer_before <= 0.01:
            sj_id = utils.stringify_id(sec["StartJunctionId"])
            dtype, did = first_cut.id.split("_", 1)
            did = utils.stringify_id(did)
            if dtype not in ("terminal", "berth"):
                split_node = f"{dtype}_{did}_{sec_id}_split"
                pt = Point(line_geom.coords[0])
                pt_4326 = gpd.GeoSeries([pt], crs=utm_crs).to_crs("EPSG:4326").iloc[0]

                _assign_geom_wkt(
                    original_dropins_on_sec,
                    dtype,
                    did,
                    "split_points",
                    pt_4326.wkt,
                    sec_id=sec_id,
                )

                all_features.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(pt_4326),
                        "properties": {
                            "id": f"consumed_start_{sec_id}_{did}",
                            "feature_type": "fairway_segment",
                            "segment_type": "approach_or_exit",
                            "source_node": sj_id,
                            "target_node": split_node,
                            "length_m": 0.0,
                            "section_id": sec_id,
                        },
                    }
                )
                _yield_junction_nodes(
                    all_features, line_geom, True, False, sec["StartJunctionId"], None
                )

    # End junction
    if sec.get("EndJunctionId") and pd.notna(sec.get("EndJunctionId")):
        last_cut = max(cuts, key=lambda c: c.projected_distance + c.buffer_after)
        if last_cut.projected_distance + last_cut.buffer_after >= total_length - 0.01:
            ej_id = utils.stringify_id(sec["EndJunctionId"])
            dtype, did = last_cut.id.split("_", 1)
            did = utils.stringify_id(did)
            if dtype not in ("terminal", "berth"):
                merge_node = f"{dtype}_{did}_{sec_id}_merge"
                pt = Point(line_geom.coords[-1])
                pt_4326 = gpd.GeoSeries([pt], crs=utm_crs).to_crs("EPSG:4326").iloc[0]

                _assign_geom_wkt(
                    original_dropins_on_sec,
                    dtype,
                    did,
                    "merge_points",
                    pt_4326.wkt,
                    sec_id=sec_id,
                )

                all_features.append(
                    {
                        "type": "Feature",
                        "geometry": mapping(pt_4326),
                        "properties": {
                            "id": f"consumed_end_{sec_id}_{did}",
                            "feature_type": "fairway_segment",
                            "segment_type": "approach_or_exit",
                            "source_node": merge_node,
                            "target_node": ej_id,
                            "length_m": 0.0,
                            "section_id": sec_id,
                        },
                    }
                )
                _yield_junction_nodes(
                    all_features, line_geom, False, True, None, sec["EndJunctionId"]
                )


def _generate_spliced_features(
    all_features, sec, segments, original_dropins_on_sec, utm_crs
):
    """
    Convert spliced segments into GeoJSON features.
    """
    sec_id = utils.stringify_id(sec["id"])
    fairway_id = utils.stringify_id(sec.get("fairway_id"))

    for i, segment in enumerate(segments):
        seg_4326 = (
            gpd.GeoSeries([segment.geometry], crs=utm_crs).to_crs("EPSG:4326").iloc[0]
        )
        source_node, is_start_junc = _determine_source_node(
            segment,
            sec.get("StartJunctionId"),
            original_dropins_on_sec,
            seg_4326,
            sec_id,
        )
        target_node, is_end_junc = _determine_target_node(
            segment, sec.get("EndJunctionId"), original_dropins_on_sec, seg_4326, sec_id
        )

        all_features.append(
            {
                "type": "Feature",
                "geometry": mapping(seg_4326),
                "properties": {
                    "id": f"fairway_segment_section_{sec_id}_{i}",
                    "feature_type": "fairway_segment",
                    "segment_type": "fairway"
                    if is_start_junc and is_end_junc
                    else "approach_or_exit",
                    "section_id": sec_id,
                    "fairway_id": fairway_id,
                    "name": sec.get("name")
                    or sec.get("Name")
                    or sec.get("FairwayName"),
                    "source_node": source_node,
                    "target_node": target_node,
                    "length_m": geod.geometry_length(seg_4326),
                },
            }
        )
        _yield_junction_nodes(
            all_features,
            seg_4326,
            is_start_junc,
            is_end_junc,
            sec.get("StartJunctionId"),
            sec.get("EndJunctionId"),
        )


def _determine_source_node(
    segment: Any, start_junc: Any, dropins: List[Dict], seg_4326: Any, sec_id: str
) -> Tuple[Optional[str], bool]:
    is_start = True
    node = utils.stringify_id(start_junc)
    if segment.source_structure_id:
        dtype, did = segment.source_structure_id.split("_", 1)
        did = utils.stringify_id(did)
        if dtype in ("terminal", "berth"):
            node = f"{dtype}_{did}_connection"
            _assign_geom_wkt(
                dropins,
                dtype,
                did,
                "connection_geometry",
                Point(seg_4326.coords[0]).wkt,
            )
        else:
            node = f"{dtype}_{did}_{sec_id}_merge"
            _assign_geom_wkt(
                dropins,
                dtype,
                did,
                "merge_points",
                Point(seg_4326.coords[0]).wkt,
                sec_id=sec_id,
            )
        is_start = False
    return node, is_start


def _determine_target_node(
    segment: Any, end_junc: Any, dropins: List[Dict], seg_4326: Any, sec_id: str
) -> Tuple[Optional[str], bool]:
    is_end = True
    node = utils.stringify_id(end_junc)
    if segment.target_structure_id:
        dtype, did = segment.target_structure_id.split("_", 1)
        did = utils.stringify_id(did)
        if dtype in ("terminal", "berth"):
            node = f"{dtype}_{did}_connection"
            _assign_geom_wkt(
                dropins,
                dtype,
                did,
                "connection_geometry",
                Point(seg_4326.coords[-1]).wkt,
            )
        else:
            node = f"{dtype}_{did}_{sec_id}_split"
            _assign_geom_wkt(
                dropins,
                dtype,
                did,
                "split_points",
                Point(seg_4326.coords[-1]).wkt,
                sec_id=sec_id,
            )
        is_end = False
    return node, is_end


def _generate_structure_cuts(
    line_rd: Any, dropins_on_sec: List[Dict], utm_crs: str, mode: str = "detailed"
) -> List[StructureCut]:
    cuts = []
    for dropin in dropins_on_sec:
        obj = dropin["obj"]
        # Use 'topological_anchor' for precise splicing position (e.g. snapped points for EURIS)
        # Fallback to 'geometry' (which might be a Polygon centroid)
        geom_val = obj.get("topological_anchor") or obj.get("geometry")
        if not geom_val:
            raise ValueError(
                f"Drop-in {dropin['type']} {obj.get('id', obj.get('Id'))} has no geometry or topological_anchor. "
                "Cannot calculate splicing position."
            )

        geom = wkt.loads(geom_val) if isinstance(geom_val, str) else geom_val
        geom_rd = gpd.GeoSeries([geom], crs="EPSG:4326").to_crs(utm_crs).iloc[0]
        if geom_rd.geom_type != "Point":
            geom_rd = geom_rd.centroid

        dist = line_rd.project(geom_rd)

        if dropin["type"] == "lock" and mode == "detailed":
            # For detailed locks, project the actual chamber geometries to find exact bounds
            min_proj = float("inf")
            max_proj = float("-inf")
            valid = False
            for child in obj.get("locks", []):
                for ch in child.get("chambers", []):
                    c_geom_val = ch.get("route_geometry") or ch.get("geometry")
                    if c_geom_val:
                        c_geom = (
                            wkt.loads(c_geom_val)
                            if isinstance(c_geom_val, str)
                            else c_geom_val
                        )
                        c_geom_rd = (
                            gpd.GeoSeries([c_geom], crs="EPSG:4326")
                            .to_crs(utm_crs)
                            .iloc[0]
                        )
                        # project points to find min and max extent
                        coords = []
                        if c_geom_rd.geom_type in ("Polygon", "MultiPolygon"):
                            coords = c_geom_rd.exterior.coords
                        elif c_geom_rd.geom_type in ("LineString", "MultiLineString"):
                            coords = c_geom_rd.coords
                        else:
                            coords = [c_geom_rd]
                        for coord in coords:
                            proj = line_rd.project(Point(coord))
                            min_proj = min(min_proj, proj)
                            max_proj = max(max_proj, proj)
                            valid = True

            if valid:
                center_dist = (min_proj + max_proj) / 2.0
                half_len = (max_proj - min_proj) / 2.0
                buffer_before = half_len + settings.DETAILED_LOCK_SPLICING_BUFFER_M
                buffer_after = half_len + settings.DETAILED_LOCK_SPLICING_BUFFER_M
                dist = center_dist
                geom_rd = line_rd.interpolate(dist)
            else:
                buffer_before = settings.DETAILED_LOCK_SPLICING_BUFFER_M
                buffer_after = settings.DETAILED_LOCK_SPLICING_BUFFER_M
        elif dropin["type"] == "lock":
            buffer_dist = obj.get("fairway_buffer_dist")
            if buffer_dist is None:
                max_len = 0.0
                for child in obj.get("locks", []):
                    for ch in child.get("chambers", []):
                        length_val = ch.get("dim_length") or ch.get("length")
                        if length_val:
                            max_len = max(max_len, float(length_val))
                buffer_dist = settings.SIMPLIFIED_LOCK_SPLICING_BUFFER_M
            buffer_before = buffer_dist
            buffer_after = buffer_dist
        elif dropin["type"] in ("terminal", "berth"):
            buffer_before = 0.0
            buffer_after = 0.0
        else:
            buffer_before = settings.BRIDGE_SPLICING_BUFFER_M
            buffer_after = settings.BRIDGE_SPLICING_BUFFER_M

        cuts.append(
            StructureCut(
                id=f"{dropin['type']}_{utils.stringify_id(obj.get('id', obj.get('Id')))}",
                geometry=geom_rd,
                projected_distance=dist,
                buffer_before=buffer_before,
                buffer_after=buffer_after,
            )
        )
    return cuts


def _yield_junction_nodes(all_features, line, is_start, is_end, start_junc, end_junc):
    if is_start and pd.notna(start_junc):
        node_id = utils.stringify_id(start_junc)
        all_features.append(
            {
                "type": "Feature",
                "geometry": mapping(Point(line.coords[0])),
                "properties": {
                    "id": node_id,
                    "feature_type": "node",
                    "node_type": "junction",
                    "node_id": node_id,
                },
            }
        )
    if is_end and pd.notna(end_junc):
        node_id = utils.stringify_id(end_junc)
        all_features.append(
            {
                "type": "Feature",
                "geometry": mapping(Point(line.coords[-1])),
                "properties": {
                    "id": node_id,
                    "feature_type": "node",
                    "node_type": "junction",
                    "node_id": node_id,
                },
            }
        )


def _assign_geom_wkt(dropins_list, dtype, did, key, wkt_str, sec_id=None):
    did_str = utils.stringify_id(did)
    for dropin in dropins_list:
        if (
            dropin["type"] == dtype
            and utils.stringify_id(dropin["obj"].get("id", dropin["obj"].get("Id")))
            == did_str
        ):
            if sec_id:
                if key not in dropin["obj"]:
                    dropin["obj"][key] = {}
                dropin["obj"][key][sec_id] = wkt_str
            else:
                dropin["obj"][key] = wkt_str
            break
