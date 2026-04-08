import logging
from typing import List, Dict, Set, Tuple, Optional, Any

import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, mapping
from tqdm import tqdm
from pyproj import Transformer

from fis.splicer import FairwaySplicer, StructureCut
from fis import settings, utils

logger = logging.getLogger(__name__)

_UTM_CRS_CACHE = {}
_TRANSFORMER_CACHE = {}


def _get_cached_utm_crs(geom: Any) -> str:
    """
    Cache UTM CRS estimation based on rounded coordinates to avoid redundant calls.
    """
    if geom is None or geom.is_empty:
        return "EPSG:32631"  # Fallback to UTM 31N

    # Use centroid rounded to 0.5 degrees (~50km) as cache key
    c = geom.centroid
    key = (round(c.x * 2) / 2, round(c.y * 2) / 2)
    if key not in _UTM_CRS_CACHE:
        _UTM_CRS_CACHE[key] = gpd.GeoSeries([geom], crs="EPSG:4326").estimate_utm_crs()
    return _UTM_CRS_CACHE[key]


def _get_transformer(target_crs: str) -> Transformer:
    if target_crs not in _TRANSFORMER_CACHE:
        _TRANSFORMER_CACHE[target_crs] = Transformer.from_crs(
            "EPSG:4326", target_crs, always_xy=True
        )
    return _TRANSFORMER_CACHE[target_crs]


def _reproject_geom(geom: Any, transformer: Transformer) -> Any:
    from shapely.ops import transform

    return transform(transformer.transform, geom)


def splice_fairways(
    sections: pd.DataFrame,
    dropins_by_section: Dict[Any, List[Dict]],
    embedded_bridges: Dict[str, Dict],
    mode: str = "detailed",
) -> List[Dict]:
    """
    Iterates over all fairway sections and splices them into sub-segments
    based on the structures (drop-ins) that lie upon them.
    """
    if sections is not None:
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

        for old, new in [
            ("start_junction_id", "StartJunctionId"),
            ("end_junction_id", "EndJunctionId"),
            ("StartJunctionId", "start_junction_id"),
            ("EndJunctionId", "end_junction_id"),
        ]:
            if old in sections.columns and new not in sections.columns:
                sections[new] = sections[old]

    all_features = []

    sections_gdf = _prepare_sections_gdf(sections)
    embedded_ids = {str(k) for k in embedded_bridges.keys()}

    # Performance: Pre-calculate string IDs for junctions and sections
    sections_gdf["sid_str"] = sections_gdf["id"].apply(utils.stringify_id)
    sections_gdf["sj_str"] = sections_gdf["StartJunctionId"].apply(utils.stringify_id)
    sections_gdf["ej_str"] = sections_gdf["EndJunctionId"].apply(utils.stringify_id)
    sections_gdf["fw_str"] = sections_gdf.get(
        "fairway_id", pd.Series([None] * len(sections_gdf))
    ).apply(utils.stringify_id)

    for _, sec in tqdm(
        sections_gdf.iterrows(),
        total=len(sections_gdf),
        desc="Splicing fairways",
        mininterval=2.0,
    ):
        line_geom = sec.geometry
        if not line_geom or line_geom.is_empty:
            continue

        sid = sec["sid_str"]
        dropins_on_sec = dropins_by_section.get(sid, [])

        # Pre-process dropins for this section into a lookup dictionary
        dropin_lookup = {}
        for d in dropins_on_sec:
            if "id_str" not in d:
                d["id_str"] = utils.stringify_id(d["obj"].get("id", d["obj"].get("Id")))
            dropin_lookup[(d["type"], d["id_str"])] = d

        if dropins_on_sec:
            logger.debug(
                "Splicing section %s with %d dropins", sid, len(dropins_on_sec)
            )

        visible_dropins = [
            d
            for d in dropins_on_sec
            if mode == "simplified" or not _is_embedded(d, embedded_ids)
        ]

        if not visible_dropins:
            _handle_clear_section(all_features, sec)
            continue

        _slice_section_with_dropins(
            all_features, sec, visible_dropins, dropin_lookup, mode=mode
        )

    # Performance Cleanup: Remove temporary attributes attached during splicing to allow Parquet export
    # These attributes (like geom objects) cannot be serialized by pyarrow in the object column.
    for dropins in dropins_by_section.values():
        for d in dropins:
            obj = d.get("obj", {})
            for attr in ["id_str", "anchor_geom", "opening_ids_str"]:
                if attr in d:
                    del d[attr]
                if attr in obj:
                    del obj[attr]

            # Deep cleanup for locks/chambers
            if d.get("type") == "lock":
                for lk in obj.get("locks", []):
                    for ch in lk.get("chambers", []):
                        if "geom_obj" in ch:
                            del ch["geom_obj"]

    return all_features


def _is_embedded(dropin: Dict, embedded_ids: Set[str]) -> bool:
    if dropin["type"] != "bridge":
        return False
    if "opening_ids_str" not in dropin:
        dropin["opening_ids_str"] = {str(op["id"]) for op in dropin["obj"]["openings"]}
    return not dropin["opening_ids_str"].isdisjoint(embedded_ids)


def _prepare_sections_gdf(sections: pd.DataFrame) -> gpd.GeoDataFrame:
    if sections is not None and "geometry" in sections.columns:
        sections = sections.copy()
        sections["geometry"] = sections["geometry"].apply(
            lambda x: wkt.loads(x) if isinstance(x, str) else x
        )
    return gpd.GeoDataFrame(sections, geometry="geometry", crs="EPSG:4326")


def _handle_clear_section(all_features, sec):
    sid = sec["sid_str"]
    fairway_id = sec["fw_str"]
    name = sec.get("name") or sec.get("Name") or sec.get("FairwayName")
    sj_str = sec["sj_str"]
    ej_str = sec["ej_str"]
    line_geom = sec.geometry

    utm_crs = _get_cached_utm_crs(line_geom)
    transformer = _get_transformer(utm_crs)
    line_rd = _reproject_geom(line_geom, transformer)

    all_features.append(
        {
            "type": "Feature",
            "geometry": mapping(line_geom),
            "properties": {
                "id": f"fairway_segment_section_{sid}",
                "feature_type": "fairway_segment",
                "segment_type": "fairway",
                "section_id": sid,
                "fairway_id": fairway_id,
                "name": name,
                "source_node": sj_str,
                "target_node": ej_str,
                "length_m": line_rd.length,
            },
        }
    )
    _yield_junction_nodes(all_features, line_geom, True, True, sj_str, ej_str)


def _slice_section_with_dropins(
    all_features, sec, visible_dropins, dropin_lookup, mode="detailed"
):
    line_geom = sec.geometry
    utm_crs = _get_cached_utm_crs(line_geom)
    transformer = _get_transformer(utm_crs)
    line_rd = _reproject_geom(line_geom, transformer)

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
        dropin_lookup,
    )

    _generate_spliced_features(all_features, sec, segments, dropin_lookup, utm_crs)


def _handle_consumed_junctions(
    all_features, sec, cuts, line_geom, utm_crs, total_length, dropin_lookup
):
    """
    Generate connecting edges if the structures consumed the start or end junctions.
    """
    if not cuts:
        return

    sec_id = sec["sid_str"]
    sj_str = sec["sj_str"]
    ej_str = sec["ej_str"]

    # Start junction
    if sj_str and sj_str != "None":
        first_cut = min(cuts, key=lambda c: c.projected_distance - c.buffer_before)
        if (
            first_cut.projected_distance - first_cut.buffer_before
            <= settings.SPLICING_JUNCTION_TOLERANCE_M
        ):
            dtype, did = first_cut.id.split("_", 1)
            if dtype not in ("terminal", "berth"):
                pt_4326 = Point(line_geom.coords[0])
                _assign_geom_wkt(
                    dropin_lookup,
                    dtype,
                    did,
                    "merge_points",
                    pt_4326.wkt,
                    sec_id=sec_id,
                )
                _assign_geom_wkt(
                    dropin_lookup,
                    dtype,
                    did,
                    "merge_nodes",
                    sj_str,
                    sec_id=sec_id,
                )
                _yield_junction_nodes(
                    all_features, line_geom, True, False, sj_str, None
                )

    # End junction
    if ej_str and ej_str != "None":
        last_cut = max(cuts, key=lambda c: c.projected_distance + c.buffer_after)
        if (
            last_cut.projected_distance + last_cut.buffer_after
            >= total_length - settings.SPLICING_JUNCTION_TOLERANCE_M
        ):
            dtype, did = last_cut.id.split("_", 1)
            if dtype not in ("terminal", "berth"):
                pt_4326 = Point(line_geom.coords[-1])
                _assign_geom_wkt(
                    dropin_lookup,
                    dtype,
                    did,
                    "split_points",
                    pt_4326.wkt,
                    sec_id=sec_id,
                )
                _assign_geom_wkt(
                    dropin_lookup,
                    dtype,
                    did,
                    "split_nodes",
                    ej_str,
                    sec_id=sec_id,
                )
                _yield_junction_nodes(
                    all_features, line_geom, False, True, None, ej_str
                )


def _generate_spliced_features(all_features, sec, segments, dropin_lookup, utm_crs):
    """
    Convert spliced segments into GeoJSON features.
    """
    sec_id = sec["sid_str"]
    fairway_id = sec["fw_str"]
    sj_str = sec["sj_str"]
    ej_str = sec["ej_str"]

    for i, segment in enumerate(segments):
        seg_4326 = None
        if segment.geometry:
            seg_4326 = (
                gpd.GeoSeries([segment.geometry], crs=utm_crs)
                .to_crs("EPSG:4326")
                .iloc[0]
            )

        # Use the start of the section geometry as a fallback
        ref_geom = seg_4326 if seg_4326 else Point(sec.geometry.coords[0])

        source_node, is_start_junc = _determine_source_node(
            segment,
            sj_str,
            dropin_lookup,
            ref_geom,
            sec_id,
        )
        target_node, is_end_junc = _determine_target_node(
            segment, ej_str, dropin_lookup, ref_geom, sec_id
        )

        if source_node == target_node:
            continue

        # Use ref_geom if segment.geometry is None (zero-length connection)
        geom_to_map = seg_4326 if seg_4326 else ref_geom

        all_features.append(
            {
                "type": "Feature",
                "geometry": mapping(geom_to_map),
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
                    "length_m": segment.geometry.length if segment.geometry else 0.0,
                },
            }
        )
        _yield_junction_nodes(
            all_features,
            geom_to_map,
            is_start_junc,
            is_end_junc,
            sj_str,
            ej_str,
        )


def _determine_source_node(
    segment: Any, sj_str: Optional[str], dropin_lookup: Dict, ref_geom: Any, sec_id: str
) -> Tuple[Optional[str], bool]:
    is_start = True
    node = sj_str
    if segment.source_structure_id:
        dtype, did = segment.source_structure_id.split("_", 1)

        # Determine the physical point for this connection
        conn_pt = (
            ref_geom if ref_geom.geom_type == "Point" else Point(ref_geom.coords[0])
        )

        if dtype in ("terminal", "berth"):
            node = f"{dtype}_{did}_connection"
            _assign_geom_wkt(
                dropin_lookup,
                dtype,
                did,
                "connection_geometry",
                conn_pt.wkt,
            )
        else:
            node = _get_assigned_node(dropin_lookup, dtype, did, "merge_nodes", sec_id)
            if not node:
                node = f"{dtype}_{did}_{sec_id}_merge"
                _assign_geom_wkt(
                    dropin_lookup, dtype, did, "merge_nodes", node, sec_id=sec_id
                )

            _assign_geom_wkt(
                dropin_lookup,
                dtype,
                did,
                "merge_points",
                conn_pt.wkt,
                sec_id=sec_id,
            )
        is_start = False
    return node, is_start


def _determine_target_node(
    segment: Any, ej_str: Optional[str], dropin_lookup: Dict, ref_geom: Any, sec_id: str
) -> Tuple[Optional[str], bool]:
    is_end = True
    node = ej_str
    if segment.target_structure_id:
        dtype, did = segment.target_structure_id.split("_", 1)

        # Determine the physical point for this connection
        conn_pt = (
            ref_geom if ref_geom.geom_type == "Point" else Point(ref_geom.coords[-1])
        )

        if dtype in ("terminal", "berth"):
            node = f"{dtype}_{did}_connection"
            _assign_geom_wkt(
                dropin_lookup,
                dtype,
                did,
                "connection_geometry",
                conn_pt.wkt,
            )
        else:
            node = _get_assigned_node(dropin_lookup, dtype, did, "split_nodes", sec_id)
            if not node:
                node = f"{dtype}_{did}_{sec_id}_split"
                _assign_geom_wkt(
                    dropin_lookup, dtype, did, "split_nodes", node, sec_id=sec_id
                )

            _assign_geom_wkt(
                dropin_lookup,
                dtype,
                did,
                "split_points",
                conn_pt.wkt,
                sec_id=sec_id,
            )
        is_end = False
    return node, is_end


def _get_assigned_node(dropin_lookup, dtype, did, key, sec_id):
    dropin = dropin_lookup.get((dtype, did))
    if dropin:
        return dropin["obj"].get(key, {}).get(sec_id)
    return None


def _generate_structure_cuts(
    line_rd: Any, visible_dropins: List[Dict], utm_crs: str, mode: str = "detailed"
) -> List[StructureCut]:
    cuts = []
    transformer = _get_transformer(utm_crs)
    for dropin in visible_dropins:
        obj = dropin["obj"]
        did_str = dropin["id_str"]

        if "anchor_geom" not in dropin:
            gv = obj.get("topological_anchor") or obj.get("geometry")
            if not gv:
                raise ValueError(f"Drop-in {dropin['type']} {did_str} has no geometry.")
            dropin["anchor_geom"] = wkt.loads(gv) if isinstance(gv, str) else gv

        geom_rd = _reproject_geom(dropin["anchor_geom"], transformer)
        if geom_rd.geom_type != "Point":
            geom_rd = geom_rd.centroid

        dist_to_center = line_rd.distance(geom_rd)
        if dist_to_center > 500.0:
            continue

        dist = line_rd.project(geom_rd)

        if dropin["type"] == "lock" and mode == "detailed":
            min_proj = float("inf")
            max_proj = float("-inf")
            valid = False
            for child in obj.get("locks", []):
                for ch in child.get("chambers", []):
                    if "geom_obj" not in ch:
                        gv = ch.get("route_geometry") or ch.get("geometry")
                        if gv:
                            ch["geom_obj"] = (
                                wkt.loads(gv) if isinstance(gv, str) else gv
                            )

                    if "geom_obj" in ch:
                        c_geom_rd = _reproject_geom(ch["geom_obj"], transformer)
                        coords = []
                        if c_geom_rd.geom_type == "Polygon":
                            coords = list(c_geom_rd.exterior.coords)
                        elif c_geom_rd.geom_type == "MultiPolygon":
                            for poly in c_geom_rd.geoms:
                                coords.extend(poly.exterior.coords)
                        elif c_geom_rd.geom_type == "LineString":
                            coords = list(c_geom_rd.coords)
                        elif c_geom_rd.geom_type == "MultiLineString":
                            for line in c_geom_rd.geoms:
                                coords.extend(line.coords)
                        elif c_geom_rd.geom_type == "Point":
                            coords = [c_geom_rd.coords[0]]
                        else:
                            try:
                                coords = list(c_geom_rd.coords)
                            except (AttributeError, NotImplementedError):
                                coords = []

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
                id=f"{dropin['type']}_{did_str}",
                geometry=geom_rd,
                projected_distance=dist,
                buffer_before=buffer_before,
                buffer_after=buffer_after,
            )
        )
    return cuts


def _yield_junction_nodes(all_features, line, is_start, is_end, sj_str, ej_str):
    if is_start and sj_str and sj_str != "None":
        all_features.append(
            {
                "type": "Feature",
                "geometry": mapping(Point(line.coords[0])),
                "properties": {
                    "id": sj_str,
                    "feature_type": "node",
                    "node_type": "junction",
                    "node_id": sj_str,
                },
            }
        )
    if is_end and ej_str and ej_str != "None":
        all_features.append(
            {
                "type": "Feature",
                "geometry": mapping(Point(line.coords[-1])),
                "properties": {
                    "id": ej_str,
                    "feature_type": "node",
                    "node_type": "junction",
                    "node_id": ej_str,
                },
            }
        )


def _assign_geom_wkt(dropin_lookup, dtype, did_str, key, wkt_str, sec_id=None):
    dropin = dropin_lookup.get((dtype, did_str))
    if dropin:
        if sec_id:
            if key not in dropin["obj"]:
                dropin["obj"][key] = {}
            dropin["obj"][key][sec_id] = wkt_str
        else:
            dropin["obj"][key] = wkt_str
