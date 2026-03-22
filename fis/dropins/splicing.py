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
    """
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
    for op in dropin["obj"].get("openings", []):
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

    for i, segment in enumerate(segments):
        seg_4326 = (
            gpd.GeoSeries([segment.geometry], crs=utm_crs).to_crs("EPSG:4326").iloc[0]
        )
        source_node, is_start_junc = _determine_source_node(
            segment, sec.get("StartJunctionId"), original_dropins_on_sec, seg_4326
        )
        target_node, is_end_junc = _determine_target_node(
            segment, sec.get("EndJunctionId"), original_dropins_on_sec, seg_4326
        )

        all_features.append(
            {
                "type": "Feature",
                "geometry": mapping(seg_4326),
                "properties": {
                    "id": f"fairway_segment_section_{utils.stringify_id(sec['id'])}_{i}",
                    "feature_type": "fairway_segment",
                    "segment_type": "clear"
                    if is_start_junc and is_end_junc
                    else "approach_or_exit",
                    "section_id": utils.stringify_id(sec["id"]),
                    "fairway_id": utils.stringify_id(sec.get("fairway_id")),
                    "name": sec.get("Name", sec.get("FairwayName")),
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
    segment: Any, start_junc: Any, dropins: List[Dict], seg_4326: Any
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
            node = f"{dtype}_{did}_merge"
            _assign_geom_wkt(dropins, dtype, did, "geometry_after_wkt", seg_4326.wkt)
        is_start = False
    return node, is_start


def _determine_target_node(
    segment: Any, end_junc: Any, dropins: List[Dict], seg_4326: Any
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
            node = f"{dtype}_{did}_split"
            _assign_geom_wkt(dropins, dtype, did, "geometry_before_wkt", seg_4326.wkt)
        is_end = False
    return node, is_end


def _generate_structure_cuts(
    line_rd: Any, dropins_on_sec: List[Dict], utm_crs: str, mode: str = "detailed"
) -> List[StructureCut]:
    cuts = []
    for dropin in dropins_on_sec:
        obj = dropin["obj"]
        geom_wkt = obj.get("geometry")
        if not geom_wkt:
            continue

        geom = wkt.loads(geom_wkt)
        geom_rd = gpd.GeoSeries([geom], crs="EPSG:4326").to_crs(utm_crs).iloc[0]
        if geom_rd.geom_type != "Point":
            geom_rd = geom_rd.centroid

        dist = line_rd.project(geom_rd)
        if dropin["type"] == "lock":
            buffer_dist = obj.get("fairway_buffer_dist")
            if buffer_dist is None or mode == "simplified":
                max_len = 0.0
                for child in obj.get("locks", []):
                    for ch in child.get("chambers", []):
                        if ch.get("length"):
                            max_len = max(max_len, float(ch["length"]))
                buffer_dist = (
                    settings.SIMPLIFIED_LOCK_SPLICING_BUFFER_M
                    if mode == "simplified"
                    else (max_len / 2.0) + settings.DETAILED_LOCK_SPLICING_BUFFER_M
                )
        elif dropin["type"] in ("terminal", "berth"):
            buffer_dist = 0.0
        else:
            buffer_dist = settings.BRIDGE_SPLICING_BUFFER_M

        cuts.append(
            StructureCut(
                id=f"{dropin['type']}_{utils.stringify_id(obj.get('id', obj.get('Id')))}",
                geometry=geom_rd,
                projected_distance=dist,
                buffer_distance=buffer_dist,
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


def _assign_geom_wkt(dropins_list, dtype, did, key, wkt_str):
    did_str = utils.stringify_id(did)
    for dropin in dropins_list:
        if (
            dropin["type"] == dtype
            and utils.stringify_id(dropin["obj"].get("id", dropin["obj"].get("Id")))
            == did_str
        ):
            dropin["obj"][key] = wkt_str
            break
