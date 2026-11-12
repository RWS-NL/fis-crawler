import logging
import pathlib
import json
import math
import pickle
import numpy as np
import pandas as pd
import geopandas as gpd
import networkx as nx
from scipy.spatial import KDTree
from shapely.geometry import Point
from collections import defaultdict
from dataclasses import dataclass
from dask.distributed import Client, LocalCluster


logger = logging.getLogger("fis.ivs.assign")

ZENODO_URL = (
    "https://zenodo.org/records/11191511/files/unlo-geocoded-v0.1.gpkg?download=1"
)


def is_valid(val):
    if val is None:
        return False
    if isinstance(val, float) and math.isnan(val):
        return False
    if str(val) == "nan":
        return False
    return True


def load_shiptypes(reference_dir: pathlib.Path) -> dict:
    """Loads and indexes the DTV ship types database."""
    dtv_path = reference_dir / "DTV_shiptypes_database.json"
    if not dtv_path.exists():
        raise FileNotFoundError(
            f"DTV shiptypes database not found at {dtv_path}. Run 'make download-reference' first."
        )

    with open(dtv_path, "r", encoding="utf-8") as f:
        ships = json.load(f)

    indexed = {}
    for ship in ships:
        rws_class = ship.get("RWS-class")
        if rws_class:
            indexed[rws_class.upper().strip()] = ship

    return indexed


def normalize_class_code(code: str) -> str:
    """Normalizes the IVS ship class code to match the DTV class naming convention."""
    if not isinstance(code, str):
        return ""
    code = code.upper().strip()
    if code.startswith("B0"):
        code = "BO" + code[2:]
    return code


def normalize_cemt(c: str) -> str:
    if not isinstance(c, str):
        return ""
    c = c.upper().strip().replace("_", "").replace("-", "")
    if c == "VA":
        return "VA"
    if c == "VB":
        return "VB"
    if c == "VIA":
        return "VIA"
    if c == "VIB":
        return "VIB"
    if c == "VIC":
        return "VIC"
    if c in ("IVA", "IVB"):
        return "IV"
    if c == "VIIA":
        return "VIC"
    return c


CEMT_SCALE = {
    "": 0,
    "0": 0,
    "I": 1,
    "II": 2,
    "III": 3,
    "IV": 4,
    "VA": 5,
    "VB": 6,
    "VIA": 7,
    "VIB": 8,
    "VIC": 9,
}


def get_ship_dimensions(
    sk_code: str, cargo_weight: float, capacity: float, dtv_db: dict
) -> dict:
    """Computes dynamic vessel dimensions (including loaded/empty draft interpolation)."""
    normalized = normalize_class_code(sk_code)
    ship = dtv_db.get(normalized)

    if not ship:
        return {
            "beam": 11.4,
            "length": 110.0,
            "height": 6.0,
            "draft": 2.5,
            "cemt": "VB",
        }

    beam = float(ship.get("Beam [m]", 11.4))
    length = float(ship.get("Length [m]", 110.0))

    h_avg = ship.get("Height average [m]")
    height = float(h_avg) if is_valid(h_avg) else 6.0

    d_loaded = ship.get("Draught loaded [m]")
    d_empty = ship.get("Draught empty [m]")
    d_avg = ship.get("Draught average [m]")

    loaded_draft = (
        float(d_loaded)
        if is_valid(d_loaded)
        else (float(d_avg) if is_valid(d_avg) else 3.5)
    )
    empty_draft = float(d_empty) if is_valid(d_empty) else (0.4 * loaded_draft)

    if capacity > 0 and cargo_weight > 0:
        ratio = cargo_weight / capacity
        ratio = max(0.0, min(1.0, ratio))
        draft = empty_draft + ratio * (loaded_draft - empty_draft)
    else:
        draft = empty_draft

    ship_cemt = normalize_cemt(ship.get("CEMT-class", ""))

    return {
        "beam": beam,
        "length": length,
        "height": height,
        "draft": draft,
        "cemt": ship_cemt,
    }


# Canonical redirects for FIS fairway_section_id values overwritten in the merged
# graph by parallel sections between the same junctions or harbour splicing (#214).
FIS_SECTION_ID_REDIRECTS: dict[str, str] = {
    "12840": "47304",  # Prinses Irenesluizen
    "38667": "43326",  # Sluiskolk Hartelsluis & Hartelbrug
    "7426": "49924",  # Oost/Westkolk Prinses Marijkesluis & brug
    "28025": "39670",  # Sluiskolk Algerasluis & Algerabrug
    "30984601": "30985410",  # Brug sluis Heumen (keersluis)
    "54334": "30985692",  # Nieuwe Langebrug
    "35228475": "35228494",  # Hoeksterpoortbrug (oost)
    "36567535": "36567845",  # Brug in de Himdyk & Smellebrêge
    "59275111": "59275532",  # Brug Molenspoor
    "16765": "39904",  # Van Brienenoordbrug (Zuiddiepje), De Arend en de Zeemeeuwbrug, Ben Schopbrug
}


def get_edge_key(d: dict, u: str | None = None, v: str | None = None) -> str | None:
    """Extracts the lookup ID key from a merged graph edge.

    Falls back to a deterministic synthetic key ``virtual_<u>_<v>`` for
    non-harbour connector edges (such as EURIS or FIS-EURIS border links)
    that lack ``fis_id`` and ``code`` (#214).
    """
    val = d.get("fis_id") or d.get("code")
    if is_valid(val):
        return str(val)
    if (
        u is not None
        and v is not None
        and d.get("segment_type") != "harbour_access"
        and d.get("data_source") != "vinharbour"
    ):
        a, b = sorted((str(u), str(v)))
        return f"virtual_{a}_{b}"
    return None


def _resolve_fis_section_id(sid: object, name: str) -> str | None:
    """Resolves a FIS dropin fairway_section_id to its canonical graph fis_id."""
    if "Irenesluis" in name or "Irenesluizen" in name:
        return "47304"
    if not is_valid(sid):
        return None
    sid_str = str(int(float(sid)))
    return FIS_SECTION_ID_REDIRECTS.get(sid_str, sid_str)


def build_edge_structures_lookup() -> dict:
    """
    Builds a unified lookup mapping FIS fairway section IDs and EURIS fairway section codes
    to their respective lock chambers and bridge openings from the detailed databases.
    """
    fis_ch_path = pathlib.Path("output/dropins-fis-detailed/chambers.geoparquet")
    fis_op_path = pathlib.Path("output/dropins-fis-detailed/openings.geoparquet")
    eur_edges_path = pathlib.Path("output/dropins-euris-detailed/edges.geoparquet")
    eur_ch_path = pathlib.Path("output/dropins-euris-detailed/chambers.geoparquet")
    eur_op_path = pathlib.Path("output/dropins-euris-detailed/openings.geoparquet")

    # Fail fast if required detailed dropins data files are missing
    for path in (fis_ch_path, fis_op_path, eur_edges_path, eur_ch_path, eur_op_path):
        if not path.exists():
            raise FileNotFoundError(
                f"Required detailed dropin file {path} is missing. Ensure schematize steps ran successfully."
            )

    lookup = defaultdict(lambda: {"chambers": [], "openings": []})

    # Load FIS chambers and openings
    for path, bucket in ((fis_ch_path, "chambers"), (fis_op_path, "openings")):
        df_fis = pd.read_parquet(path)
        for _, r in df_fis.iterrows():
            sid = _resolve_fis_section_id(
                r.get("fairway_section_id"), str(r.get("name", ""))
            )
            if sid is not None:
                lookup[sid][bucket].append(r.to_dict())

    # Load EURIS edges, chambers, and openings
    df_eur_edges = pd.read_parquet(eur_edges_path)
    euris_sec_locks = defaultdict(set)
    euris_sec_bridges = defaultdict(set)
    for _, r in df_eur_edges.iterrows():
        sid = r.get("section_id")
        lid = r.get("lock_id")
        bid = r.get("bridge_id")
        if is_valid(sid):
            if is_valid(lid):
                euris_sec_locks[str(sid)].add(str(lid))
            if is_valid(bid):
                euris_sec_bridges[str(sid)].add(str(bid))

    df_eur_ch = pd.read_parquet(eur_ch_path)
    euris_ch_by_lock = defaultdict(list)
    for _, r in df_eur_ch.iterrows():
        lid = r.get("lock_id")
        if is_valid(lid):
            euris_ch_by_lock[str(lid)].append(r.to_dict())

    df_eur_op = pd.read_parquet(eur_op_path)
    euris_op_by_bridge = defaultdict(list)
    for _, r in df_eur_op.iterrows():
        bid = r.get("bridge_id")
        if is_valid(bid):
            euris_op_by_bridge[str(bid)].append(r.to_dict())

    for sid, lids in euris_sec_locks.items():
        for lid in lids:
            lookup[sid]["chambers"].extend(euris_ch_by_lock[lid])

    for sid, bids in euris_sec_bridges.items():
        for bid in bids:
            lookup[sid]["openings"].extend(euris_op_by_bridge[bid])

    return lookup


PENALTY_PHYSICAL = 1000.0
PENALTY_SEA_ROUTE = 100000.0


def _get_chamber_width(ch: dict) -> float | None:
    val = ch.get("dim_gate_width", ch.get("width"))
    if is_valid(val):
        try:
            return float(val)
        except ValueError, TypeError:
            pass
    return None


def _get_chamber_length(ch: dict) -> float | None:
    val = ch.get("dim_usable_length", ch.get("length"))
    if is_valid(val):
        try:
            return float(val)
        except ValueError, TypeError:
            pass
    return None


def _get_opening_width(op: dict) -> float | None:
    val = op.get("dim_structural_width", op.get("width_convoy", op.get("width")))
    if is_valid(val):
        try:
            return float(val)
        except ValueError, TypeError:
            pass
    return None


def _chamber_fits(ch: dict, beam: float, length: float) -> bool:
    """Checks if a vessel fits in a single lock chamber (both beam and length)."""
    ch_w = _get_chamber_width(ch)
    ch_l = _get_chamber_length(ch)
    return (ch_w is None or ch_w >= beam) and (ch_l is None or ch_l >= length)


def _opening_fits(op: dict, beam: float) -> bool:
    """Checks if a vessel fits through a bridge opening."""
    w = _get_opening_width(op)
    return w is None or w >= beam


def _group_chambers_by_lock(chambers: list) -> dict[str, list]:
    """Groups lock chambers by structure identifier (lock_id, parent_id, or name)."""
    groups = defaultdict(list)
    for ch in chambers:
        lid = (
            ch.get("lock_id")
            or ch.get("parent_id")
            or ch.get("name")
            or ch.get("id")
            or "lock"
        )
        groups[str(lid)].append(ch)
    return groups


def _group_openings_by_bridge(openings: list) -> dict[str, list]:
    """Groups bridge openings by structure identifier (bridge_id, parent_id, or name)."""
    groups = defaultdict(list)
    for op in openings:
        bid = (
            op.get("bridge_id")
            or op.get("parent_id")
            or op.get("name")
            or op.get("id")
            or "bridge"
        )
        groups[str(bid)].append(op)
    return groups


def _is_recreation_chamber(ch: dict) -> bool:
    """Checks if a chamber is designated for pleasure craft / recreation."""
    name = f"{ch.get('name', '')} {ch.get('lock_name', '')}".lower()
    return any(term in name for term in ("jacht", "recreatie", "pleziervaart"))


def _is_recreation_lock(lock_chambers: list) -> bool:
    """
    Checks if a lock complex is exclusively dedicated to pleasure craft / recreation
    (all chambers are recreation chambers) which should not be used by commercial freight vessels.
    """
    return bool(lock_chambers) and all(_is_recreation_chamber(c) for c in lock_chambers)


def _find_min_width(chambers, openings, d_edge):
    """
    Return (min_width, limiting_struct) across lock chambers, bridge openings, and the fairway edge.
    For each individual lock and bridge, the widest passable opening/chamber is used (parallel options).
    Across multiple structures in series on the section and the fairway edge itself,
    the bottleneck (minimum width) is returned.
    """
    min_width = 999.0
    limiting_struct = None

    for lid, l_chambers in _group_chambers_by_lock(chambers).items():
        comm_chambers = [c for c in l_chambers if not _is_recreation_chamber(c)]
        candidate_chambers = comm_chambers if comm_chambers else l_chambers
        best_ch = max(candidate_chambers, key=lambda c: _get_chamber_width(c) or 0.0)
        w = _get_chamber_width(best_ch)
        if w is not None and w < min_width:
            min_width = w
            limiting_struct = (
                best_ch.get("id"),
                best_ch.get("name", "Lock Chamber"),
                "lock",
            )

    for bid, b_openings in _group_openings_by_bridge(openings).items():
        best_op = max(b_openings, key=lambda o: _get_opening_width(o) or 0.0)
        w = _get_opening_width(best_op)
        if w is not None and w < min_width:
            min_width = w
            limiting_struct = (
                best_op.get("id"),
                best_op.get("name", "Bridge Opening"),
                "bridge",
            )

    edge_w = d_edge.get("dim_structural_width", d_edge.get("mwidthcm"))
    if is_valid(edge_w):
        edge_w_m = float(edge_w) / 100.0 if "mwidthcm" in d_edge else float(edge_w)
        if edge_w_m < min_width:
            min_width = edge_w_m
            limiting_struct = (
                get_edge_key(d_edge),
                d_edge.get("name", "Fairway"),
                "fairway",
            )

    return min_width, limiting_struct


def _find_min_length(chambers):
    """
    Return (min_length, limiting_chamber) across lock structures.
    For each lock complex, the longest chamber is used (parallel options).
    Across multiple locks in series on the section, the bottleneck (minimum length) is returned.
    """
    min_len = 999.0
    limiting_ch = None

    for lid, l_chambers in _group_chambers_by_lock(chambers).items():
        comm_chambers = [c for c in l_chambers if not _is_recreation_chamber(c)]
        candidate_chambers = comm_chambers if comm_chambers else l_chambers
        best_ch = max(candidate_chambers, key=lambda c: _get_chamber_length(c) or 0.0)
        ch_l = _get_chamber_length(best_ch)
        if ch_l is not None and ch_l < min_len:
            min_len = ch_l
            limiting_ch = (best_ch.get("id"), best_ch.get("name", "Lock Chamber"))

    return min_len, limiting_ch


@dataclass
class AnnotatedPenalty:
    """Represents a penalty applied to an edge traversal with its origin and context."""

    source: str  # "lock", "bridge", "fairway", "sea", "cemt"
    violation_type: str  # "beam", "length", "recreation", "sea", "cemt"
    cost: float  # e.g. PENALTY_PHYSICAL (1000.0) or PENALTY_SEA_ROUTE (100000.0)
    struct_id: str | None = None
    struct_name: str | None = None
    ship_value: float | str | None = None
    limit_value: float | str | None = None
    description: str = ""

    def __getitem__(self, item: str):
        if item == "type":
            return self.violation_type
        return getattr(self, item)

    def get(self, item: str, default=None):
        try:
            return self[item]
        except AttributeError, KeyError:
            return default

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "type": self.violation_type,
            "cost": self.cost,
            "struct_id": self.struct_id,
            "struct_name": self.struct_name,
            "ship_value": self.ship_value,
            "limit_value": self.limit_value,
            "description": self.description,
        }


def calc_lock_penalties(
    chambers: list[dict], ship_dims: dict
) -> list[AnnotatedPenalty]:
    """
    Evaluates lock structures for commercial traffic.
    - Classifies chambers individually. If a lock complex only has recreation chambers,
      a recreation penalty is applied. If it contains commercial chambers, commercial
      traffic uses those without recreation penalty.
    - A vessel passes if at least one chamber accommodates both beam and length (issue #212).
    - If no commercial chamber fits, a physical penalty is applied for the bottleneck dimensions.
    """
    penalties: list[AnnotatedPenalty] = []
    ship_beam = ship_dims.get("beam", 0.0)
    ship_length = ship_dims.get("length", 0.0)

    for lid, l_chambers in _group_chambers_by_lock(chambers).items():
        comm_chambers = [c for c in l_chambers if not _is_recreation_chamber(c)]

        if not comm_chambers:
            first = l_chambers[0]
            penalties.append(
                AnnotatedPenalty(
                    source="lock",
                    violation_type="recreation",
                    cost=PENALTY_PHYSICAL,
                    struct_id=first.get("id"),
                    struct_name=first.get("lock_name")
                    or first.get("name", "Recreation Lock"),
                    ship_value=ship_beam,
                    limit_value=0.0,
                    description=f"Recreation lock not permitted for commercial traffic: {first.get('name')}",
                )
            )
            continue

        if any(_chamber_fits(c, ship_beam, ship_length) for c in comm_chambers):
            continue

        best_ch = max(
            comm_chambers,
            key=lambda c: (
                _get_chamber_width(c) or 0.0,
                _get_chamber_length(c) or 0.0,
            ),
        )
        ch_w = _get_chamber_width(best_ch) or 0.0
        ch_l = _get_chamber_length(best_ch) or 0.0

        beam_failed = ship_beam > ch_w > 0.0
        length_failed = ship_length > ch_l > 0.0
        if not beam_failed and not length_failed:
            beam_failed = True
            length_failed = True

        if beam_failed:
            penalties.append(
                AnnotatedPenalty(
                    source="lock",
                    violation_type="beam",
                    cost=PENALTY_PHYSICAL,
                    struct_id=best_ch.get("id"),
                    struct_name=best_ch.get("name", "Lock Chamber"),
                    ship_value=ship_beam,
                    limit_value=ch_w,
                    description=f"Ship beam {ship_beam}m exceeds lock width {ch_w}m ({best_ch.get('name')})",
                )
            )
        if length_failed:
            penalties.append(
                AnnotatedPenalty(
                    source="lock",
                    violation_type="length",
                    cost=PENALTY_PHYSICAL,
                    struct_id=best_ch.get("id"),
                    struct_name=best_ch.get("name", "Lock Chamber"),
                    ship_value=ship_length,
                    limit_value=ch_l,
                    description=f"Ship length {ship_length}m exceeds lock length {ch_l}m ({best_ch.get('name')})",
                )
            )

    return penalties


def calc_bridge_penalties(
    openings: list[dict], ship_beam: float
) -> list[AnnotatedPenalty]:
    """Evaluates bridge structures: passes if at least one opening fits vessel beam."""
    penalties: list[AnnotatedPenalty] = []

    for bid, b_openings in _group_openings_by_bridge(openings).items():
        if any(_opening_fits(o, ship_beam) for o in b_openings):
            continue

        best_op = max(b_openings, key=lambda o: _get_opening_width(o) or 0.0)
        op_w = _get_opening_width(best_op) or 0.0
        penalties.append(
            AnnotatedPenalty(
                source="bridge",
                violation_type="beam",
                cost=PENALTY_PHYSICAL,
                struct_id=best_op.get("id"),
                struct_name=best_op.get("name", "Bridge Opening"),
                ship_value=ship_beam,
                limit_value=op_w,
                description=f"Ship beam {ship_beam}m exceeds bridge opening width {op_w}m ({best_op.get('name')})",
            )
        )

    return penalties


def calc_fairway_penalties(d_edge: dict, ship_beam: float) -> list[AnnotatedPenalty]:
    """Evaluates fairway edge structural width."""
    edge_w = d_edge.get("dim_structural_width", d_edge.get("mwidthcm"))
    if not is_valid(edge_w):
        return []

    edge_w_m = float(edge_w) / 100.0 if "mwidthcm" in d_edge else float(edge_w)
    if ship_beam > edge_w_m > 0.0:
        return [
            AnnotatedPenalty(
                source="fairway",
                violation_type="beam",
                cost=PENALTY_PHYSICAL,
                struct_id=get_edge_key(d_edge),
                struct_name=d_edge.get("name", "Fairway"),
                ship_value=ship_beam,
                limit_value=edge_w_m,
                description=f"Ship beam {ship_beam}m exceeds fairway width {edge_w_m}m",
            )
        ]
    return []


def calc_sea_penalties(d_edge: dict) -> list[AnnotatedPenalty]:
    """Evaluates sea route restriction."""
    water_name = str(d_edge.get("water_name", "")).strip().lower()
    if "noordzee" in water_name or "sea" in water_name:
        return [
            AnnotatedPenalty(
                source="sea",
                violation_type="sea",
                cost=PENALTY_SEA_ROUTE,
                struct_id=get_edge_key(d_edge),
                struct_name=d_edge.get("name", "Sea Route"),
                ship_value=1.0,
                limit_value=0.0,
                description="Sea route restricted for inland navigation",
            )
        ]
    return []


def calc_cemt_penalties(d_edge: dict, ship_dims: dict) -> list[AnnotatedPenalty]:
    """Evaluates fairway CEMT class against ship CEMT class."""
    edge_cemt = d_edge.get("cemt_class")
    if is_valid(edge_cemt) and ship_dims.get("cemt"):
        norm_edge = normalize_cemt(str(edge_cemt))
        norm_ship = ship_dims["cemt"]
        scale_edge = CEMT_SCALE.get(norm_edge, 0)
        scale_ship = CEMT_SCALE.get(norm_ship, 0)
        if scale_ship > scale_edge and scale_edge > 0:
            return [
                AnnotatedPenalty(
                    source="cemt",
                    violation_type="cemt",
                    cost=PENALTY_PHYSICAL,
                    struct_id=get_edge_key(d_edge),
                    struct_name=d_edge.get("name", "CEMT Class Constraint"),
                    ship_value=norm_ship,
                    limit_value=norm_edge,
                    description=f"Ship CEMT class {norm_ship} exceeds fairway class {norm_edge}",
                )
            ]
    return []


def get_edge_constraints(
    d_edge: dict, struct_data: dict, ship_dims: dict
) -> tuple[float, list[AnnotatedPenalty], dict[str, float]]:
    """
    Evaluates edge constraints and accumulates annotated penalties across all sources.

    Returns:
        total_penalty: Total sum of all soft constraint penalty costs.
        penalties: List of AnnotatedPenalty instances detailing each violation and source.
        by_source: Dictionary with aggregated penalty costs per source.
    """
    ship_beam = ship_dims.get("beam", 0.0)

    penalties: list[AnnotatedPenalty] = []
    penalties.extend(calc_lock_penalties(struct_data.get("chambers", []), ship_dims))
    penalties.extend(calc_bridge_penalties(struct_data.get("openings", []), ship_beam))
    penalties.extend(calc_fairway_penalties(d_edge, ship_beam))
    penalties.extend(calc_sea_penalties(d_edge))
    penalties.extend(calc_cemt_penalties(d_edge, ship_dims))

    by_source = {
        "lock": 0.0,
        "bridge": 0.0,
        "fairway": 0.0,
        "sea": 0.0,
        "cemt": 0.0,
    }
    for p in penalties:
        by_source[p.source] = by_source.get(p.source, 0.0) + p.cost

    total_cost = sum(p.cost for p in penalties)
    by_source["total"] = total_cost

    return total_cost, penalties, by_source


def check_edge_constraints_soft(d_edge: dict, struct_data: dict, ship_dims: dict):
    """
    Backward-compatible wrapper evaluating physical constraints of the edge.
    Returns (penalties_by_source_dict, list_of_annotated_penalties).
    """
    _, penalties, by_source = get_edge_constraints(d_edge, struct_data, ship_dims)
    return by_source, penalties


def calc_travel_time(d_edge: dict) -> float:
    """Calculates free-flow travel time in hours from edge length and speed limit."""
    length_km = float(d_edge["length_m"]) / 1000.0

    speed_km_h = 10.0
    for k in [
        "maxspeed_up",
        "maxspeed_down",
        "maxspeed",
        "calspeed_up",
        "calspeed_down",
    ]:
        if is_valid(d_edge.get(k)):
            if isinstance(d_edge[k], str):
                import re

                m = re.search(r"[-+]?\d*\.\d+|\d+", d_edge[k])
                if m:
                    val = float(m.group(0))
                    if val > 0.0:
                        speed_km_h = val
                        break
            else:
                val = float(d_edge[k])
                if val > 0.0:
                    speed_km_h = val
                    break

    speed_km_h = max(1.0, speed_km_h)
    return length_km / speed_km_h


def calc_lock_delay(chambers: list[dict], ship_dims: dict) -> float:
    """
    Calculates lock passage delay in hours for an edge.
    Count 1 passage duration per lock complex (parallel chambers are options, not additive).
    Excludes recreation-only chambers for commercial vessels if commercial chambers exist.

    Future Kooman Method integration points:
    - Step 2 (Kooman lock capacity): calculate complex capacity C based on chamber dimensions,
      leveling time, door operation and cycle time (C. Kooman & P.A. De Bruijn).
    - Step 3 (Kooman I/C delay): dynamic waiting time W(rho) from intensity/capacity ratio
      rho = I / C using queuing theory.
    """
    lock_delay = 0.0
    ship_beam = ship_dims.get("beam", 0.0)
    ship_length = ship_dims.get("length", 0.0)

    for lid, l_chambers in _group_chambers_by_lock(chambers).items():
        comm_chambers = [c for c in l_chambers if not _is_recreation_chamber(c)]
        candidate_chambers = comm_chambers if comm_chambers else l_chambers

        fitting = [
            ch for ch in candidate_chambers if _chamber_fits(ch, ship_beam, ship_length)
        ]
        chosen_ch = (
            fitting[0]
            if fitting
            else max(candidate_chambers, key=lambda c: _get_chamber_width(c) or 0.0)
        )
        passage_m = chosen_ch.get("passage_duration_m", 30.0)
        if not is_valid(passage_m):
            passage_m = 30.0
        lock_delay += float(passage_m) / 60.0

    return lock_delay


def get_edge_weight_soft(d_edge: dict, struct_data: dict, ship_dims: dict) -> float:
    """
    Calculates Dijkstra cost: travel time + lock delays + soft constraint penalties.
    """
    travel_time = calc_travel_time(d_edge)
    lock_delay = calc_lock_delay(struct_data.get("chambers", []), ship_dims)
    penalty_cost, _, _ = get_edge_constraints(d_edge, struct_data, ship_dims)
    return travel_time + lock_delay + penalty_cost


def route_batch_voyages_dask(
    batch_df: pd.DataFrame, dtv_db: dict, lookup: dict, G_merged: nx.Graph
) -> list:
    """Task executed on a Dask worker to route a batch of voyage groups locally."""
    batch_results = []

    for _, row in batch_df.iterrows():
        origin_node = row["origin_node"]
        destination_node = row["destination_node"]

        if not origin_node or not destination_node:
            batch_results.append({"status": "geocode_fail"})
            continue

        avg_cargo = (row["cargo_weight"] / 1000.0) / row["trips"]
        ship_dims = get_ship_dimensions(
            row["sk_code"], avg_cargo, row["vessel_capacity"], dtv_db
        )

        def weight_func(u, v, d):
            edge_id = get_edge_key(d, u, v)
            struct_data = lookup.get(edge_id, {"chambers": [], "openings": []})
            return get_edge_weight_soft(d, struct_data, ship_dims)

        try:
            path = nx.shortest_path(
                G_merged, origin_node, destination_node, weight=weight_func
            )

            edges_traversed = []
            violations_log = []

            for u, v in zip(path[:-1], path[1:]):
                d = G_merged[u][v]
                edge_id = get_edge_key(d, u, v)
                if edge_id:
                    struct_data = lookup.get(edge_id, {"chambers": [], "openings": []})
                    pen_dict, violations = check_edge_constraints_soft(
                        d, struct_data, ship_dims
                    )
                    edges_traversed.append({"edge_id": edge_id, "penalties": pen_dict})
                    for viol in violations:
                        violations_log.append(
                            {
                                "edge_id": edge_id,
                                "type": viol["type"],
                                "ship_class": row["sk_code"],
                                "ship_value": viol["ship_value"],
                                "limit_value": viol["limit_value"],
                                "struct_id": viol["struct_id"],
                                "struct_name": viol["struct_name"],
                                "trips": int(row["trips"]),
                                "cargo_weight": float(row["cargo_weight"]),
                            }
                        )

            batch_results.append(
                {
                    "status": "success",
                    "unlo_herkomst": row["unlo_herkomst"],
                    "unlo_bestemming": row["unlo_bestemming"],
                    "sk_code": row["sk_code"],
                    "nstr_nw": row["nstr_nw"],
                    "path": edges_traversed,
                    "path_nodes": path,
                    "violations": violations_log,
                    "trips": int(row["trips"]),
                    "cargo_weight": float(row["cargo_weight"]),
                }
            )
        except nx.NetworkXNoPath, nx.NodeNotFound:
            batch_results.append({"status": "no_path"})

    return batch_results


def load_unlocode_coordinates(reference_dir: pathlib.Path) -> dict:
    """Downloads and loads Zenodo coordinates reference data."""
    zenodo_path = reference_dir / "unlo-geocoded-v0.1.gpkg"
    if not zenodo_path.exists():
        raise FileNotFoundError(
            f"Zenodo reference UN/LOCODE data not found at {zenodo_path}. Run 'make download-reference' first."
        )
    zenodo_gdf = gpd.read_file(zenodo_path)
    zenodo_gdf["key"] = zenodo_gdf["country_code"] + zenodo_gdf["location_code"]

    # Correct geocoding error for Neeltje Jans (NLNTJ) in Zenodo reference
    zenodo_gdf.loc[zenodo_gdf["key"] == "NLNTJ", "geometry"] = Point(3.7115, 51.6391)

    return {
        row["key"]: row["geometry"]
        for _, row in zenodo_gdf.iterrows()
        if row["geometry"]
    }


def build_locode_node_lookup(G_merged: nx.Graph) -> dict:
    """Builds a lookup mapping locode strings and prefixes to graph nodes."""
    node_locode_map = defaultdict(list)
    for n_id, n_data in G_merged.nodes(data=True):
        loc = n_data.get("locode")
        if is_valid(loc):
            loc_str = str(loc).strip().upper()
            node_locode_map[loc_str].append((n_id, n_data.get("node_type", "")))
            if len(loc_str) >= 5:
                prefix = loc_str[:5]
                node_locode_map[prefix].append((n_id, n_data.get("node_type", "")))
    return node_locode_map


# Key commercial fairway junction nodes for major inland ports (#213)
# Explicitly pins high-volume commercial ports to unhindered CEMT fairway junctions
# to avoid geocoding into shallow, narrow, or restricted harbour/city basins.
KEY_PORT_FAIRWAY_NODES: dict[str, str] = {
    # Rotterdam (Nieuwe Maas / Waalhaven / Eemhaven junction, CEMT VI_c)
    "NLRTM": "FIS_8863496",
    # Amsterdam (Buiten-IJ / Amsterdam-Rijnkanaal junction, CEMT VI_b)
    "NLAMS": "FIS_8864384",
    # Antwerpen (Westerschelde / Rede Antwerpen fairway, CEMT VI_c)
    "BEANR": "FIS_35228495",
    # Duisburg (Rhein / Ruhr junction Hafenmund, CEMT VI_b)
    "DEDUI": "EURIS_DE_J1140",
    # Lelystad (Markermeer / IJsselmeer approach, CEMT VI_a)
    "NLLEY": "FIS_8865470",
    # Gent (Kanaal Gent-Terneuzen commercial corridor, CEMT VI_b)
    "BEGNE": "FIS_35228492",
    # Terneuzen (Kanaal Gent-Terneuzen / Westerschelde approach, CEMT VI_b)
    "NLTNZ": "FIS_8867489",
    # Dordrecht (Beneden-Merwede / Oude Maas junction, CEMT VI_c)
    "NLDOR": "FIS_8867986",
}


def geocode_unlocode(
    unlocode: str,
    zenodo_coords: dict,
    node_locode_map: dict,
    tree: KDTree,
    node_ids: list,
    G_merged: nx.Graph,
) -> str | None:
    """Matches a single UN/LOCODE to the nearest suitable graph node.

    Major commercial ports defined in `KEY_PORT_FAIRWAY_NODES` are mapped directly to
    designated high-capacity fairway junction nodes if present in `G_merged` (#213).
    For other locations, the lookup first prefers harbour or terminal nodes tagged with
    the UN/LOCODE, falling back to spatial nearest-neighbour matching via KDTree.
    """
    unlocode = str(unlocode).strip().upper()
    if unlocode in KEY_PORT_FAIRWAY_NODES:
        pinned_node = KEY_PORT_FAIRWAY_NODES[unlocode]
        if pinned_node in G_merged:
            return pinned_node

    geom = zenodo_coords.get(unlocode)
    if unlocode in node_locode_map:
        candidates = node_locode_map[unlocode]
        best_node = None
        min_score = float("inf")
        for n_id, ntype in candidates:
            n_geom = G_merged.nodes[n_id].get("geometry")
            if n_geom and hasattr(n_geom, "x"):
                dist = 0.0
                if geom:
                    dist = (n_geom.x - geom.x) ** 2 + (n_geom.y - geom.y) ** 2
                pref = 2
                if ntype == "harbour":
                    pref = 0
                elif ntype == "terminal":
                    pref = 1
                score = pref * 10.0 + dist
                if score < min_score:
                    min_score = score
                    best_node = n_id
        if best_node:
            return best_node
    if geom:
        dist, idx = tree.query((geom.x, geom.y))
        return node_ids[idx]
    return None


def load_voyages(
    ivs_dir: pathlib.Path, year: int | None = None, month: int | None = None
) -> pd.DataFrame:
    """Reads the raw parquet partition(s). If year/month are specified, loads that partition; otherwise loads all parquets under ivs_dir."""
    if year is not None and month is not None:
        ivs_file = ivs_dir / f"year={year}" / f"month={month:02d}" / "part.0.parquet"
        if not ivs_file.exists():
            fallback_files = list(
                ivs_dir.glob(f"year={year}/month={month:02d}/*.parquet")
            )
            if not fallback_files:
                raise FileNotFoundError(f"IVS data not found at {ivs_file}")
            ivs_file = fallback_files[0]
        logger.info(f"Reading IVS dataset from {ivs_file}...")
        return pd.read_parquet(ivs_file)
    else:
        parquet_files = list(ivs_dir.glob("**/*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No IVS parquet datasets found in {ivs_dir}")
        logger.info(
            f"Reading IVS datasets from {len(parquet_files)} files under {ivs_dir}..."
        )
        dfs = [pd.read_parquet(f) for f in parquet_files]
        return pd.concat(dfs, ignore_index=True)


def group_voyages(df: pd.DataFrame) -> pd.DataFrame:
    """Groups and aggregates voyages by origin, destination, vessel class, and commodity."""
    df = df.dropna(subset=["unlo_herkomst", "unlo_bestemming", "sk_code"])
    return (
        df.groupby(
            ["unlo_herkomst", "unlo_bestemming", "sk_code", "nstr_nw"], dropna=False
        )
        .agg(
            cargo_weight=("v38_vervoerd_gewicht", "sum"),
            vessel_capacity=("v18_laadvermogen", "mean"),
            trips=("v05_06_begindt_evenement", "count"),
        )
        .reset_index()
    )


def geocode_voyage_groups(
    voyage_groups: pd.DataFrame,
    zenodo_coords: dict,
    node_locode_map: dict,
    tree: KDTree,
    node_ids: list,
    G_merged: nx.Graph,
) -> pd.DataFrame:
    """Populates origin_node and destination_node columns for all voyage groups."""
    o_nodes = []
    d_nodes = []
    for _, row in voyage_groups.iterrows():
        o_nodes.append(
            geocode_unlocode(
                row["unlo_herkomst"],
                zenodo_coords,
                node_locode_map,
                tree,
                node_ids,
                G_merged,
            )
        )
        d_nodes.append(
            geocode_unlocode(
                row["unlo_bestemming"],
                zenodo_coords,
                node_locode_map,
                tree,
                node_ids,
                G_merged,
            )
        )
    voyage_groups["origin_node"] = o_nodes
    voyage_groups["destination_node"] = d_nodes
    return voyage_groups


def run_parallel_routing(
    voyage_groups: pd.DataFrame, G_merged: nx.Graph, lookup: dict, dtv_db: dict
) -> list:
    """Initializes Dask cluster, scatters large variables, and submits parallel batch routing."""
    chunk_size = 1000
    chunks = [
        voyage_groups.iloc[i : i + chunk_size].copy()
        for i in range(0, len(voyage_groups), chunk_size)
    ]

    logger.info("Initializing Dask LocalCluster...")
    cluster = LocalCluster(n_workers=4, threads_per_worker=1)
    client = Client(cluster)
    logger.info(f"Dask dashboard available at: {client.dashboard_link}")

    logger.info("Scattering large database variables to workers...")
    [dtv_db_future, lookup_future, G_merged_future] = client.scatter(
        [dtv_db, lookup, G_merged], broadcast=True
    )

    logger.info(f"Submitting {len(chunks)} batch routing tasks to Dask...")
    futures = []
    for chunk in chunks:
        futures.append(
            client.submit(
                route_batch_voyages_dask,
                chunk,
                dtv_db_future,
                lookup_future,
                G_merged_future,
            )
        )

    logger.info("Routing batches in parallel...")
    batch_results = client.gather(futures)

    client.close()
    cluster.close()

    return [res for batch in batch_results for res in batch]


def aggregate_results(results: list, G_merged: nx.Graph) -> tuple:
    """Aggregates batch routing outputs into intensity counts, penalized segments, and paths."""
    edge_cargo = defaultdict(float)
    edge_trips = defaultdict(int)
    violations_tracker = defaultdict(list)

    # Pre-build edge metadata dictionary to avoid nested O(E) searches
    edge_metadata = {}
    for u, v, d in G_merged.edges(data=True):
        eid = get_edge_key(d, u, v)
        if eid:
            edge_metadata[eid] = {
                "geometry": d.get("geometry"),
                "name": d.get("name", "Unnamed"),
                "data_source": d.get("data_source", "unknown"),
            }

    edge_penalties = defaultdict(
        lambda: {
            "lock": 0.0,
            "bridge": 0.0,
            "fairway": 0.0,
            "sea": 0.0,
            "cemt": 0.0,
            "total": 0.0,
        }
    )

    trips_data = []
    unique_trips_data = []
    success_count = 0
    no_path_count = 0
    geocode_fail_count = 0

    for res in results:
        status = res["status"]
        if status == "success":
            success_count += 1
            for edge_info in res["path"]:
                edge_id = edge_info["edge_id"]
                p_dict = edge_info["penalties"]
                edge_cargo[edge_id] += float(res["cargo_weight"])
                edge_trips[edge_id] += int(res["trips"])
                for k in ["lock", "bridge", "fairway", "sea", "cemt", "total"]:
                    edge_penalties[edge_id][k] += p_dict[k] * int(res["trips"])

            for viol in res["violations"]:
                viol_key = (viol["edge_id"], viol["type"])
                violations_tracker[viol_key].append(viol)

            for edge_info in res["path"]:
                edge_id = edge_info["edge_id"]
                p_dict = edge_info["penalties"]
                meta = edge_metadata.get(edge_id)
                if meta and meta["geometry"]:
                    trips_data.append(
                        {
                            "unlo_herkomst": res["unlo_herkomst"],
                            "unlo_bestemming": res["unlo_bestemming"],
                            "sk_code": res["sk_code"],
                            "nstr_nw": res.get("nstr_nw"),
                            "trips": res["trips"],
                            "cargo_weight_kg": res["cargo_weight"],
                            "edge_id": edge_id,
                            "penalty_lock": p_dict["lock"],
                            "penalty_bridge": p_dict["bridge"],
                            "penalty_fairway": p_dict["fairway"],
                            "penalty_sea": p_dict["sea"],
                            "penalty_cemt": p_dict["cemt"],
                            "penalty_total": p_dict["total"],
                            "geometry": meta["geometry"],
                        }
                    )

            unique_trips_data.append(
                {
                    "unlo_herkomst": res["unlo_herkomst"],
                    "unlo_bestemming": res["unlo_bestemming"],
                    "sk_code": res["sk_code"],
                    "nstr_nw": res.get("nstr_nw"),
                    "trips": res["trips"],
                    "cargo_weight_kg": res["cargo_weight"],
                    "nodes": res["path_nodes"],
                }
            )
        elif status == "geocode_fail":
            geocode_fail_count += 1
        else:
            no_path_count += 1

    logger.info(
        f"Dask parallel batch routing complete. Success: {success_count}, No Path: {no_path_count}, Geocode Fail: {geocode_fail_count}"
    )

    # Build intensity data
    intensity_data = []
    for edge_id, meta in edge_metadata.items():
        if edge_id in edge_cargo or edge_id in edge_trips:
            p_info = edge_penalties[edge_id]
            intensity_data.append(
                {
                    "id": edge_id,
                    "data_source": meta["data_source"],
                    "name": meta["name"],
                    "cargo_weight_kg": edge_cargo[edge_id],
                    "trip_count": edge_trips[edge_id],
                    "penalty_lock": p_info["lock"],
                    "penalty_bridge": p_info["bridge"],
                    "penalty_fairway": p_info["fairway"],
                    "penalty_sea": p_info["sea"],
                    "penalty_cemt": p_info["cemt"],
                    "penalty_total": p_info["total"],
                    "geometry": meta["geometry"],
                }
            )

    # Build penalized data
    penalized_data = []
    for (edge_id, v_type), records in violations_tracker.items():
        meta = edge_metadata.get(edge_id)
        if meta and meta["geometry"]:
            tot_trips = sum(r["trips"] for r in records)
            tot_cargo = sum(r["cargo_weight"] for r in records)
            worst_r = max(records, key=lambda x: x["trips"])

            penalized_data.append(
                {
                    "edge_id": edge_id,
                    "name": meta["name"],
                    "data_source": meta["data_source"],
                    "violation_type": v_type,
                    "ship_class": worst_r["ship_class"],
                    "ship_value": worst_r["ship_value"],
                    "limit_value": worst_r["limit_value"],
                    "struct_id": worst_r["struct_id"],
                    "struct_name": worst_r["struct_name"],
                    "trips": tot_trips,
                    "cargo_weight_kg": tot_cargo,
                    "geometry": meta["geometry"],
                }
            )

    return intensity_data, penalized_data, trips_data, unique_trips_data


def export_assignment_outputs(
    output_dir: pathlib.Path,
    intensity_data: list,
    penalized_data: list,
    trips_data: list,
    unique_trips_data: list,
):
    """Exports geodataframes and unique trips path data to GeoJSON, Parquet and GPKG."""
    if not intensity_data or not trips_data or not unique_trips_data:
        raise ValueError(
            "Assignment outputs are empty. No voyages were routed successfully."
        )

    intensity_gdf = gpd.GeoDataFrame(
        intensity_data, geometry="geometry", crs="EPSG:4326"
    )
    intensity_gdf.to_file(output_dir / "intensity.geojson", driver="GeoJSON")
    intensity_gdf.to_parquet(output_dir / "intensity.geoparquet")

    gpkg_path = output_dir / "routing_detailed_analysis.gpkg"
    intensity_gdf.to_file(gpkg_path, layer="connected_routes", driver="GPKG")

    trips_gdf = gpd.GeoDataFrame(trips_data, geometry="geometry", crs="EPSG:4326")
    trips_gdf.to_file(gpkg_path, layer="assigned_trips", driver="GPKG")
    logger.info(f"Saved individual trip routes to {gpkg_path}.")

    if penalized_data:
        penalized_gdf = gpd.GeoDataFrame(
            penalized_data, geometry="geometry", crs="EPSG:4326"
        )
        penalized_gdf.to_file(gpkg_path, layer="penalized_edges", driver="GPKG")
        logger.info(f"Saved penalized edges debug layer to {gpkg_path}.")

    unique_df = pd.DataFrame(unique_trips_data)
    unique_df.to_parquet(output_dir / "trips_with_paths.parquet")
    logger.info(
        f"Saved unique trip paths to {output_dir / 'trips_with_paths.parquet'}."
    )


def assign_traffic(
    graph_path: pathlib.Path,
    base_graph: pathlib.Path,
    ivs_dir: pathlib.Path,
    output_dir: pathlib.Path,
    year: int | None = None,
    month: int | None = None,
):
    """Assigns IVS voyages with clean, decoupled helper functions."""
    output_dir.mkdir(parents=True, exist_ok=True)
    reference_dir = pathlib.Path("reference")

    # Load inputs
    zenodo_coords = load_unlocode_coordinates(reference_dir)
    dtv_db = load_shiptypes(reference_dir)

    logger.info(f"Loading base merged graph from {base_graph}...")
    with open(base_graph, "rb") as f:
        G_merged = pickle.load(f)

    lookup = build_edge_structures_lookup()

    # Build spatial lookup for geocoding
    node_list = []
    for n_id, n_data in G_merged.nodes(data=True):
        d = dict(n_data)
        d["node_id_key"] = n_id
        node_list.append(d)

    nodes_gdf = gpd.GeoDataFrame(
        node_list,
        index=[d["node_id_key"] for d in node_list],
        geometry="geometry",
        crs="EPSG:4326",
    )
    nodes_gdf = nodes_gdf[nodes_gdf.geometry.notnull() & ~nodes_gdf.geometry.is_empty]
    node_ids = list(nodes_gdf.index)
    coords = np.array([(p.x, p.y) for p in nodes_gdf.geometry])
    tree = KDTree(coords)

    node_locode_map = build_locode_node_lookup(G_merged)

    # Load, group, and geocode voyages
    df_voyages = load_voyages(ivs_dir, year, month)
    voyage_groups = group_voyages(df_voyages)
    voyage_groups = geocode_voyage_groups(
        voyage_groups, zenodo_coords, node_locode_map, tree, node_ids, G_merged
    )

    # Execute routing
    results = run_parallel_routing(voyage_groups, G_merged, lookup, dtv_db)

    # Aggregate results
    intensity_data, penalized_data, trips_data, unique_trips_data = aggregate_results(
        results, G_merged
    )

    # Export outputs
    export_assignment_outputs(
        output_dir, intensity_data, penalized_data, trips_data, unique_trips_data
    )

    logger.info("Traffic assignment complete!")
