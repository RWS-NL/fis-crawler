"""Independent sill-crest measurement from the Rijkswaterstaat 1m bottom-height map.

The lock gates sit at the two short ends of a chamber. Sampling the
`bodemhoogte_1mtr` raster (NAP) along the chamber's long axis gives the bottom
profile through both gates: the local maxima near the ends are the sill crests,
which we cross-check against the FIS sill values.

Service: https://geo.rijkswaterstaat.nl/arcgis/rest/services/GDR/bodemhoogte_1mtr/MapServer
  - MapServer, EPSG:28992 (RD New) — identical to the chamber geometry CRS.
  - The `identify` operation returns the raster pixel value (bottom height, NAP)
    in the result's `attributes["Pixel Value"]`.

Every query is cached to a persistent JSON keyed on the rounded RD coordinate so
repeated runs never re-hit the service.
"""

import json
import math
import os

import requests
from shapely import minimum_rotated_rectangle
from shapely.geometry import LineString

SERVICE = (
    "https://geo.rijkswaterstaat.nl/arcgis/rest/services/GDR/bodemhoogte_1mtr/MapServer"
)
CACHE_PATH = "output/lock-validation/bathymetry_cache.json"

_NODATA_TOKENS = {"NoData", "nodata", "", None}


def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_cache(cache):
    try:
        with open(CACHE_PATH, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"Failed to save bathymetry cache: {e}")


def _cache_key(x, y):
    # 1m raster → 0.5m rounding keeps distinct samples without flooding the cache.
    return f"{round(x, 1)},{round(y, 1)}"


def _query_identify(x, y, tolerance, session):
    """Raw MapServer identify call; returns float value or None (NoData/error)."""
    params = {
        "geometry": json.dumps({"x": x, "y": y, "spatialReference": {"wkid": 28992}}),
        "geometryType": "esriGeometryPoint",
        "sr": 28992,
        "layers": "all",
        "tolerance": tolerance,
        "mapExtent": f"{x - 50},{y - 50},{x + 50},{y + 50}",
        "imageDisplay": "100,100,96",
        "returnGeometry": "false",
        "f": "json",
    }
    getter = session.get if session is not None else requests.get
    try:
        r = getter(f"{SERVICE}/identify", params=params, timeout=30)
        r.raise_for_status()
        for res in r.json().get("results", []):
            attrs = res.get("attributes") or {}
            raw = attrs.get("Pixel Value")
            if raw in _NODATA_TOKENS:
                continue
            try:
                return float(raw)
            except (ValueError, TypeError):
                continue
    except Exception:
        pass
    return None


def identify_bottom(x, y, cache, session=None):
    """Bottom height (NAP, m) at an RD point, or None for confirmed nodata.

    Tries progressively larger tolerances (2 px → 10 px) to handle drempel
    points placed precisely on gate structures where the 1m raster may have a
    1-cell gap. Only caches None when the service explicitly returns no raster
    value (not on network errors, so transient failures do not persist).

    Results are memoised in ``cache`` (mutated in place). Pass a persistent dict
    from :func:`load_cache` and persist it with :func:`save_cache`.
    """
    key = _cache_key(x, y)
    if key in cache:
        return cache[key]

    for tol in (2, 10):
        value = _query_identify(x, y, tol, session)
        if value is not None:
            cache[key] = value
            return value

    cache[key] = None
    return None


def gate_centres(geom_rd):
    """The two gate centres = midpoints of the chamber OBB's short edges."""
    rect = minimum_rotated_rectangle(geom_rd)
    if not hasattr(rect, "exterior") or rect.exterior is None:
        return None
    xs, ys = rect.exterior.coords.xy
    corners = [(xs[i], ys[i]) for i in range(4)]
    edges = []
    for i in range(4):
        x0, y0 = corners[i]
        x1, y1 = corners[(i + 1) % 4]
        edges.append(((x0, y0), (x1, y1), math.hypot(x1 - x0, y1 - y0)))
    short_two = sorted(edges, key=lambda e: e[2])[:2]
    mids = [((e[0][0] + e[1][0]) / 2, (e[0][1] + e[1][1]) / 2) for e in short_two]
    return mids[0], mids[1]


def _obb_axis_line(geom_rd, extend_m=15.0):
    """OBB-derived axis: line through the two gate centres, extended past both gates.

    The OBB of RWS lock chambers is visually confirmed to be well-aligned with
    the navigation direction, so this is a reliable fallback when no section is
    available.
    """
    centres = gate_centres(geom_rd)
    if centres is None:
        return None
    (x0, y0), (x1, y1) = centres
    dx, dy = x1 - x0, y1 - y0
    length = math.hypot(dx, dy)
    if length == 0:
        return None
    ux, uy = dx / length, dy / length
    a = (x0 - ux * extend_m, y0 - uy * extend_m)
    b = (x1 + ux * extend_m, y1 + uy * extend_m)
    return LineString([a, b])


def axis_profile_line(geom_rd, sections_rd=None, extend_m=15.0, swap=False):
    """LineString along the chamber navigation axis, extended past both gates.

    Gate centres come from the two short-edge midpoints of the OBB, which is
    visually confirmed to align with the navigation direction for all RWS target
    locks. The order returned by gate_centres() follows shapely's CCW exterior
    ordering, which does NOT reliably place the Bo/upstream gate first (that
    ordering is a geometry artifact, not physically grounded — see
    docs/werkwijze_sluiscontrole.md §3.4). Pass ``swap=True`` (determined from the
    boven/beneden node labels in output/lock-schematization/nodes.geoparquet,
    see validate_lock_dimensions.py::determine_gate_swap) to place the actual
    Bo/upstream gate first instead.

    ``sections_rd`` is accepted for API compatibility but no longer used for
    orientation: picking the closest section often fails for locks connecting two
    different waterways (e.g. a canal discharging into a river perpendicular to
    it), where the nearest section may run in the wrong direction.
    """
    centres = gate_centres(geom_rd)
    if centres is None:
        return None
    if swap:
        centres = (centres[1], centres[0])

    (x0, y0), (x1, y1) = centres
    dx, dy = x1 - x0, y1 - y0
    length = math.hypot(dx, dy)
    if length == 0:
        return None
    ux, uy = dx / length, dy / length
    a = (x0 - ux * extend_m, y0 - uy * extend_m)
    b = (x1 + ux * extend_m, y1 + uy * extend_m)
    return LineString([a, b])


def sample_profile(
    line,
    cache,
    session=None,
    step_m=2.0,
    gate_distances=None,
    gate_window_m=15.0,
    gate_step_m=1.0,
):
    """Sample bottom heights along ``line``. Returns list of (distance_m, nap).

    Uses ``step_m`` everywhere except within ``gate_window_m`` of each gate,
    where ``gate_step_m`` (default 1 m) is used to resolve the sill crest shape.
    ``gate_distances`` is a list of distances along the line where gates sit.
    """
    if line is None:
        return []

    total = line.length
    gate_distances = gate_distances or []

    # Build a sorted, deduplicated list of sample distances
    distances = set()
    d = 0.0
    while d <= total + 1e-6:
        distances.add(min(d, total))
        # Determine step: 1m if near a gate, else coarse step
        near_gate = any(abs(d - gd) < gate_window_m for gd in gate_distances)
        d += gate_step_m if near_gate else step_m

    out = []
    for d in sorted(distances):
        pt = line.interpolate(d)
        nap = identify_bottom(pt.x, pt.y, cache, session=session)
        out.append((d, nap))
    return out


def crest_near(profile, gate_distance, window_m=12.0):
    """Highest (crest) bottom height within ``window_m`` of a gate position."""
    vals = [
        nap
        for d, nap in profile
        if nap is not None and abs(d - gate_distance) <= window_m
    ]
    return max(vals) if vals else None


def measure_sill_crests(
    geom_rd,
    cache,
    sections_rd=None,
    session=None,
    step_m=2.0,
    extend_m=15.0,
    swap=False,
):
    """Independent sill-crest heights (NAP) at both gates from the 1m bottom map.

    ``crest1``/``gate1_distance`` are the Bo/upstream gate and ``crest2``/
    ``gate2_distance`` the Be/downstream gate when ``swap`` is set correctly for
    this chamber (see ``axis_profile_line``); otherwise the order is an
    unverified geometry artifact.

    Returns dict with the sampled ``profile`` (for plotting), the two gate
    distances along the profile, and the crest height at each gate.
    """
    line = axis_profile_line(
        geom_rd, sections_rd=sections_rd, extend_m=extend_m, swap=swap
    )
    if line is None:
        return None
    gate1_d = extend_m
    gate2_d = line.length - extend_m
    profile = sample_profile(
        line,
        cache,
        session=session,
        step_m=step_m,
        gate_distances=[gate1_d, gate2_d],
    )
    return {
        "profile": profile,
        "line": line,
        "line_length": line.length,
        "gate1_distance": gate1_d,
        "gate2_distance": gate2_d,
        "crest1": crest_near(profile, gate1_d),
        "crest2": crest_near(profile, gate2_d),
    }


def match_crests_to_fis(crests, fis_values, tol=0.5):
    """Corroborate FIS sill values against measured crests (set-based).

    Avoids assuming which gate is which side: for each FIS value find the nearest
    measured crest and flag agreement within ``tol``. Returns list of
    (fis_value, nearest_crest, abs_diff, agrees).
    """
    measured = [c for c in crests if c is not None]
    out = []
    for fv in fis_values:
        if fv is None or not measured:
            out.append((fv, None, None, False))
            continue
        nearest = min(measured, key=lambda c: abs(c - fv))
        diff = abs(nearest - fv)
        out.append((fv, nearest, diff, diff <= tol))
    return out
