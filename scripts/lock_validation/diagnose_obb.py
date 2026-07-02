"""Diagnose which lock chamber produces a skewed OBB navigation axis.

Read-only diagnostic for issue #58 / PR #188.

The validation pipeline derives a lock's navigation axis from the chamber
footprint via ``bathymetry.gate_centres()`` (midpoints of the two shortest edges
of the minimum rotated rectangle). For a well-behaved, long-narrow chamber this
axis runs parallel to the long edge of the OBB. A *skewed* axis shows up as a
large angle between the gate-centres axis and the OBB long edge -- an intrinsic
signal that does not depend on how ``measurements.gpkg`` was filled.

We restrict the scan to the target chambers by spatially matching each
``profiel_as`` line in ``output/manual_checks/measurements.gpkg`` to its chamber
polygon, and additionally report the angle difference against the (manually
straightened) reference line as a confirmation column.

Run: ``uv run scripts/lock_validation/diagnose_obb.py``
"""

import math
import os
import sys

import geopandas as gpd

# Allow importing the sibling bathymetry module when run as a script.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bathymetry import gate_centres  # noqa: E402
from shapely import minimum_rotated_rectangle  # noqa: E402

RD = 28992
FIS_CHAMBERS = "output/fis-export/chamber.geoparquet"
MEASUREMENTS = "output/manual_checks/measurements.gpkg"


def _edges(rect):
    """Return the 4 edges of an MRR polygon as (midpoint, vector, length)."""
    xs, ys = rect.exterior.coords.xy
    corners = [(xs[i], ys[i]) for i in range(4)]
    out = []
    for i in range(4):
        x0, y0 = corners[i]
        x1, y1 = corners[(i + 1) % 4]
        mid = ((x0 + x1) / 2, (y0 + y1) / 2)
        vec = (x1 - x0, y1 - y0)
        out.append((mid, vec, math.hypot(*vec)))
    return out


def _heading(vec):
    """Heading of a vector in degrees, folded to [0, 180)."""
    ang = math.degrees(math.atan2(vec[1], vec[0])) % 180.0
    return ang


def _angle_between(v1, v2):
    """Smallest angle (deg, 0..90) between two undirected line directions."""
    d = abs(_heading(v1) - _heading(v2)) % 180.0
    return min(d, 180.0 - d)


def axis_vector_from_gate_centres(geom_rd):
    centres = gate_centres(geom_rd)
    if centres is None:
        return None
    (x0, y0), (x1, y1) = centres
    return (x1 - x0, y1 - y0)


def main():
    fis = gpd.read_parquet(FIS_CHAMBERS)
    if fis.crs is None:
        fis = fis.set_crs(epsg=4326)
    fis_rd = fis.to_crs(epsg=RD)

    prof = gpd.read_file(MEASUREMENTS, layer="profiel_as")
    if prof.crs is None:
        prof = prof.set_crs(epsg=4326)
    prof_rd = prof.to_crs(epsg=RD)

    # Spatial index on chamber polygons for matching.
    sindex = fis_rd.sindex

    rows = []
    for _, pr in prof_rd.iterrows():
        line = pr.geometry
        if line is None or line.is_empty:
            continue
        probe = line.interpolate(0.5, normalized=True)

        # Find the chamber polygon containing the line midpoint; fall back to the
        # nearest polygon if no polygon contains it.
        cand_idx = list(sindex.query(probe, predicate="contains"))
        if cand_idx:
            chamber = fis_rd.iloc[cand_idx[0]]
        else:
            nearest_idx = list(sindex.nearest(probe, return_all=False))[1][0]
            chamber = fis_rd.iloc[nearest_idx]

        geom_rd = chamber.geometry
        rect = minimum_rotated_rectangle(geom_rd)
        if not hasattr(rect, "exterior") or rect.exterior is None:
            continue
        edges = _edges(rect)
        long_edge = max(edges, key=lambda e: e[2])
        short_edge = min(edges, key=lambda e: e[2])
        long_len, short_len = long_edge[2], short_edge[2]
        aspect = long_len / short_len if short_len else float("inf")

        axis_vec = axis_vector_from_gate_centres(geom_rd)
        if axis_vec is None:
            continue

        # Primary, provenance-independent signal: axis vs OBB long edge.
        skew_deg = _angle_between(axis_vec, long_edge[1])

        # Confirmation: gate-centres axis vs the manually straightened line.
        ref_vec = (
            line.coords[-1][0] - line.coords[0][0],
            line.coords[-1][1] - line.coords[0][1],
        )
        ref_diff = _angle_between(axis_vec, ref_vec)

        rows.append(
            {
                "sluis": pr.get("sluis"),
                "kolk": pr.get("kolk"),
                "chamber": chamber.get("Name"),
                "skew_deg": skew_deg,
                "aspect": aspect,
                "obb_len_m": long_len,
                "obb_wid_m": short_len,
                "vs_ref_deg": ref_diff,
            }
        )

    rows.sort(key=lambda r: r["skew_deg"], reverse=True)

    print(
        f"{'sluis':<16}{'kolk':<14}{'skew°':>7}{'aspect':>8}"
        f"{'len_m':>8}{'wid_m':>7}{'vs_ref°':>9}  chamber"
    )
    print("-" * 90)
    for r in rows:
        print(
            f"{str(r['sluis'])[:15]:<16}{str(r['kolk'])[:13]:<14}"
            f"{r['skew_deg']:>7.1f}{r['aspect']:>8.2f}"
            f"{r['obb_len_m']:>8.0f}{r['obb_wid_m']:>7.0f}{r['vs_ref_deg']:>9.1f}"
            f"  {r['chamber']}"
        )

    print(f"\n{len(rows)} chambers matched.")
    if rows:
        worst = rows[0]
        print(
            f"Most skewed axis: {worst['sluis']} / {worst['kolk']} "
            f"({worst['skew_deg']:.1f}° off the OBB long edge, "
            f"aspect {worst['aspect']:.2f})."
        )


if __name__ == "__main__":
    main()
