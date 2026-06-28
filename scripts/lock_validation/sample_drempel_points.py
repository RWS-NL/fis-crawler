"""Sample RWS bodemhoogte_1mtr at each manually-placed drempelkruin point.

Reads output/manual_checks/measurements.gpkg (drempelkruin layer), queries the
bodemhoogte_1mtr MapServer identify endpoint at each point's RD coordinate, and
updates meting_1m_nap in-place via SQLite (so the other layers are untouched).
Every query is cached in output/lock-validation/bathymetry_cache.json.

Sluizen with no bathymetry coverage (Prinses Beatrix, Krammer Noordkolk Be) are
expected to return None and are written as NULL.

Run: uv run scripts/lock_validation/sample_drempel_points.py
"""

import os
import shutil
import sys

import geopandas as gpd
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bathymetry import identify_bottom, load_cache, save_cache  # noqa: E402

MEASUREMENTS = "output/manual_checks/measurements.gpkg"
LAYER = "drempelkruin"
RD = 28992

# Sluizen / (sluis, kolk, zijde) with confirmed no bathymetry coverage
_NO_DATA_SLUIS = {"Prinses Beatrix"}
_NO_DATA_POINT = {("Krammer", "Noordkolk Krammersluizen", "Be")}


def _is_known_nodata(sluis, kolk, zijde):
    return sluis in _NO_DATA_SLUIS or (sluis, kolk, zijde) in _NO_DATA_POINT


def main():
    gdf = gpd.read_file(MEASUREMENTS, layer=LAYER)
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    gdf_rd = gdf.to_crs(epsg=RD)

    cache = load_cache()
    session = requests.Session()

    # Collect sampled values indexed by fid (or positional index)
    results = {}  # idx → float | None

    for idx, row in gdf_rd.iterrows():
        sluis = str(row.get("sluis") or "")
        kolk = str(row.get("kolk") or "")
        zijde = str(row.get("zijde") or "")
        pt = row.geometry
        if pt is None or pt.is_empty:
            print(f"  SKIP {sluis} / {kolk} / {zijde}: geen punt")
            results[idx] = None
            continue

        x, y = pt.x, pt.y

        if _is_known_nodata(sluis, kolk, zijde):
            val = None
            print(f"  NODATA (verwacht)  {sluis} / {kolk} / {zijde}")
        else:
            val = identify_bottom(x, y, cache, session=session)
            old = gdf.at[idx, "meting_1m_nap"]
            if val is None:
                print(
                    f"  NODATA (geen raster) {sluis} / {kolk} / {zijde}  ({x:.0f}, {y:.0f})"
                )
            else:
                old_s = f"{old:.3f}" if old == old and old is not None else "—"
                print(f"  {sluis} / {kolk} / {zijde}: {val:.3f} m NAP  (was {old_s})")

        results[idx] = val

    save_cache(cache)

    # Apply sampled values to the (WGS84) gdf
    no_data = 0
    updated = 0
    for idx, val in results.items():
        gdf.at[idx, "meting_1m_nap"] = val
        if val is None:
            no_data += 1
        else:
            updated += 1

    # Write back all layers to a temp file, then replace original.
    # (Plain sqlite3 UPDATE triggers SpatiaLite functions that plain sqlite3 lacks;
    # geopandas handles GPKG writes correctly.)
    import os as _os

    other_layers = ["profiel_as", "breedte"]
    others = {lyr: gpd.read_file(MEASUREMENTS, layer=lyr) for lyr in other_layers}

    tmp = MEASUREMENTS + ".tmp.gpkg"
    gdf.to_file(tmp, layer=LAYER, driver="GPKG", mode="w")
    for lyr, odf in others.items():
        odf.to_file(tmp, layer=lyr, driver="GPKG", mode="a")

    # Atomic-ish replace: backup original, move tmp into place
    backup = MEASUREMENTS + ".bak"
    shutil.copy2(MEASUREMENTS, backup)
    _os.replace(tmp, MEASUREMENTS)

    print(f"\nBackup → {backup}")
    print(f"Bijgewerkt: {updated}  |  Geen data: {no_data}  |  Totaal: {len(gdf)}")
    print(f"Geschreven naar {MEASUREMENTS} (layer: {LAYER})")


if __name__ == "__main__":
    main()
