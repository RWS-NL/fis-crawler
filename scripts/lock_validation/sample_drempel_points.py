"""Sample RWS bodemhoogte_1mtr at manually placed drempelkruin points.

Reads reference/measurements.gpkg (drempelkruin layer), queries the
RWS bodemhoogte_1mtr MapServer identify endpoint at each point's RD coordinate,
and updates meting_1m_nap in-place.
Every query is cached in output/lock-validation/bathymetry_cache.json.

Run:
    uv run python scripts/lock_validation/sample_drempel_points.py
"""

from pathlib import Path
import click
import geopandas as gpd
import requests

from fis.lock.bathymetry import (
    identify_bottom,
    load_cache,
    save_cache,
)

RD_CRS = "EPSG:28992"
WGS84_CRS = "EPSG:4326"

_NO_DATA_SLUIS = {"Prinses Beatrix"}
_NO_DATA_POINT = {("Krammer", "Noordkolk Krammersluizen", "Be")}


def _is_known_nodata(sluis: str, kolk: str, zijde: str) -> bool:
    return sluis in _NO_DATA_SLUIS or (sluis, kolk, zijde) in _NO_DATA_POINT


@click.command()
@click.option(
    "--measurements",
    type=click.Path(path_type=Path),
    default=Path("reference/measurements.gpkg"),
    help="Path to measurements GeoPackage.",
)
@click.option(
    "--layer",
    default="drempelkruin",
    help="Target layer containing sill crest points.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Sample without writing changes back to the GeoPackage.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force re-querying the service even if cached, especially for None values.",
)
def main(measurements: Path, layer: str, dry_run: bool, force: bool):
    """Sample 1m bathymetry at manually placed drempel points."""
    if not measurements.exists():
        raise FileNotFoundError(f"Measurements file not found: {measurements}")

    gdf = gpd.read_file(measurements, layer=layer)
    required_cols = {"sluis", "kolk", "zijde", "geometry"}
    missing = required_cols - set(gdf.columns)
    if missing:
        raise ValueError(f"Layer '{layer}' is missing required columns: {missing}")

    if gdf.crs is None:
        gdf = gdf.set_crs(WGS84_CRS)
    gdf_rd = gdf.to_crs(RD_CRS)

    cache = load_cache()
    if force:
        # Clear None entries so they can be re-queried
        cache = {k: v for k, v in cache.items() if v is not None}
    session = requests.Session()

    results = {}
    updated = 0
    no_data = 0

    click.echo(
        f"Sampling {len(gdf_rd)} drempel points from {measurements} ({layer})..."
    )

    for idx, row in gdf_rd.iterrows():
        sluis = str(row.get("sluis") or "")
        kolk = str(row.get("kolk") or "")
        zijde = str(row.get("zijde") or "")
        pt = row.geometry

        if pt is None or pt.is_empty:
            raise ValueError(
                f"Point geometry missing for {sluis} / {kolk} / {zijde} (row {idx})"
            )

        if _is_known_nodata(sluis, kolk, zijde):
            click.echo(f"  [NODATA - bekend] {sluis} / {kolk} / {zijde}")
            results[idx] = None
            no_data += 1
            continue

        val = identify_bottom(pt.x, pt.y, cache, session=session)
        results[idx] = val

        old_val = gdf.at[idx, "meting_1m_nap"]
        old_str = (
            f"{old_val:.3f} m" if old_val is not None and old_val == old_val else "—"
        )

        if val is None:
            click.echo(
                f"  [NODATA] {sluis} / {kolk} / {zijde} ({pt.x:.1f}, {pt.y:.1f})"
            )
            no_data += 1
            continue

        click.echo(
            f"  [OK] {sluis} / {kolk} / {zijde}: {val:.3f} m NAP (was {old_str})"
        )
        updated += 1

    save_cache(cache)
    click.echo(f"Cache saved. Updated: {updated}, NoData: {no_data}, Total: {len(gdf)}")

    if dry_run:
        click.echo("Dry run completed; GeoPackage not modified.")
        return

    for idx, val in results.items():
        gdf.at[idx, "meting_1m_nap"] = val

    gdf.to_file(measurements, layer=layer, driver="GPKG", mode="w")
    click.echo(
        f"Successfully updated {measurements} ({layer}) with {len(results)} sampled measurements."
    )


if __name__ == "__main__":
    main()
