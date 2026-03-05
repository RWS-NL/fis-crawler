import geopandas as gpd
from pathlib import Path


def main():
    print("Loading FIS lock schematization exported data...")
    fis_locks_path = Path("output/lock-schematization/lock.geoparquet")
    fis_chambers_path = Path("output/lock-schematization/chamber.geoparquet")
    if not fis_locks_path.exists() or not fis_chambers_path.exists():
        print(
            "Error: Could not find lock.geoparquet or chamber.geoparquet. Run `uv run lock-schematize` first."
        )
        return

    fis_locks = gpd.read_parquet(fis_locks_path)
    fis_chambers = gpd.read_parquet(fis_chambers_path)
    print(f"Loaded {len(fis_locks)} locks and {len(fis_chambers)} chambers from FIS.")

    print("Fetching DISK beheerobjecten:schutsluis...")
    disk_url = "https://geo.rijkswaterstaat.nl/services/ogc/gdr/disk_beheerobjecten/ows?service=WFS&version=2.0.0&request=GetFeature&typeName=disk_beheerobjecten:schutsluis&outputFormat=application/json"
    disk_locks = gpd.read_file(disk_url)
    print(f"Loaded {len(disk_locks)} locks from DISK.")

    # Project to EPSG:28992 for accurate distance measurement (meters)
    fis_locks_rd = fis_locks.to_crs("EPSG:28992")
    fis_chambers_rd = fis_chambers.to_crs("EPSG:28992")
    disk_locks_rd = disk_locks.to_crs("EPSG:28992")

    # Match based on spatial proximity, starting with CHAMBERS FIRST to reduce 1-many mappings
    # We use strict intersection without buffering first to avoid 1-to-many spillover to adjacent chambers
    fis_locks_buffered = fis_locks_rd.copy()
    fis_locks_buffered.geometry = fis_locks_buffered.buffer(500)

    # 1. Join DISK to Chambers (strict intersection)
    joined_chambers = gpd.sjoin(
        disk_locks_rd, fis_chambers_rd, how="left", predicate="intersects"
    )

    # Track matches
    matched_via_chamber = joined_chambers[joined_chambers["index_right"].notna()].copy()
    print(
        f"Successfully matched {len(matched_via_chamber)} DISK locks to FIS Chambers."
    )

    # Grab the unmatched for secondary fallback join against Locks
    unmatched_via_chamber_mask = joined_chambers["index_right"].isna()
    disk_locks_unmatched = (
        joined_chambers[unmatched_via_chamber_mask]
        .drop(columns=["index_right", "id_right"])
        .rename(columns={"id_left": "id"})
    )
    # Fix dropping of id that is necessary for sjoin
    if (
        "id" not in disk_locks_unmatched.columns
        and "disk_id" in disk_locks_unmatched.columns
    ):
        disk_locks_unmatched = disk_locks_unmatched.rename(columns={"disk_id": "id"})

    # 2. Join remaining DISK to Locks (fallback)
    joined_locks = gpd.sjoin(
        disk_locks_unmatched, fis_locks_buffered, how="left", predicate="intersects"
    )
    matched_via_lock = joined_locks[joined_locks["index_right"].notna()].copy()
    print(
        f"Successfully matched {len(matched_via_lock)} remaining DISK locks to FIS Complex bounding area."
    )

    # Combine successful matches to find which FIS parent complexes actually have a DISK object
    # For chambers, we need the ParentId to link back to the lock complex
    matched_chamber_complex_ids = matched_via_chamber["ParentId"].unique()
    matched_lock_complex_ids = matched_via_lock[
        "id_right"
    ].unique()  # 'id_right' is the fis_locks 'id'

    all_matched_complexes = set(matched_chamber_complex_ids).union(
        set(matched_lock_complex_ids)
    )
    print(
        f"\nTotal unique FIS complexes that have at least 1 DISK schutsluis: {len(all_matched_complexes)}"
    )

    # Filter FIS locks to only those with at least one DISK schutsluis
    fis_locks_filtered = fis_locks[fis_locks["id"].isin(all_matched_complexes)].copy()
    out_dir = Path("output")
    out_dir.mkdir(exist_ok=True)

    filtered_locks_out = out_dir / "fis_locks_with_disk.geojson"
    fis_locks_filtered.to_file(filtered_locks_out, driver="GeoJSON")
    print(
        f"Exported {len(fis_locks_filtered)} active FIS locks to {filtered_locks_out}"
    )

    # Generate updated discrepancy files for user validation
    unmatched_locks = joined_locks[joined_locks["index_right"].isna()].copy()

    cols_to_save_disk = [
        "id_left",
        "complexid",
        "complex_naam",
        "naam",
        "omschrijving",
        "geometry",
    ]
    unmatched_geojson = out_dir / "disk_unmatched_locks.geojson"
    unmatched_gdf = unmatched_locks[cols_to_save_disk].rename(
        columns={"id_left": "disk_id"}
    )
    unmatched_gdf = unmatched_gdf.to_crs("EPSG:4326")
    unmatched_gdf.to_file(unmatched_geojson, driver="GeoJSON")
    print(
        f"Saved {len(unmatched_locks)} still-unmatched DISK locks to {unmatched_geojson}."
    )

    # Recalculate 1-to-many using chamber joins where possible
    # A DISK lock matching multiple FIS chambers
    match_counts_chambers = matched_via_chamber.groupby("id_left").size()
    one_to_many_ids = match_counts_chambers[match_counts_chambers > 1].index

    one_to_many = matched_via_chamber[
        matched_via_chamber["id_left"].isin(one_to_many_ids)
    ].copy()
    # Bring in FIS chamber name
    one_to_many = one_to_many.merge(
        fis_chambers_rd[["id", "name"]],
        left_on="index_right",
        right_index=True,
        suffixes=("", "_fis"),
    )

    one_to_many_cols = [
        "id_left",
        "complexid",
        "complex_naam",
        "naam",
        "omschrijving",
        "name_fis",
        "geometry",
    ]
    one_to_many_geojson = out_dir / "disk_one_to_many_locks_chambers.geojson"
    if not one_to_many.empty:
        one_to_many_gdf = one_to_many[one_to_many_cols].rename(
            columns={"id_left": "disk_id"}
        )
        one_to_many_gdf = one_to_many_gdf.to_crs("EPSG:4326")
        one_to_many_gdf.to_file(one_to_many_geojson, driver="GeoJSON")

    print(
        f"Saved {one_to_many['id_left'].nunique() if not one_to_many.empty else 0} DISK locks that matched multiple FIS CHAMBERS to {one_to_many_geojson}."
    )

    print("\nFile generation complete.")


if __name__ == "__main__":
    main()
