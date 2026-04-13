import logging
import pathlib
import sqlite3
import zipfile

import geopandas as gpd
import pandas as pd
from shapely import wkt

from fis import settings, utils
from fis.graph.bivas import normalize_code
from fis.lock.core import group_complexes, load_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_euris_chambers(euris_zip_path: pathlib.Path, output_dir: pathlib.Path):
    \"\"\"Extract LockChamber geojson from EURIS zip.\"\"\"
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(euris_zip_path, \"r\") as z:
        for name in z.namelist():
            if \"LockChamber_\" in name and name.endswith(\".geojson\"):
                z.extract(name, output_dir)
                return output_dir / name
    return None


def load_bivas_locks(db_path):
    \"\"\"Load BIVAS locks and their geometries, including TrajectCode for ID matching.\"\"\"
    conn = sqlite3.connect(db_path)
    # Load arcs that are locks, with their trajectory info
    query = \"\"\"
    SELECT a.ID as bivas_id, a.Name as bivas_name, l.LockLength__m, l.LockWidth__m,
           l.NumberOfLocks as bivas_number_of_locks,
           a.MaximumDepth__m as bivas_depth,
           t.TrajectCode, t.StartKilometer, t.EndKilometer,
           n1.XCoordinate as x1, n1.YCoordinate as y1,
           n2.XCoordinate as x2, n2.YCoordinate as y2
    FROM arcs a
    JOIN locks l ON a.ID = l.ArcID AND a.BranchSetId = l.BranchSetId
    LEFT JOIN arc_vin_trajectory_connection t ON a.ID = t.ArcID
    JOIN nodes n1 ON a.FromNodeID = n1.ID AND a.BranchSetId = n1.BranchSetId
    JOIN nodes n2 ON a.ToNodeID = n2.ID AND a.BranchSetId = n2.BranchSetId
    WHERE a.BranchSetId = 337
    \"\"\"
    df = pd.read_sql_query(query, conn)
    conn.close()

    from shapely.geometry import LineString

    geoms = [LineString([(r.x1, r.y1), (r.x2, r.y2)]) for r in df.itertuples()]
    gdf = gpd.GeoDataFrame(df, geometry=geoms, crs=\"EPSG:28992\")
    gdf = gdf.to_crs(\"EPSG:4326\")

    # Normalize trajectory code for matching
    gdf[\"TrajectCode_norm\"] = gdf[\"TrajectCode\"].apply(normalize_code)
    return gdf


def main():
    export_dir = pathlib.Path(\"output/fis-export\")
    disk_dir = pathlib.Path(\"output/disk-export\")
    euris_zip = pathlib.Path(\"output/euris-export/NL_NetworkData_20260224_v2.4.zip\")
    bivas_db = pathlib.Path(\"reference/Bivas.5.10.1.sqlite\")
    output_file_gpq = pathlib.Path(
        \"output/bivas-validation/lock_chamber_consistency.geoparquet\"
    )
    output_file_geojson = pathlib.Path(
        \"output/bivas-validation/lock_chamber_consistency.geojson\"
    )

    output_file_gpq.parent.mkdir(parents=True, exist_ok=True)

    # 1. Load FIS + DISK data
    logger.info(\"Loading FIS and DISK data...\")
    data = load_data(export_dir, disk_dir)
    complexes = group_complexes(data)

    # Extract chambers with extra FIS metadata
    chambers_jsonl = export_dir / \"chamber.jsonl\"
    chambers_raw = pd.read_json(chambers_jsonl, lines=True)
    chambers_raw[\"Id\"] = chambers_raw[\"Id\"].apply(utils.stringify_id)

    rows = []
    for c in complexes:
        lock_id = utils.stringify_id(c[\"id\"])
        lock_name = c.get(\"name\")
        route_code_norm = normalize_code(c.get(\"route_code\"))

        disk_id = None
        disk_complex_id = None
        disk_name = None
        if c.get(\"disk_locks\"):
            dl = c[\"disk_locks\"][0]
            disk_id = dl.get(\"id\")
            disk_complex_id = dl.get(\"complexid\")
            disk_name = dl.get(\"naam\")

        for l_obj in c.get(\"locks\", []):
            for chamber in l_obj.get(\"chambers\", []):
                geom_wkt = chamber.get(\"geometry\")
                if not geom_wkt:
                    continue
                geom = wkt.loads(geom_wkt)

                # Get raw row for missing sill depths if needed
                cid = chamber.get(\"id\")
                raw_row = (
                    chambers_raw[chambers_raw[\"Id\"] == cid].iloc[0]
                    if cid in chambers_raw[\"Id\"].values
                    else {}
                )

                rows.append({
                    \"geometry\": geom,
                    \"fis_chamber_id\": cid,
                    \"fis_chamber_name\": chamber.get(\"name\"),
                    \"fis_lock_id\": lock_id,
                    \"fis_lock_name\": lock_name,
                    \"route_code_norm\": route_code_norm,
                    \"route_km_begin\": raw_row.get(\"RouteKmBegin\"),
                    \"route_km_end\": raw_row.get(\"RouteKmEnd\"),
                    \"disk_id\": disk_id,
                    \"disk_complex_id\": disk_complex_id,
                    \"disk_name\": disk_name,
                    \"fis_width\": chamber.get(\"width\"),
                    \"fis_length\": chamber.get(\"length\"),
                    \"fis_height\": chamber.get(\"height\"),
                    \"fis_sill_bebu\": raw_row.get(\"SillDepthBeBu\"),
                    \"fis_sill_bobi\": raw_row.get(\"SillDepthBoBi\"),
                    \"isrs_code\": c.get(\"isrs_code\"),
                })

    fis_chambers_gdf = gpd.GeoDataFrame(rows, crs=\"EPSG:4326\")

    # Calculate max dimensions per lock for BIVAS comparison
    lock_max_dims = (
        fis_chambers_gdf.groupby(\"fis_lock_id\")
        .agg({\"fis_length\": \"max\", \"fis_width\": \"max\"})
        .rename(
            columns={
                \"fis_length\": \"fis_lock_max_length\",
                \"fis_width\": \"fis_lock_max_width\",
            }
        )
    )

    fis_chambers_gdf = fis_chambers_gdf.merge(lock_max_dims, on=\"fis_lock_id\", how=\"left\")

    # 2. Load EURIS chambers
    logger.info(\"Loading EURIS chambers...\")
    tmp_dir = pathlib.Path(\"output/tmp\")
    euris_geojson = extract_euris_chambers(euris_zip, tmp_dir)
    if euris_geojson:
        euris_gdf = gpd.read_file(euris_geojson)
        schema = utils.load_schema()
        euris_gdf = utils.normalize_attributes(euris_gdf, \"chambers\", schema)
    else:
        logger.warning(\"EURIS chambers not found.\")
        euris_gdf = gpd.GeoDataFrame()

    # 3. Load BIVAS locks
    logger.info(\"Loading BIVAS locks...\")
    bivas_gdf = load_bivas_locks(bivas_db)

    # 4. Matching and Merging
    results = []

    # Project for spatial matching
    fis_chambers_rd = fis_chambers_gdf.to_crs(settings.PROJECTED_CRS)
    euris_rd = euris_gdf.to_crs(settings.PROJECTED_CRS) if not euris_gdf.empty else None
    bivas_rd = bivas_gdf.to_crs(settings.PROJECTED_CRS)

    for idx, row in fis_chambers_gdf.iterrows():
        res = row.to_dict()

        # Match EURIS
        matched_euris = None
        if euris_rd is not None and not euris_rd.empty:
            isrs_code = row.get(\"isrs_code\")
            if isrs_code:
                matches = euris_gdf[euris_gdf[\"id\"] == isrs_code]
                if not matches.empty:
                    matched_euris = matches.iloc[0]

            if matched_euris is None:
                chamber_geom_rd = fis_chambers_rd.loc[idx].geometry
                intersecting = euris_rd[euris_rd.intersects(chamber_geom_rd.buffer(15))]
                if not intersecting.empty:
                    matched_euris = intersecting.iloc[0]

        if matched_euris is not None:
            res.update({
                \"euris_id\": matched_euris.get(\"id\"),
                \"euris_name\": matched_euris.get(\"name\"),
                \"euris_width\": (
                    matched_euris.get(\"dim_width_cm\") / 100.0
                    if matched_euris.get(\"dim_width_cm\")
                    else None
                ),
                \"euris_length\": (
                    matched_euris.get(\"dim_length_cm\") / 100.0
                    if matched_euris.get(\"dim_length_cm\")
                    else None
                ),
                \"euris_height\": (
                    matched_euris.get(\"dim_height_cm\") / 100.0
                    if matched_euris.get(\"dim_height_cm\")
                    else None
                ),
                \"euris_depth\": (
                    matched_euris.get(\"mdraughtcm\") / 100.0
                    if matched_euris.get(\"mdraughtcm\")
                    else None
                ),
            })

        # Match BIVAS
        matched_bivas = None
        # 1. Try ID + KM Match
        rc_norm = row.get(\"route_code_norm\")
        km_begin = row.get(\"route_km_begin\")
        km_end = (
            row.get(\"route_km_end\") if row.get(\"route_km_end\") is not None else km_begin
        )

        if pd.notna(rc_norm) and km_begin is not None:
            mask = bivas_gdf[\"TrajectCode_norm\"] == rc_norm
            potential_bivas = bivas_gdf[mask]
            for _, b_row in potential_bivas.iterrows():
                # Check KM overlap
                b_min = min(b_row[\"StartKilometer\"], b_row[\"EndKilometer\"])
                b_max = max(b_row[\"StartKilometer\"], b_row[\"EndKilometer\"])
                f_min = min(km_begin, km_end)
                f_max = max(km_begin, km_end)
                # Simple overlap check
                if not (f_max < b_min or f_min > b_max):
                    matched_bivas = b_row
                    break

        # 2. Spatial fallback
        if matched_bivas is None:
            chamber_geom_rd = fis_chambers_rd.loc[idx].geometry
            intersecting_bivas = bivas_rd[bivas_rd.intersects(chamber_geom_rd.buffer(50))]
            if not intersecting_bivas.empty:
                matched_bivas = intersecting_bivas.iloc[0]

        if matched_bivas is not None:
            res.update({
                \"bivas_id\": matched_bivas.get(\"bivas_id\"),
                \"bivas_name\": matched_bivas.get(\"bivas_name\"),
                \"bivas_width\": matched_bivas.get(\"LockWidth__m\"),
                \"bivas_length\": matched_bivas.get(\"LockLength__m\"),
                \"bivas_depth\": matched_bivas.get(\"bivas_depth\"),
                \"bivas_number_of_locks\": matched_bivas.get(\"bivas_number_of_locks\"),
            })

        results.append(res)

    final_gdf = gpd.GeoDataFrame(results, crs=\"EPSG:4326\")

    # Ensure requested columns exist
    requested_cols = [
        \"geometry\",
        \"fis_chamber_id\",
        \"fis_lock_id\",
        \"disk_id\",
        \"disk_complex_id\",
        \"bivas_id\",
        \"fis_width\",
        \"fis_length\",
        \"fis_height\",
        \"fis_sill_bebu\",
        \"fis_sill_bobi\",
        \"fis_lock_max_length\",
        \"fis_lock_max_width\",
        \"euris_width\",
        \"euris_length\",
        \"euris_height\",
        \"euris_depth\",
        \"bivas_width\",
        \"bivas_length\",
        \"bivas_depth\",
        \"bivas_number_of_locks\",
        \"fis_chamber_name\",
        \"disk_name\",
        \"euris_name\",
        \"bivas_name\",
    ]

    final_gdf = final_gdf[[c for c in requested_cols if c in final_gdf.columns]]

    logger.info(f\"Saving {len(final_gdf)} chambers to {output_file_gpq}\")
    final_gdf.to_parquet(output_file_gpq)
    final_gdf.to_file(output_file_geojson, driver=\"GeoJSON\")


if __name__ == \"__main__\":
    main()
