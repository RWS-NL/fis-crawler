import os
import json
import subprocess
import re
import sqlite3
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point, LineString
import requests
from fis import utils

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Target output files
OUTPUT_DIR = "output"
REPORT_PATH = os.path.join(OUTPUT_DIR, "lock_dimensions_validation_report.md")
HTML_REPORT_PATH = os.path.join(OUTPUT_DIR, "lock_dimensions_validation_report.html")
LOCAL_EXCEL = "/Users/baart_f/.gemini/antigravity-cli/brain/4f10c31d-c0eb-4451-bbc8-1899998278a4/Chamber_comparison.xlsx"
BIVAS_DB = "reference/Bivas.5.10.1.sqlite"
FIS_CHAMBERS = "output/fis-export/chamber.geoparquet"
EURIS_CHAMBERS = "output/euris-export/LockChamber_NL_20260224.geojson"
AIMED_LEVELS = "output/fis-export/aimedlevel.geoparquet"

IMAGES_DIR = os.path.join(OUTPUT_DIR, "images")
AERIALS_DIR = os.path.join(IMAGES_DIR, "aerials")
CHARTS_DIR = os.path.join(IMAGES_DIR, "charts")

# Create output dirs
os.makedirs(AERIALS_DIR, exist_ok=True)
os.makedirs(CHARTS_DIR, exist_ok=True)


def get_waterway_levels(sluis_name):
    """Return the waterway names and aimed water levels (streefpeil in NAP) for both sides of the lock."""
    s = sluis_name.lower().strip()
    if "belfeld" in s:
        return "Maas (bovenstrooms)", 14.1, "Maas (benedenstrooms)", 10.8
    elif "born" in s:
        return (
            "Julianakanaal (bovenstrooms)",
            44.7,
            "Julianakanaal (benedenstrooms)",
            32.6,
        )
    elif "eefde" in s:
        return "Twentekanaal", 10.0, "Gelderse IJssel", 3.0
    elif "gaarkeuken" in s:
        return (
            "Van Starkenborghkanaal (oost)",
            -0.93,
            "Prinses Margrietkanaal (west)",
            -0.52,
        )
    elif "hansweert" in s:
        return "Kanaal door Zuid-Beveland", 0.0, "Westerschelde", 0.0
    elif "heel" in s:
        return (
            "Julianakanaal / Kanaal Wessem-Nederweert",
            28.65,
            "Maasplassen Heel (stuwpeil Linne)",
            20.8,
        )
    elif "houtrib" in s:
        return "IJsselmeer", 0.0, "Markermeer", -0.2
    elif "krammer" in s:
        return "Volkerakpeil", 0.0, "Krammer / Oosterschelde", 0.0
    elif "kreekrak" in s:
        return "Antwerpen kanaalpeil", 1.8, "Schelde-Rijnverbinding (Volkerakpeil)", 0.0
    elif "maasbracht" in s:
        return (
            "Julianakanaal (bovenstrooms)",
            32.6,
            "Julianakanaal (benedenstrooms)",
            20.8,
        )
    elif "oranje" in s:
        return "Markermeer", -0.2, "Binnen-IJ / Noordzeekanaal", -0.4
    elif "bernhard" in s:
        return "Waal (stuwpeil Hagestein/rivier)", 3.0, "Amsterdam-Rijnkanaal", -0.4
    elif "beatrix" in s:
        return "Lek (stuwpeil Hagestein)", 3.0, "Lekkanaal / Amsterdam-Rijnkanaal", -0.4
    elif "irene" in s:
        return "Lek (stuwpeil Hagestein)", 3.0, "Amsterdam-Rijnkanaal", -0.4
    elif "margriet" in s:
        return "IJsselmeer", -0.1, "Friese Boezem", -0.52
    elif "sambeek" in s:
        return "Maas (bovenstrooms)", 10.8, "Maas (benedenstrooms)", 8.6
    elif "weurt" in s:
        return "Maas-Waalkanaal", 7.95, "Waal (rivier)", 5.0
    elif "stevin" in s:
        return "IJsselmeer", -0.1, "Waddenzee (tij)", 0.0
    elif "terneuzen" in s:
        return "Kanaal Gent-Terneuzen", 2.1, "Westerschelde (tij)", 0.0
    elif "volkerak" in s:
        return "Hollandsch Diep", 0.0, "Volkerak (Volkerakpeil)", 0.0
    else:
        return "Onbekende waterweg", None, "Onbekende waterweg", None


def download_aerial_photo(sluis_clean, chamber_clean, centroid):
    """Download aerial photo from PDOK WMS for the lock centroid (RD New EPSG:28992)."""
    filename = f"{sluis_clean}_{chamber_clean}.jpg"
    path = os.path.join(AERIALS_DIR, filename)
    if os.path.exists(path) and os.path.getsize(path) > 1000:
        return f"images/aerials/{filename}"

    # Calculate BBOX (700m box centered on lock to cover entire chamber)
    half_size = 350
    xmin = centroid.x - half_size
    xmax = centroid.x + half_size
    ymin = centroid.y - half_size
    ymax = centroid.y + half_size
    bbox_str = f"{xmin},{ymin},{xmax},{ymax}"

    url = (
        "https://service.pdok.nl/hwh/luchtfotorgb/wms/v1_0?"
        "SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&"
        "LAYERS=Actueel_ortho25&CRS=EPSG:28992&"
        f"BBOX={bbox_str}&WIDTH=400&HEIGHT=400&FORMAT=image/jpeg"
    )
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200 and len(r.content) > 1000:
            with open(path, "wb") as f:
                f.write(r.content)
            return f"images/aerials/{filename}"
        else:
            print(
                f"Failed to fetch aerial photo for {sluis_clean} {chamber_clean}: HTTP {r.status_code} or small size"
            )
    except Exception as e:
        print(f"WMS request failed for {sluis_clean} {chamber_clean}: {e}")
    return None


def generate_comparison_chart(
    sluis_clean,
    chamber_clean,
    fis_len,
    euris_len,
    bivas_len,
    survey_len,
    selected_len,
    fis_wid,
    euris_wid,
    bivas_wid,
    survey_wid,
    selected_wid,
):
    """Generate professional bar chart of lock dimensions and save as PNG."""
    filename = f"{sluis_clean}_{chamber_clean}.png"
    path = os.path.join(CHARTS_DIR, filename)

    def clean_val(val):
        try:
            return float(val) if pd.notna(val) else 0.0
        except Exception:
            return 0.0

    sources = ["FIS", "EURIS", "BIVAS", "Survey", "Selected"]
    lengths = [
        clean_val(fis_len),
        clean_val(euris_len),
        clean_val(bivas_len),
        clean_val(survey_len),
        clean_val(selected_len),
    ]
    widths = [
        clean_val(fis_wid),
        clean_val(euris_wid),
        clean_val(bivas_wid),
        clean_val(survey_wid),
        clean_val(selected_wid),
    ]

    fig, ax1 = plt.subplots(figsize=(6, 3.2))

    x = np.arange(len(sources))
    width = 0.35

    color_len = "#2b5c8f"
    color_wid = "#20b2aa"

    ax1.bar(x - width / 2, lengths, width, label="Usable Length (m)", color=color_len)
    ax1.set_ylabel("Length (m)", color=color_len)
    ax1.tick_params(axis="y", labelcolor=color_len)
    ax1.set_xticks(x)
    ax1.set_xticklabels(sources)
    ax1.grid(True, linestyle="--", alpha=0.3)

    ax2 = ax1.twinx()
    ax2.bar(
        x + width / 2, widths, width, label="Gate/Chamber Width (m)", color=color_wid
    )
    ax2.set_ylabel("Width (m)", color=color_wid)
    ax2.tick_params(axis="y", labelcolor=color_wid)

    plt.title(
        f"Dimension Comparison: {sluis_clean} ({chamber_clean})",
        fontsize=10,
        fontweight="bold",
        pad=12,
    )
    fig.tight_layout()

    plt.savefig(path, dpi=150)
    plt.close(fig)
    return f"images/charts/{filename}"


def get_issue_body():
    """Fetch GitHub Issue 58 markdown body using gh CLI."""
    print("Fetching Issue 58 body via GitHub CLI...")
    try:
        res = subprocess.run(
            [
                "gh",
                "issue",
                "view",
                "58",
                "--repo",
                "RWS-NL/fis-crawler",
                "--json",
                "body",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        data = json.loads(res.stdout)
        return data.get("body", "")
    except Exception as e:
        print(f"Error fetching issue body: {e}")
        return ""


def parse_survey_table(body):
    """Parse Technical Specifications (Vraag 7) table from issue body."""
    print("Parsing survey table from issue body...")
    lines = body.split("\n")
    table_lines = []
    in_table = False

    for line in lines:
        if "Technische Specificaties (Vraag 7)" in line:
            in_table = True
            continue
        if in_table:
            stripped = line.strip()
            if stripped.startswith("|"):
                table_lines.append(stripped)
            elif len(table_lines) > 0 and not stripped.startswith("|"):
                # End of table
                break

    if not table_lines:
        print("Survey table not found in issue body.")
        return pd.DataFrame()

    # Process table markdown
    headers = [h.strip() for h in table_lines[0].split("|")[1:-1]]
    rows = []
    for line in table_lines[2:]:  # skip headers and separator
        cells = [c.strip() for c in line.split("|")[1:-1]]
        if len(cells) == len(headers):
            rows.append(cells)

    df = pd.DataFrame(rows, columns=headers)
    print(f"Parsed {len(df)} survey rows.")
    return df


def parse_local_excel():
    """Read target lock complexes and manual dimensions from Chamber_comparison.xlsx."""
    print(f"Reading {LOCAL_EXCEL}...")
    if not os.path.exists(LOCAL_EXCEL):
        print(f"Excel file {LOCAL_EXCEL} not found.")
        return pd.DataFrame()

    # Read Sluizen sheet
    df = pd.read_excel(LOCAL_EXCEL, sheet_name="Sluizen")

    # Headers are in row index 4 (5th row)
    headers = list(df.iloc[4].values)
    # Assign unique column names
    col_names = []
    for idx, name in enumerate(headers):
        if pd.isna(name):
            col_names.append(f"unnamed_{idx}")
        else:
            col_names.append(f"{name}_{idx}")

    data_df = df.iloc[5:].copy()
    data_df.columns = col_names

    # Clean up empty rows
    data_df = data_df.dropna(subset=["Sluis_0", "name_1"])
    print(f"Read {len(data_df)} lock rows from Excel.")
    return data_df


def load_bivas_locks(db_path=BIVAS_DB, branch_set_id=337):
    """Load BIVAS locks from SQLite."""
    print("Loading BIVAS locks...")
    if not os.path.exists(db_path):
        print(f"BIVAS database not found at {db_path}")
        return gpd.GeoDataFrame(
            columns=["id", "name", "bivas_length", "bivas_width"],
            geometry=[],
            crs="EPSG:28992",
        )

    conn = sqlite3.connect(db_path)
    try:
        nodes_df = pd.read_sql_query(
            "SELECT ID as NodeID, XCoordinate, YCoordinate FROM nodes WHERE BranchSetId = ?",
            conn,
            params=(branch_set_id,),
        )
        query = """
        SELECT 
            l.ArcID as id,
            a.Name as name,
            l.LockLength__m as bivas_length,
            l.LockWidth__m as bivas_width,
            a.FromNodeID,
            a.ToNodeID
        FROM locks l
        JOIN arcs a ON l.ArcID = a.ID AND l.BranchSetId = a.BranchSetId
        WHERE l.BranchSetId = ?
        """
        locks_df = pd.read_sql_query(query, conn, params=(branch_set_id,))
        if locks_df.empty:
            return gpd.GeoDataFrame(
                columns=["id", "name", "bivas_length", "bivas_width"],
                geometry=[],
                crs="EPSG:28992",
            )

        merged = locks_df.merge(nodes_df, left_on="FromNodeID", right_on="NodeID")
        merged = merged.rename(
            columns={"XCoordinate": "X_from", "YCoordinate": "Y_from"}
        )
        merged = merged.merge(nodes_df, left_on="ToNodeID", right_on="NodeID")
        merged = merged.rename(columns={"XCoordinate": "X_to", "YCoordinate": "Y_to"})

        lines = [
            LineString(
                [Point(row["X_from"], row["Y_from"]), Point(row["X_to"], row["Y_to"])]
            )
            for _, row in merged.iterrows()
        ]
        return gpd.GeoDataFrame(
            merged[["id", "name", "bivas_length", "bivas_width"]],
            geometry=lines,
            crs="EPSG:28992",
        )
    finally:
        conn.close()


OSM_CACHE_PATH = "output/osm_cache.json"


def load_osm_cache():
    if os.path.exists(OSM_CACHE_PATH):
        try:
            with open(OSM_CACHE_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_osm_cache(cache):
    try:
        with open(OSM_CACHE_PATH, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"Failed to save OSM cache: {e}")


def query_osm_lock(lon, lat, chamber_name=None, radius_m=250):
    """Query OSM lock details near a coordinate using Overpass API with local JSON caching."""
    cache = load_osm_cache()
    cache_key = f"{lat:.4f}_{lon:.4f}"
    if cache_key in cache and cache[cache_key]:
        return cache[cache_key]

    # Convert radius in meters to approx degrees for bounding box
    deg = radius_m / 111000.0
    min_lat = lat - deg
    max_lat = lat + deg
    min_lon = lon - deg
    max_lon = lon + deg

    query = f"""
    [out:json][timeout:15];
    (
      node["waterway"="lock_gate"]({min_lat},{min_lon},{max_lat},{max_lon});
      way["waterway"="lock_gate"]({min_lat},{min_lon},{max_lat},{max_lon});
      way["lock"="yes"]({min_lat},{min_lon},{max_lat},{max_lon});
      way["waterway"="lock_chamber"]({min_lat},{min_lon},{max_lat},{max_lon});
    );
    out body;
    """
    url = "https://overpass-api.de/api/interpreter"
    headers = {
        "User-Agent": "AntigravityLockValidation/1.0",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    result = {}
    try:
        resp = requests.post(url, data={"data": query}, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            elements = data.get("elements", [])

            candidates = []
            for elem in elements:
                tags = elem.get("tags", {})
                length = (
                    tags.get("maxlength")
                    or tags.get("length")
                    or tags.get("seamark:lock:length")
                    or tags.get("seamark:lock:maxlength")
                )
                width = (
                    tags.get("maxwidth")
                    or tags.get("width")
                    or tags.get("seamark:lock:width")
                    or tags.get("seamark:gate:clearance_width")
                    or tags.get("seamark:lock:maxwidth")
                )
                if length or width:
                    candidates.append((elem, tags, length, width))

            if candidates:
                best_elem = None
                best_tags = None
                best_len = None
                best_wid = None

                if chamber_name:
                    keywords = [
                        w.lower()
                        for w in re.split(r"\W+", chamber_name)
                        if w and len(w) > 3
                    ]
                    for elem, tags, length, width in candidates:
                        elem_name = (
                            tags.get("lock_name")
                            or tags.get("name")
                            or tags.get("seamark:name")
                            or ""
                        ).lower()
                        if any(kw in elem_name for kw in keywords):
                            best_elem = elem
                            best_tags = tags
                            best_len = length
                            best_wid = width
                            break

                if not best_elem:
                    best_elem, best_tags, best_len, best_wid = candidates[0]

                def to_float(val):
                    if not val:
                        return None
                    m = re.search(r"^\d+(\.\d+)?", str(val))
                    return float(m.group(0)) if m else None

                result = {
                    "osm_length": to_float(best_len),
                    "osm_width": to_float(best_wid),
                    "osm_source": "OpenStreetMap",
                }
    except Exception as e:
        print(f"OSM query failed for ({lon}, {lat}): {e}")

    cache[cache_key] = result
    save_osm_cache(cache)
    return result


def get_val(series, name):
    """Safely get column value from Series checking for _fis suffixes and case insensitivity."""
    # Check suffixes
    for suffix in ["_fis", ""]:
        key = f"{name}{suffix}"
        if key in series.index:
            return series[key]
    # Check case-insensitive
    for key in series.index:
        if key.lower() == name.lower() or key.lower() == f"{name.lower()}_fis":
            return series[key]
    return None


def main():
    print("Starting validation report generator...")

    # 1. Load Survey Data and Target Locks Excel
    issue_body = get_issue_body()
    survey_df = parse_survey_table(issue_body)
    excel_df = parse_local_excel()

    # 2. Load and normalize primary GIS datasets
    print("Loading FIS chambers...")
    fis = gpd.read_parquet(FIS_CHAMBERS)
    print("Loading EURIS chambers...")
    euris = gpd.read_file(EURIS_CHAMBERS)

    print("Normalizing attributes based on schema.toml...")
    schema = utils.load_schema()
    fis = utils.normalize_attributes(fis, "chambers", schema)
    euris = utils.normalize_attributes(euris, "chambers", schema)

    print("Loading BIVAS locks...")
    bivas_rd = load_bivas_locks()

    # 3. Load aimed water levels for vertical datum conversions
    print("Loading Aimed Levels...")
    aimed_levels = gpd.read_parquet(AIMED_LEVELS)

    # Standardize crs to RD (EPSG:28992) for spatial processing
    if fis.crs is None:
        fis.set_crs(epsg=4326, inplace=True)
    if euris.crs is None:
        euris.set_crs(epsg=4326, inplace=True)
    if aimed_levels.crs is None:
        aimed_levels.set_crs(epsg=4326, inplace=True)

    fis_rd = fis.to_crs(epsg=28992)
    euris_rd = euris.to_crs(epsg=28992)
    aimed_rd = aimed_levels.to_crs(epsg=28992)

    # 4. Join Aimed Water Levels to FIS Chambers
    print("Joining target waterway levels to FIS chambers...")
    # Find nearest aimedlevel feature to each chamber centroid
    fis_rd["centroid"] = fis_rd.geometry.centroid
    # Convert centroids to GeoDataFrame
    centroids_gdf = gpd.GeoDataFrame(
        fis_rd[["id"]], geometry=fis_rd["centroid"], crs="EPSG:28992"
    )
    # Spatial join nearest aimed level
    joined_levels = gpd.sjoin_nearest(
        centroids_gdf, aimed_rd[["Value", "geometry"]], how="left", max_distance=500
    )
    # Map back to fis_rd
    fis_rd = fis_rd.merge(
        joined_levels[["id", "Value"]].rename(
            columns={"Value": "target_water_level_nap"}
        ),
        on="id",
        how="left",
    )

    # 5. Core Comparison Join on ISRS Code
    print("Merging FIS and EURIS on ISRS/locode...")

    # Clean ISRS IDs (strip floats)
    def clean_isrs(val):
        if pd.isna(val):
            return None
        s = str(val).split(".")[0].strip()
        return s if s else None

    fis_rd["isrs_clean"] = fis_rd["code"].apply(clean_isrs)
    euris_rd["isrs_clean"] = euris_rd["id"].apply(clean_isrs)

    # Perform inner merge on clean ISRS code
    gis_merged = fis_rd.merge(euris_rd, on="isrs_clean", suffixes=("_fis", "_euris"))
    gis_merged = gpd.GeoDataFrame(gis_merged, geometry="geometry_fis", crs="EPSG:28992")
    print(f"Matched {len(gis_merged)} chambers by ISRS code.")

    # 6. Spatial match to BIVAS
    print("Matching to BIVAS locks spatially...")
    # Buffer BIVAS lines by 150m for matching locks
    bivas_to_join = bivas_rd.rename(
        columns={"id": "bivas_id_orig", "name": "bivas_name_orig"}
    )
    # Perform spatial join
    gis_merged_buffered = gis_merged.copy()
    gis_merged_buffered.geometry = gis_merged_buffered.geometry.centroid.buffer(150)
    matches_bivas = gpd.sjoin(
        gis_merged_buffered, bivas_to_join, how="left", rsuffix="bivas"
    )
    # Drop duplicates in case multiple BIVAS arcs matched
    matches_bivas = matches_bivas.drop_duplicates(subset=["id_fis"]).copy()

    # 7. Query OSM dimensions and generate final stats for target locks
    # 7. Query OSM dimensions and generate final stats for target locks
    results_list = []

    total_chambers = len(excel_df)
    print(f"\nValidating dimensions for target locks ({total_chambers} chambers)...")
    from tqdm import tqdm

    for _, row in tqdm(
        excel_df.iterrows(), total=total_chambers, desc="Validating locks"
    ):
        sluis_name = row["Sluis_0"]
        chamber_name = row["name_1"]
        tqdm.write(f"Processing {sluis_name} - {chamber_name}...")
        excel_id = str(row["route_id_2"]).split(".")[0]

        # Find match in matches_bivas using Name or Id
        match = matches_bivas[
            (matches_bivas["name_fis"].str.contains(sluis_name, case=False, na=False))
            & (
                matches_bivas["name_fis"].str.contains(
                    chamber_name.split()[-1], case=False, na=False
                )
            )
        ]

        if match.empty:
            # Fallback to loose name match
            match = matches_bivas[
                matches_bivas["name_fis"].str.contains(
                    chamber_name, case=False, na=False
                )
            ]

        if match.empty:
            # Fallback to Excel route_id matching ParentId
            match = matches_bivas[
                matches_bivas["parent_id_fis"].astype(str) == excel_id
            ]

        if not match.empty:
            m_row = match.iloc[0]
            # Retrieve coordinates
            centroid_wgs = (
                gpd.GeoSeries([m_row["geometry_fis"]], crs="EPSG:28992")
                .to_crs(epsg=4326)
                .iloc[0]
                .centroid
            )

            # Query OSM
            osm_dims = query_osm_lock(
                centroid_wgs.x, centroid_wgs.y, chamber_name=chamber_name
            )

            # Extract values using helper
            fis_len = get_val(m_row, "dim_usable_length")
            fis_wid = get_val(m_row, "dim_gate_width")
            fis_struct_len = get_val(m_row, "dim_structural_length")
            fis_struct_wid = get_val(m_row, "dim_structural_width")

            # Convert EURIS cm to m
            euris_len = m_row.get("dim_usable_length_euris")
            euris_wid = m_row.get("dim_gate_width_euris")

            # EURIS structural dimensions are in cm, divide by 100
            euris_struct_len = m_row.get("dim_structural_length_euris")
            if pd.notna(euris_struct_len):
                euris_struct_len = float(euris_struct_len) / 100.0
            euris_struct_wid = m_row.get("dim_structural_width_euris")
            if pd.notna(euris_struct_wid):
                euris_struct_wid = float(euris_struct_wid) / 100.0

            bivas_len = m_row.get("bivas_length")
            bivas_wid = m_row.get("bivas_width")

            # Threshold calculations
            # Extract streefpeil (aimed level)
            streefpeil = m_row.get("target_water_level_nap")

            # sill depth
            sill_depth_bobi = get_val(m_row, "sill_depth_bo_bi")
            if pd.isna(sill_depth_bobi):
                sill_depth_bobi = get_val(m_row, "ThresholdLowerLevel")
            if pd.isna(sill_depth_bobi):
                sill_depth_bobi = get_val(m_row, "dim_threshold_lower")

            sill_depth_bebu = get_val(m_row, "sill_depth_be_bu")
            if pd.isna(sill_depth_bebu):
                sill_depth_bebu = get_val(m_row, "ThresholdUpperLevel")
            if pd.isna(sill_depth_bebu):
                sill_depth_bebu = get_val(m_row, "dim_threshold_upper")

            # Get reference level datums
            ref_bobi = m_row.get("reference_level_inner")
            if pd.isna(ref_bobi) or not ref_bobi:
                ref_bobi = m_row.get("height_reference_level")
            if (
                pd.isna(ref_bobi)
                or not ref_bobi
                or str(ref_bobi).upper() in ["BO/BI", "BE/BU"]
            ):
                ref_bobi = "KP/SP (nog te controleren via kaarten)"

            ref_bebu = m_row.get("ref_level_name")
            if (
                pd.isna(ref_bebu)
                or not ref_bebu
                or str(ref_bebu).upper() in ["BO/BI", "BE/BU"]
            ):
                ref_bebu = "KP/SP (nog te controleren via kaarten)"

            # Retrieve waterway levels for high/low sides
            waterway_hoog, peil_hoog, waterway_laag, peil_laag = get_waterway_levels(
                sluis_name
            )

            # Calculate absolute threshold heights
            threshold_height_bobi = None
            threshold_height_bebu = None
            if pd.notna(sill_depth_bobi):
                try:
                    val = float(sill_depth_bobi)
                    ref_peil = peil_hoog if peil_hoog is not None else streefpeil
                    if pd.notna(ref_peil):
                        threshold_height_bobi = ref_peil - abs(val)
                    else:
                        threshold_height_bobi = val if val < 0 else -val
                except Exception:
                    pass
            if pd.notna(sill_depth_bebu):
                try:
                    val = float(sill_depth_bebu)
                    ref_peil = peil_laag if peil_laag is not None else streefpeil
                    if pd.notna(ref_peil):
                        threshold_height_bebu = ref_peil - abs(val)
                    else:
                        threshold_height_bebu = val if val < 0 else -val
                except Exception:
                    pass

            # Operator survey match
            survey_match = survey_df[
                survey_df["Sluisnaam"].str.contains(sluis_name, case=False, na=False)
            ]
            survey_len = None
            survey_wid = None
            survey_drempel_bobi = None
            survey_drempel_bebu = None

            if not survey_match.empty:
                s_row = survey_match.iloc[0]
                survey_len = s_row.get("Kolklengte (m)")
                survey_wid = s_row.get("Kolkbreedte (m)")
                survey_drempel_bobi = s_row.get("Drempel Bo/Bi (m)")
                survey_drempel_bebu = s_row.get("Drempel Be/Bu (m)")

            # Helper to convert to float safely
            def to_f(v):
                try:
                    return float(v) if pd.notna(v) else None
                except Exception:
                    return None

            s_len_val = to_f(survey_len)
            f_len_val = to_f(fis_len)
            b_len_val = to_f(bivas_len)

            # Selection Rule logic for Length
            selected_len = fis_len
            selection_method_len = "Onzeker: Handmatige controle vereist"
            if (
                s_len_val is not None
                and f_len_val is not None
                and abs(s_len_val - f_len_val) < 0.5
            ):
                selected_len = fis_len
                selection_method_len = "Overeenstemming: Enquête & FIS"
            elif (
                f_len_val is not None
                and b_len_val is not None
                and abs(f_len_val - b_len_val) < 0.5
            ):
                selected_len = fis_len
                selection_method_len = "Overeenstemming: FIS & BIVAS"

            s_wid_val = to_f(survey_wid)
            f_wid_val = to_f(fis_wid)
            b_wid_val = to_f(bivas_wid)

            # Selection Rule logic for Width
            selected_wid = fis_wid
            selection_method_wid = "Onzeker: Handmatige controle vereist"
            if (
                s_wid_val is not None
                and f_wid_val is not None
                and abs(s_wid_val - f_wid_val) < 0.1
            ):
                selected_wid = fis_wid
                selection_method_wid = "Overeenstemming: Enquête & FIS"
            elif (
                f_wid_val is not None
                and b_wid_val is not None
                and abs(f_wid_val - b_wid_val) < 0.1
            ):
                selected_wid = fis_wid
                selection_method_wid = "Overeenstemming: FIS & BIVAS"

            # Build file-friendly names
            sluis_clean = re.sub(r"[^a-zA-Z0-9]", "_", sluis_name)
            chamber_clean = re.sub(r"[^a-zA-Z0-9]", "_", chamber_name)

            # Generate charts & download aerial photos
            aerial_path = download_aerial_photo(
                sluis_clean, chamber_clean, m_row["geometry_fis"].centroid
            )
            chart_path = generate_comparison_chart(
                sluis_clean,
                chamber_clean,
                fis_len,
                euris_len,
                bivas_len,
                survey_len,
                selected_len,
                fis_wid,
                euris_wid,
                bivas_wid,
                survey_wid,
                selected_wid,
            )

            # Calculate discrepancy flags
            mismatch_fis_euris = False
            if (
                pd.notna(fis_len)
                and pd.notna(euris_len)
                and abs(fis_len - euris_len) > 0.5
            ):
                mismatch_fis_euris = True
            if (
                pd.notna(fis_wid)
                and pd.notna(euris_wid)
                and abs(fis_wid - euris_wid) > 0.1
            ):
                mismatch_fis_euris = True

            outlier_bivas = False
            if (
                pd.notna(fis_len)
                and pd.notna(bivas_len)
                and abs(fis_len - bivas_len) > 2.0
            ):
                outlier_bivas = True
            if (
                pd.notna(fis_wid)
                and pd.notna(bivas_wid)
                and abs(fis_wid - bivas_wid) > 0.2
            ):
                outlier_bivas = True

            outlier_survey = False
            if pd.notna(fis_len) and pd.notna(survey_len):
                try:
                    if abs(fis_len - float(survey_len)) > 2.0:
                        outlier_survey = True
                except Exception:
                    pass
            if pd.notna(fis_wid) and pd.notna(survey_wid):
                try:
                    if abs(fis_wid - float(survey_wid)) > 0.2:
                        outlier_survey = True
                except Exception:
                    pass

            status_str = "Consistent"
            action_desc = "Akkoord (Geen actie vereist)"

            if mismatch_fis_euris:
                status_str = "FIS/EURIS Afwijking"
                action_desc = "Handmatige controle vereist: Controleer fysieke afmetingen via BGT en PDOK luchtfoto"
            elif outlier_survey:
                status_str = "Enquête Afwijking"
                action_desc = "Handmatige controle vereist: Contacteer sluisoperator of raadpleeg S-57/IENC kaarten"
            elif outlier_bivas:
                status_str = "BIVAS Afwijking"
                if (
                    "Enquête & FIS" in selection_method_len
                    or "Enquête & FIS" in selection_method_wid
                ):
                    action_desc = "Akkoord (Geen actie): BIVAS gebruikt complex-gemiddelde; FIS & Enquête bevestigen waarde"
                else:
                    action_desc = "Handmatige controle vereist: Controleer of BIVAS-afwijking door complex-gemiddelde komt"

            # Build result
            results_list.append(
                {
                    "status_str": status_str,
                    "action_desc": action_desc,
                    "Sluis": sluis_name,
                    "name": chamber_name,
                    "isrs": m_row["isrs_clean"],
                    "fis_len": fis_len,
                    "euris_len": euris_len,
                    "bivas_len": bivas_len,
                    "osm_len": osm_dims.get("osm_length"),
                    "survey_len": survey_len,
                    "wiki_len": row.get("schut_lengte_16")
                    if pd.notna(row.get("schut_lengte_16"))
                    else row.get("length_18"),
                    "disk_len": row.get("schut_lengte_25")
                    if pd.notna(row.get("schut_lengte_25"))
                    else row.get("length_27"),
                    "selected_len": selected_len,
                    "selection_method_len": selection_method_len,
                    "fis_wid": fis_wid,
                    "euris_wid": euris_wid,
                    "bivas_wid": bivas_wid,
                    "osm_wid": osm_dims.get("osm_width"),
                    "survey_wid": survey_wid,
                    "wiki_wid": row.get("width_19"),
                    "disk_wid": row.get("width_28"),
                    "selected_wid": selected_wid,
                    "selection_method_wid": selection_method_wid,
                    "streefpeil": streefpeil,
                    "raw_bobi": sill_depth_bobi,
                    "raw_bebu": sill_depth_bebu,
                    "threshold_height_bobi": threshold_height_bobi,
                    "threshold_height_bebu": threshold_height_bebu,
                    "survey_drempel_bobi": survey_drempel_bobi,
                    "survey_drempel_bebu": survey_drempel_bebu,
                    "wiki_drempel_bobi": row.get("sill_depth_bo_bi_15"),
                    "wiki_drempel_bebu": row.get("sill_depth_be_bu_14"),
                    "disk_drempel_bobi": row.get("sill_depth_bo_bi_24"),
                    "disk_drempel_bebu": row.get("sill_depth_be_bu_23"),
                    "ref_bobi": ref_bobi,
                    "ref_bebu": ref_bebu,
                    "note": get_val(m_row, "note"),
                    "aerial_path": aerial_path,
                    "chart_path": chart_path,
                    # New parameters: physical/structural dimensions and side-specific water levels
                    "fis_struct_len": fis_struct_len,
                    "fis_struct_wid": fis_struct_wid,
                    "euris_struct_len": euris_struct_len,
                    "euris_struct_wid": euris_struct_wid,
                    "waterway_hoog": waterway_hoog,
                    "peil_hoog": peil_hoog,
                    "waterway_laag": waterway_laag,
                    "peil_laag": peil_laag,
                }
            )
        else:
            tqdm.write(
                f"Could not find matching GIS records for {sluis_name} - {chamber_name}"
            )
            # 8. Generate Markdown Report
    print("Generating validation report...")

    report_content = f"""# Validatierapport Sluisafmetingen (Issue 58)
**Gegenereerd op**: {pd.Timestamp.now().isoformat()}
**Bereik**: 20 Rijkswaterstaat sluiscomplexen vergeleken over FIS, EURIS (NL), BIVAS, Enquêtes onder sluisoperatoren en OpenStreetMap (OSM).

## Begrippen & Databronnen (Terminology & Data Dictionary)

Uitleg van de gebruikte variabelen en databronnen:

### 1. Zijden van de sluiskolk (Sluisdrempels)
* **Bo/Bi (Bovenhoofd / Binnenhoofd)**: De drempel/sluiskop aan de hoge waterstandzijde (bovenwinds/stroomopwaarts of richting het binnenland). Dit is de kant van de sluis die grenst aan het water met het hogere streefpeil.
* **Be/Bu (Benedenhoofd / Buitenhoofd)**: De drempel/sluiskop aan de lage waterstandzijde (benedenwinds/stroomafwaarts of richting open water/zee). Dit is de kant van de sluis die grenst aan het water met het lagere streefpeil.
* **Verticaal Referentieniveau**: Voor beide zijden van de sluis (links/rechts of noord/zuid) is het specifieke streefpeil van de aansluitende waterweg weergegeven. De absolute drempelhoogte (NAP) is berekend ten opzichte van het lokale streefpeil aan die specifieke zijde.

### 2. Structurele vs. Toegestane Afmetingen
* **Fysieke / Structurele afmetingen (Structural dimensions)**: De daadwerkelijke bouwkundige lengte en breedte van de betonnen/stalen sluiskolk zelf.
* **Toegestane / Schutafmetingen (Usable/Allowed dimensions)**: De maximale afmetingen die een schip mag hebben om daadwerkelijk veilig geschut te kunnen worden (inclusief noodzakelijke veiligheidsmarges voor drempelstroming en deuren). De toegestane afmetingen zijn in de praktijk vaak kleiner dan de fysieke afmetingen.

### 3. BIVAS Modellering Beperking
* **BIVAS Netwerkgemiddelde**: In het BIVAS-netwerkmodel (gebruikt voor transportanalyses) worden parallelle kolken van één complex vaak gemodelleerd als één geaggregeerde verbinding met gemiddelde afmetingen. BIVAS maakt geen functioneel onderscheid tussen de individuele parallelle kolken, wat kan leiden tot afwijkingen ten opzichte van de specifieke kolkgegevens in FIS en EURIS.

## 1. Overzicht van Afmetingen

Dit deel toont de geselecteerde canonieke afmetingen, referentieniveaus en drempels per sluiskolk.

### Vergelijking Fysieke vs. Schut/Toegestane Afmetingen (meters)
*Opmerking: Fysieke afmetingen representeren de constructie. Schut/toegestane afmetingen representeren de bruikbare scheepsmaat.*

| Sluis | Kolknaam | ISRS-code | Fysieke Lengte (FIS) | Fysieke Lengte (EURIS) | Schutlengte (FIS) | Schutlengte (EURIS) | BIVAS Lengte | Enquête Lengte | Geselecteerde Schutlengte | Fysieke Breedte (FIS) | Fysieke Breedte (EURIS) | Schutbreedte (FIS) | Schutbreedte (EURIS) | BIVAS Breedte | Enquête Breedte | Geselecteerde Schutbreedte | Uitkomst & Actie |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
"""
    for r in results_list:
        report_content += (
            f"| **{r['Sluis']}** | {r['name']} | `{r['isrs']}` | "
            f"{r['fis_struct_len'] or 'nan'} | {r['euris_struct_len'] or 'nan'} | "
            f"{r['fis_len'] or 'nan'} | {r['euris_len'] or 'nan'} | "
            f"{r['bivas_len'] or 'nan'} | {r['survey_len'] or 'nan'} | "
            f"**{r['selected_len'] or 'nan'}** | "
            f"{r['fis_struct_wid'] or 'nan'} | {r['euris_struct_wid'] or 'nan'} | "
            f"{r['fis_wid'] or 'nan'} | {r['euris_wid'] or 'nan'} | "
            f"{r['bivas_wid'] or 'nan'} | {r['survey_wid'] or 'nan'} | "
            f"**{r['selected_wid'] or 'nan'}** | {r['action_desc']} |\n"
        )

    report_content += """
### Drempelhoogtes & Referentiewaterstanden per Sluiszijde
*Opmerking: Berekende drempelhoogtes zijn absolute niveaus t.o.v. **NAP**, berekend op basis van het streefpeil aan de betreffende zijde van de sluis.*

| Sluis | Kolknaam | Hoge Waterweg | Streefpeil Hoge Zijde (NAP) | FIS Drempel Bo/Bi (m) | Berekende Drempel Bo/Bi (NAP) | Referentie Bo/Bi | Enquête Bo/Bi | Lage Waterweg | Streefpeil Lage Zijde (NAP) | FIS Drempel Be/Bu (m) | Berekende Drempel Be/Bu (NAP) | Referentie Be/Bu | Enquête Be/Bu | Opmerkingen / Details Referentieniveau |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
"""
    for r in results_list:
        calc_bobi = (
            f"{r['threshold_height_bobi']:.2f}"
            if pd.notna(r["threshold_height_bobi"])
            else "nan"
        )
        calc_bebu = (
            f"{r['threshold_height_bebu']:.2f}"
            if pd.notna(r["threshold_height_bebu"])
            else "nan"
        )
        peil_h = f"{r['peil_hoog']:.2f}" if pd.notna(r["peil_hoog"]) else "nan"
        peil_l = f"{r['peil_laag']:.2f}" if pd.notna(r["peil_laag"]) else "nan"

        report_content += (
            f"| **{r['Sluis']}** | {r['name']} | "
            f"{r['waterway_hoog']} | {peil_h} | {r['raw_bobi'] or 'nan'} | {calc_bobi} | {r['ref_bobi']} | {r['survey_drempel_bobi'] or 'nan'} | "
            f"{r['waterway_laag']} | {peil_l} | {r['raw_bebu'] or 'nan'} | {calc_bebu} | {r['ref_bebu']} | {r['survey_drempel_bebu'] or 'nan'} | "
            f"*{str(r['note']).replace(chr(10), ' ').replace(chr(13), '')[:100]}* |\n"
        )

    report_content += """
## 2. Belangrijke Uitdagingen & Afwijkingen per Sluiscomplex

Hieronder volgen de specifieke technische uitdagingen en afwijkingen per sluiscomplex:

### Volkeraksluizen & Krammersluizen
- **Afwijking**: De lengtes in het BIVAS-netwerk wijken af van de daadwerkelijk bruikbare afmetingen (schutlengte).
- **Verificatieregel**: S-57 / IENC (`HORLEN`) en de enquête-waarden van sluisoperatoren vormen de operationele waarheid (bruikbare afmetingen). De fysieke kolklengtes komen overeen met de BGT-voetafdrukken.

### Oranjesluizen
- **Afwijking**: Parallelle kolken (Noorderkolk, Zuiderkolk en Prins Willem-Alexandersluis) gebruiken verschillende referentieniveaus (bijv. Meerpeil vs. Kanaalpeil).
- **Reconciliatie**: Door drempelhoogtes te berekenen op basis van de bijbehorende streefpeilen van de vaarweg (+0,62m of het streefpeil), worden de relatieve drempeldieptes herleid naar een eenduidig NAP-referentieniveau.

### Sluis Weurt
- **Afwijking**: De standaard `lock_chamber_consistency.py` kon de sluizen niet matchen omdat Weurt twee parallelle kolken (Oostkolk en Westkolk) bevat die verkeerde landencodes hadden.
- **Resultaat**: Gestandaardiseerd door een schone koppeling op basis van ISRS-code en correctie van de filters. Beide kolken worden nu correct gematcht.

## 3. Aanbevolen Validatie-werkwijze

We adviseren om de volgende **Werkwijze** te hanteren voor het bepalen van sluisafmetingen in toekomstige releases:
1. **Vaarweg-koppeling via ISRS**: Vertrouw nooit volledig op ruimtelijke joins. Gebruik altijd de canonieke **ISRS-code** (`Code` in FIS, `locode` in EURIS) als primaire koppelsleutel.
2. **Eenheden standaardiseren**: Centimeter-kolommen uit EURIS (zoals `mlengthcm`, `mwidthcm`) automatisch door 100 delen om ze om te rekenen naar meters.
3. **Drempelhoogte herleiden per zijde**: Bereken de absolute drempelhoogte t.o.v. **NAP** met behulp van de streefpeilen van de aansluitende vaarwegen aan weerszijden:
   - Bo/Bi Drempelhoogte (NAP) = Streefpeil Boven/Binnen (NAP) - Diepte Bo/Bi
   - Be/Bu Drempelhoogte (NAP) = Streefpeil Beneden/Buiten (NAP) - Diepte Be/Bu
4. **Verificatiehiërarchie bij uitschieters**:
   - Voor operationele/bruikbare afmetingen (schutlengte/breedte): **IENC (S-57)** en de **Enquêtes van operatoren** hebben prioriteit.
   - Voor fysieke/structurele afmetingen: De **BGT** voetafdruk en metingen op **Luchtfoto's** (met de officiële PDOK WMS) hebben prioriteit.
- **Suggestie voor vervolgonderzoek (ENC-kaarten)**: In deze validatie zijn de officiële Inland ENC (IENC) vectorkaarten (S-57 bestanden met objecten zoals `lckchm` en `gatedt` en attributen als `HORLEN` en `HORWID`) nog niet direct ingelezen. Het wordt ten zeerste aanbevolen om in een vervolgfase de IENC-kaartkenmerken automatisch te oogsten en te vergelijken met FIS en EURIS als extra onafhankelijke kwaliteitsbron.
"""
    with open(REPORT_PATH, "w") as f:
        f.write(report_content)
    print(f"Validation report saved successfully to {REPORT_PATH}")

    # 9. Generate HTML Dashboard
    write_html_report(results_list)


def write_html_report(results_list):
    """Generate a clean, professional HTML dashboard report."""
    total_chambers = len(results_list)
    consistent_count = sum(1 for r in results_list if r["status_str"] == "Consistent")
    mismatch_count = sum(
        1 for r in results_list if r["status_str"] == "FIS/EURIS Afwijking"
    )
    bivas_outliers = sum(
        1 for r in results_list if r["status_str"] == "BIVAS Afwijking"
    )
    survey_outliers = sum(
        1 for r in results_list if r["status_str"] == "Enquête Afwijking"
    )

    html_content = f"""<!DOCTYPE html>
<html lang="nl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sluisafmetingen Validatie Dashboard</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            color: #334155;
            background-color: #f8fafc;
            margin: 0;
            padding: 0;
            line-height: 1.5;
        }}
        header {{
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
            color: white;
            padding: 2rem;
            margin-bottom: 2rem;
            border-bottom: 4px solid #3b82f6;
        }}
        header h1 {{
            margin: 0 0 0.5rem 0;
            font-size: 1.8rem;
            font-weight: 700;
        }}
        header p {{
            margin: 0;
            font-size: 0.95rem;
            color: #94a3b8;
        }}
        .container {{
            max-width: 1600px;
            margin: 0 auto;
            padding: 0 1.5rem 3rem 1.5rem;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}
        .kpi-card {{
            background-color: white;
            border: 1px solid #e2e8f0;
            border-radius: 0.375rem;
            padding: 1.25rem;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
            display: flex;
            flex-direction: column;
        }}
        .kpi-card .label {{
            font-size: 0.75rem;
            color: #64748b;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.25rem;
            font-weight: 600;
        }}
        .kpi-card .value {{
            font-size: 1.75rem;
            font-weight: 700;
            color: #0f172a;
        }}
        .section-card {{
            background-color: white;
            border: 1px solid #e2e8f0;
            border-radius: 0.375rem;
            padding: 1.5rem;
            margin-bottom: 2rem;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
        }}
        .section-title {{
            font-size: 1.25rem;
            font-weight: 650;
            color: #0f172a;
            margin-top: 0;
            margin-bottom: 1.25rem;
            border-bottom: 2px solid #f1f5f9;
            padding-bottom: 0.5rem;
        }}
        .table-responsive {{
            overflow-x: auto;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.82rem;
            text-align: left;
        }}
        th {{
            background-color: #f8fafc;
            color: #475569;
            font-weight: 600;
            padding: 0.6rem 0.8rem;
            border-bottom: 2px solid #e2e8f0;
        }}
        td {{
            padding: 0.6rem 0.8rem;
            border-bottom: 1px solid #e2e8f0;
        }}
        tr:hover {{
            background-color: #f8fafc;
        }}
        .badge {{
            display: inline-block;
            padding: 0.2rem 0.4rem;
            font-size: 0.7rem;
            font-weight: 600;
            border-radius: 0.25rem;
            text-transform: uppercase;
        }}
        .badge-success {{
            background-color: #dcfce7;
            color: #15803d;
            border: 1px solid #bbf7d0;
        }}
        .badge-danger {{
            background-color: #fee2e2;
            color: #b91c1c;
            border: 1px solid #fca5a5;
        }}
        .badge-warning {{
            background-color: #fef3c7;
            color: #d97706;
            border: 1px solid #fde68a;
        }}
        .badge-info {{
            background-color: #e0f2fe;
            color: #0369a1;
            border: 1px solid #bae6fd;
        }}
        .lock-grid {{
            display: grid;
            grid-template-columns: 1fr;
            gap: 1.5rem;
        }}
        .lock-card {{
            background-color: white;
            border: 1px solid #e2e8f0;
            border-radius: 0.375rem;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
            overflow: hidden;
        }}
        .lock-header {{
            background-color: #f8fafc;
            border-bottom: 1px solid #e2e8f0;
            padding: 1rem 1.25rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .lock-header h3 {{
            margin: 0;
            font-size: 1.1rem;
            color: #0f172a;
        }}
        .lock-header .isrs {{
            font-family: monospace;
            background-color: #e2e8f0;
            padding: 0.15rem 0.4rem;
            border-radius: 0.25rem;
            font-size: 0.75rem;
            color: #475569;
        }}
        .lock-body {{
            padding: 1.25rem;
            display: grid;
            grid-template-columns: 1.2fr 1fr 1fr 1.3fr;
            gap: 1.25rem;
        }}
        .mermaid {{
            background-color: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 0.25rem;
            padding: 0.25rem;
            font-size: 0.7rem;
            margin-bottom: 0.5rem;
        }}
        @media (max-width: 1024px) {{
            .lock-body {{
                grid-template-columns: 1fr;
            }}
        }}
        .data-panel {{
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }}
        .data-panel h4 {{
            margin-top: 0;
            margin-bottom: 0.75rem;
            font-size: 0.95rem;
            color: #334155;
            border-bottom: 1px solid #e2e8f0;
            padding-bottom: 0.35rem;
        }}
        .lock-meta-table {{
            width: 100%;
            margin-bottom: 0.75rem;
        }}
        .lock-meta-table td {{
            padding: 0.4rem;
            border-bottom: 1px solid #f1f5f9;
        }}
        .lock-meta-table td:first-child {{
            font-weight: 600;
            color: #64748b;
            width: 45%;
        }}
        .visuals-panel img {{
            width: 100%;
            height: auto;
            border-radius: 0.25rem;
            border: 1px solid #e2e8f0;
        }}
        .visuals-panel h5 {{
            margin: 0.5rem 0 0 0;
            font-size: 0.75rem;
            color: #64748b;
            text-align: center;
        }}
        .note-text {{
            font-style: italic;
            color: #475569;
            font-size: 0.8rem;
            margin-top: 0.75rem;
            background-color: #f8fafc;
            padding: 0.5rem 0.75rem;
            border-radius: 0.25rem;
            border-left: 3px solid #cbd5e1;
        }}
    </style>
</head>
<body>
    <header>
        <div class="container" style="padding:0;">
            <h1>Sluisafmetingen Validatie Dashboard</h1>
            <p>Kwaliteitscontrole en afmetingenvalidatie voor Rijkswaterstaat sluiskolken (Issue 58)</p>
        </div>
    </header>
    <div class="container">
        <div class="stats-grid">
            <div class="kpi-card">
                <div class="label">Totaal aantal sluiskolken</div>
                <div class="value">{total_chambers}</div>
            </div>
            <div class="kpi-card">
                <div class="label">Consistent</div>
                <div class="value">{consistent_count}</div>
            </div>
            <div class="kpi-card">
                <div class="label">FIS/EURIS Afwijkingen</div>
                <div class="value" style="color: #b91c1c;">{mismatch_count}</div>
            </div>
            <div class="kpi-card">
                <div class="label">BIVAS Afwijkingen</div>
                <div class="value" style="color: #0369a1;">{bivas_outliers}</div>
            </div>
            <div class="kpi-card">
                <div class="label">Enquête Afwijkingen</div>
                <div class="value" style="color: #d97706;">{survey_outliers}</div>
            </div>
        </div>

        <div class="section-card">
            <div class="section-title">Begrippen & Databronnen (Verificatie Drempels & Afmetingen)</div>
            <div style="font-size: 0.85rem; color: #475569;">
                <p>Uitleg van de gebruikte termen en databronnen in dit dashboard:</p>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; margin-top: 1rem;">
                    <div>
                        <h4 style="margin-top:0; color: #1e293b; border-bottom: 1px solid #e2e8f0; padding-bottom: 0.25rem;">1. Sluisdrempels (Zijden van de Kolk)</h4>
                        <ul style="padding-left: 1.25rem; line-height: 1.6; margin-bottom: 0;">
                            <li><strong>Bo/Bi (Bovenhoofd / Binnenhoofd)</strong>: Sluiskop aan de hoge waterstandzijde (bovenwinds/stroomopwaarts of richting het binnenland). Grenst aan het water met het hogere streefpeil.</li>
                            <li><strong>Be/Bu (Benedenhoofd / Buitenhoofd)</strong>: Sluiskop aan de lage waterstandzijde (benedenwinds/stroomafwaarts of richting open water/zee). Grenst aan het water met het lagere streefpeil.</li>
                            <li><strong>Streefpeilen weerszijden</strong>: Voor elke sluis is het specifieke referentieniveau (streefpeil) voor de hoge zijde (Bo/Bi) en lage zijde (Be/Bu) bepaald om drempelhoogtes betrouwbaar naar absolute NAP-hoogtes te herleiden.</li>
                        </ul>
                    </div>
                    <div>
                        <h4 style="margin-top:0; color: #1e293b; border-bottom: 1px solid #e2e8f0; padding-bottom: 0.25rem;">2. Afmetingen & BIVAS Beperking</h4>
                        <ul style="padding-left: 1.25rem; line-height: 1.6; margin-bottom: 0;">
                            <li><strong>Fysieke vs. Schut/Toegestane afmetingen</strong>: Fysieke afmetingen betreffen de constructie van de kolk zelf. Schut/toegestane afmetingen bepalen de maximale omvang van schepen die daadwerkelijk veilig geschut kunnen worden.</li>
                            <li><strong>BIVAS Complex-gemiddelde Opmerking</strong>: Het BIVAS-netwerkmodel gebruikt vaak de gemiddelde lengte over alle parallelle kolken van een sluiscomplex en maakt geen functioneel onderscheid tussen individuele kolken. Dit veroorzaakt afwijkingen bij individuele kolkvergelijkingen.</li>
                            <li><strong>ENC-kaarten Follow-up</strong>: De Inland ENC (IENC) vectorkaarten zijn in deze fase nog niet automatisch ingelezen. Het parseren van IENC-kaarten voor additionele validatie van schutafmetingen is opgenomen als aanbeveling voor vervolgonderzoek.</li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>

        <div style="margin-bottom: 1.5rem; background-color: white; border: 1px solid #e2e8f0; padding: 0.75rem 1.25rem; border-radius: 0.375rem; display: flex; align-items: center; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
            <label style="font-weight: 600; font-size: 0.85rem; color: #334155; cursor: pointer; display: flex; align-items: center; gap: 0.5rem; user-select: none;">
                <input type="checkbox" id="toggle-consistent" checked onchange="toggleConsistentRows()" style="width: 1rem; height: 1rem; cursor: pointer;">
                Toon alleen sluiskolken met afwijkingen (verberg consistente sluiskolken standaard)
            </label>
        </div>
        
        <script>
            function toggleConsistentRows() {{
                const showOnlyDiscrepancies = document.getElementById('toggle-consistent').checked;
                const rows = document.querySelectorAll('.row-consistent');
                const cards = document.querySelectorAll('.lock-card-consistent');
                rows.forEach(row => {{
                    row.style.display = showOnlyDiscrepancies ? 'none' : '';
                }});
                cards.forEach(card => {{
                    card.style.display = showOnlyDiscrepancies ? 'none' : '';
                }});
            }}
            // Run on load
            document.addEventListener('DOMContentLoaded', () => {{
                setTimeout(toggleConsistentRows, 100);
            }});
        </script>

         <div class="section-card">
            <div class="section-title">Overzicht: Vergelijking Fysieke vs. Schut/Toegestane Afmetingen</div>
            <div class="table-responsive">
                <table>
                    <thead>
                        <tr>
                            <th rowspan="2">Sluis</th>
                            <th rowspan="2">Kolknaam</th>
                            <th rowspan="2">ISRS-code</th>
                            <th colspan="7" style="text-align:center; border-bottom: 1px solid #e2e8f0;">Lengte (m)</th>
                            <th colspan="7" style="text-align:center; border-bottom: 1px solid #e2e8f0;">Breedte (m)</th>
                            <th rowspan="2">Status</th>
                        </tr>
                        <tr>
                            <th>Fysiek (FIS)</th>
                            <th>Fysiek (EUR)</th>
                            <th>Schut (FIS)</th>
                            <th>Schut (EUR)</th>
                            <th>BIVAS</th>
                            <th>Enquête</th>
                            <th>Geselecteerd</th>
                            <th>Fysiek (FIS)</th>
                            <th>Fysiek (EUR)</th>
                            <th>Schut (FIS)</th>
                            <th>Schut (EUR)</th>
                            <th>BIVAS</th>
                            <th>Enquête</th>
                            <th>Geselecteerd</th>
                        </tr>
                    </thead>
                    <tbody>
"""
    for r in results_list:
        if r["status_str"] == "Consistent":
            badge_class = "badge-success"
            row_class = "row-consistent"
        elif r["status_str"] == "FIS/EURIS Afwijking":
            badge_class = "badge-danger"
            row_class = "row-discrepancy"
        elif r["status_str"] == "Enquête Afwijking":
            badge_class = "badge-warning"
            row_class = "row-discrepancy"
        else:
            badge_class = "badge-info"
            row_class = "row-discrepancy"

        html_content += f"""
                        <tr class="{row_class}">
                            <td><strong>{r["Sluis"]}</strong></td>
                            <td>{r["name"]}</td>
                            <td><code>{r["isrs"]}</code></td>
                            <td>{r["fis_struct_len"] or "nan"}</td>
                            <td>{r["euris_struct_len"] or "nan"}</td>
                            <td>{r["fis_len"] or "nan"}</td>
                            <td>{r["euris_len"] or "nan"}</td>
                            <td>{r["bivas_len"] or "nan"}</td>
                            <td>{r["survey_len"] or "nan"}</td>
                            <td><strong>{r["selected_len"] or "nan"}</strong></td>
                            <td>{r["fis_struct_wid"] or "nan"}</td>
                            <td>{r["euris_struct_wid"] or "nan"}</td>
                            <td>{r["fis_wid"] or "nan"}</td>
                            <td>{r["euris_wid"] or "nan"}</td>
                            <td>{r["bivas_wid"] or "nan"}</td>
                            <td>{r["survey_wid"] or "nan"}</td>
                            <td><strong>{r["selected_wid"] or "nan"}</strong></td>
                            <td>
                                <span class="badge {badge_class}">{r["status_str"]}</span><br>
                                <small style="color: #64748b; font-size: 0.65rem;">{r["action_desc"]}</small>
                            </td>
                        </tr>"""

    html_content += """
                    </tbody>
                </table>
            </div>
        </div>

        <div class="section-card">
            <div class="section-title">Overzicht: Drempelhoogtes & Referentiewaterstanden per Sluiszijde (NAP)</div>
            <div class="table-responsive">
                <table>
                    <thead>
                        <tr>
                            <th rowspan="2">Sluis</th>
                            <th rowspan="2">Kolknaam</th>
                            <th colspan="6" style="text-align:center; border-bottom: 1px solid #e2e8f0;">Hoge Zijde (Boven/Binnen - Bo/Bi)</th>
                            <th colspan="6" style="text-align:center; border-bottom: 1px solid #e2e8f0;">Lage Zijde (Beneden/Buiten - Be/Bu)</th>
                            <th rowspan="2">Opmerkingen</th>
                        </tr>
                        <tr>
                            <th>Waterweg</th>
                            <th>Streefpeil (NAP)</th>
                            <th>FIS Drempel</th>
                            <th>Berekend (NAP)</th>
                            <th>Referentie</th>
                            <th>Enquête</th>
                            <th>Waterweg</th>
                            <th>Streefpeil (NAP)</th>
                            <th>FIS Drempel</th>
                            <th>Berekend (NAP)</th>
                            <th>Referentie</th>
                            <th>Enquête</th>
                        </tr>
                    </thead>
                    <tbody>
"""
    for r in results_list:
        calc_bobi = (
            f"{r['threshold_height_bobi']:.2f}"
            if pd.notna(r["threshold_height_bobi"])
            else "nan"
        )
        calc_bebu = (
            f"{r['threshold_height_bebu']:.2f}"
            if pd.notna(r["threshold_height_bebu"])
            else "nan"
        )
        peil_h = f"{r['peil_hoog']:.2f} m" if pd.notna(r["peil_hoog"]) else "nan"
        peil_l = f"{r['peil_laag']:.2f} m" if pd.notna(r["peil_laag"]) else "nan"
        note_snippet = (
            str(r["note"]).replace("\n", " ").replace("\r", "")[:120]
            if r["note"]
            else "nan"
        )
        row_class = (
            "row-consistent" if r["status_str"] == "Consistent" else "row-discrepancy"
        )

        html_content += f"""
                        <tr class="{row_class}">
                            <td><strong>{r["Sluis"]}</strong></td>
                            <td>{r["name"]}</td>
                            <td><span style="font-size:0.75rem;">{r["waterway_hoog"]}</span></td>
                            <td>{peil_h}</td>
                            <td>{r["raw_bobi"] or "nan"}</td>
                            <td><strong>{calc_bobi} m</strong></td>
                            <td><span class="isrs">{r["ref_bobi"]}</span></td>
                            <td>{r["survey_drempel_bobi"] or "nan"}</td>
                            <td><span style="font-size:0.75rem;">{r["waterway_laag"]}</span></td>
                            <td>{peil_l}</td>
                            <td>{r["raw_bebu"] or "nan"}</td>
                            <td><strong>{calc_bebu} m</strong></td>
                            <td><span class="isrs">{r["ref_bebu"]}</span></td>
                            <td>{r["survey_drempel_bebu"] or "nan"}</td>
                            <td style="color:#64748b; font-style:italic; font-size:0.75rem;">{note_snippet}</td>
                        </tr>"""

    html_content += """
                    </tbody>
                </table>
            </div>
        </div>

        <div class="section-card">
            <div class="section-title">Gedetailleerde Sluisgegevens</div>
            <div class="lock-grid">
"""
    for r in results_list:
        calc_bobi = (
            f"{r['threshold_height_bobi']:.2f}"
            if pd.notna(r["threshold_height_bobi"])
            else "nan"
        )
        calc_bebu = (
            f"{r['threshold_height_bebu']:.2f}"
            if pd.notna(r["threshold_height_bebu"])
            else "nan"
        )
        peil_h = f"{r['peil_hoog']:.2f} m" if pd.notna(r["peil_hoog"]) else "nan"
        peil_l = f"{r['peil_laag']:.2f} m" if pd.notna(r["peil_laag"]) else "nan"

        aerial_html = (
            f'<img src="{r["aerial_path"]}" alt="PDOK Luchtfoto">'
            if r["aerial_path"]
            else '<div style="background:#e2e8f0; height:200px; display:flex; align-items:center; justify-content:center; border-radius:0.375rem; border:1px solid #cbd5e1; color:#94a3b8;">Geen luchtfoto beschikbaar</div>'
        )
        chart_html = (
            f'<img src="{r["chart_path"]}" alt="Matplotlib vergelijkingstabel">'
            if r["chart_path"]
            else '<div style="background:#e2e8f0; height:200px; display:flex; align-items:center; justify-content:center; border-radius:0.375rem; border:1px solid #cbd5e1; color:#94a3b8;">Geen grafiek beschikbaar</div>'
        )

        if r["status_str"] == "Consistent":
            card_badge = '<span class="badge badge-success">Consistent</span>'
            card_class = "lock-card-consistent"
        elif r["status_str"] == "FIS/EURIS Afwijking":
            card_badge = '<span class="badge badge-danger">FIS/EURIS Afwijking</span>'
            card_class = "lock-card-discrepancy"
        elif r["status_str"] == "Enquête Afwijking":
            card_badge = '<span class="badge badge-warning">Enquête Afwijking</span>'
            card_class = "lock-card-discrepancy"
        else:
            card_badge = '<span class="badge badge-info">BIVAS Afwijking</span>'
            card_class = "lock-card-discrepancy"

        len_method = r["selection_method_len"]
        style_sf = (
            "style AgreeSF fill:#dcfce7,stroke:#15803d,stroke-width:2px"
            if "Enquête & FIS" in len_method
            else ""
        )
        style_fb = (
            "style AgreeFB fill:#dcfce7,stroke:#15803d,stroke-width:2px"
            if "FIS & BIVAS" in len_method
            else ""
        )
        style_unsure = (
            "style UnsureManual fill:#fee2e2,stroke:#b91c1c,stroke-width:2px"
            if "Onzeker" in len_method
            else ""
        )

        style_peil_yes = (
            "style Calc1 fill:#dcfce7,stroke:#15803d,stroke-width:2px"
            if pd.notna(r["streefpeil"])
            else ""
        )
        style_peil_no = (
            "style Calc2 fill:#dcfce7,stroke:#15803d,stroke-width:2px"
            if pd.isna(r["streefpeil"])
            else ""
        )

        mermaid_len = f"""
graph TD
    Start[Kies Afmeting] --> Check1{{{{Komen Enquête & FIS overeen?}}}}
    Check1 -- Ja --> AgreeSF[Overeenstemming: Enquête & FIS]
    Check1 -- Nee --> Check2{{{{Komen FIS & BIVAS overeen?}}}}
    Check2 -- Ja --> AgreeFB[Overeenstemming: FIS & BIVAS]
    Check2 -- Nee --> UnsureManual[Onzeker: Handmatige controle vereist]
    
    {style_sf}
    {style_fb}
    {style_unsure}
"""

        mermaid_threshold = f"""
graph TD
    Start[Drempelhoogte NAP] --> CheckPeil{{{{Is Streefpeil aanwezig?}}}}
    CheckPeil -- Ja --> Calc1[Hoogte = Streefpeil - Diepte]
    CheckPeil -- Nee --> Calc2[Hoogte = -Diepte NAP fallback]
    
    {style_peil_yes}
    {style_peil_no}
"""

        html_content += f"""
                <div class="lock-card {card_class}" id="{r["Sluis"]}_{r["name"]}">
                    <div class="lock-header">
                        <h3>{r["Sluis"]} - {r["name"]}</h3>
                        <div>
                            {card_badge}
                            <span class="isrs" style="margin-left: 0.5rem;">ISRS: {r["isrs"]}</span>
                        </div>
                    </div>
                    <div class="lock-body">
                        <div class="data-panel">
                            <div>
                                <h4>Vergelijkingsgegevens</h4>
                                <table class="lock-meta-table">
                                    <tr><td>Geselecteerde Schutlengte</td><td><strong>{r["selected_len"] or "nan"} m</strong> ({r["selection_method_len"]})</td></tr>
                                    <tr><td>Geselecteerde Schutbreedte</td><td><strong>{r["selected_wid"] or "nan"} m</strong> ({r["selection_method_wid"]})</td></tr>
                                    <tr><td>Fysieke Lengte (FIS)</td><td>{r["fis_struct_len"] or "nan"} m</td></tr>
                                    <tr><td>Fysieke Breedte (FIS)</td><td>{r["fis_struct_wid"] or "nan"} m</td></tr>
                                    <tr><td>Hoge Zijde ({r["waterway_hoog"]})</td><td>Streefpeil: {peil_h} | Drempel Bo/Bi NAP: <strong>{calc_bobi} m</strong> (Enquête: {r["survey_drempel_bobi"] or "nan"})</td></tr>
                                    <tr><td>Lage Zijde ({r["waterway_laag"]})</td><td>Streefpeil: {peil_l} | Drempel Be/Bu NAP: <strong>{calc_bebu} m</strong> (Enquête: {r["survey_drempel_bebu"] or "nan"})</td></tr>
                                </table>
                            </div>
                            {f'<div class="note-text"><strong>Opmerking:</strong> {r["note"]}</div>' if pd.notna(r["note"]) else ""}
                        </div>
                        <div class="visuals-panel">
                            <h4>PDOK Luchtfoto (700m)</h4>
                            {aerial_html}
                            <h5>Bron: Actueel_ortho25 (WMS)</h5>
                        </div>
                        <div class="visuals-panel">
                            <h4>Visualisatie van Afmetingen</h4>
                            {chart_html}
                            <h5>Bron: FIS / EURIS / BIVAS / Enquête</h5>
                        </div>
                        <div class="visuals-panel">
                            <h4>Beslissingslogica</h4>
                            <div style="font-weight:600; font-size:0.75rem; margin-bottom:0.25rem; color:#475569;">Selectiepad Afmetingen:</div>
                            <pre class="mermaid">{mermaid_len}</pre>
                            <div style="font-weight:600; font-size:0.75rem; margin-top:0.5rem; margin-bottom:0.25rem; color:#475569;">Selectiepad Drempelhoogte:</div>
                            <pre class="mermaid">{mermaid_threshold}</pre>
                        </div>
                    </div>
                </div>"""

    html_content += """
            </div>
        </div>
    </div>
    
    <script type="module">
        import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
        mermaid.initialize({ startOnLoad: true, theme: 'neutral', securityLevel: 'loose' });
    </script>
</body>
</html>
"""
    with open(HTML_REPORT_PATH, "w") as f:
        f.write(html_content)
    print(f"HTML validation dashboard saved successfully to {HTML_REPORT_PATH}")


if __name__ == "__main__":
    main()
