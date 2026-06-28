import os
import glob
import json
import math
import subprocess
import re
import sqlite3
import argparse
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point, LineString
import requests
from fis import utils

# Ensure the lock_validation package directory is on the path so the sibling
# bathymetry module can be imported regardless of how the script is invoked.
import sys as _sys

_sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bathymetry as bathy_mod

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Target output files
OUTPUT_DIR = "output"
REPORT_PATH = os.path.join(OUTPUT_DIR, "lock_dimensions_validation_report.md")
HTML_REPORT_PATH = os.path.join(OUTPUT_DIR, "lock_dimensions_validation_report.html")
# Target lock list. Defaults to the copy committed alongside this script; override
# with --excel or the LOCK_VALIDATION_EXCEL environment variable.
DEFAULT_EXCEL = os.path.join(SCRIPT_DIR, "data", "Chamber_comparison.xlsx")
LOCAL_EXCEL = os.environ.get("LOCK_VALIDATION_EXCEL", DEFAULT_EXCEL)
BIVAS_DB = "reference/Bivas.5.10.1.sqlite"
FIS_CHAMBERS = "output/fis-export/chamber.geoparquet"
FIS_SECTIONS = "output/fis-export/section.geoparquet"
EURIS_DIR = "output/euris-export"
AIMED_LEVELS = "output/fis-export/aimedlevel.geoparquet"
AIMED_WATERLEVELS = "output/fis-export/aimedwaterlevel.geoparquet"


def find_euris_chambers(euris_dir=EURIS_DIR, country="NL"):
    """Return the newest EURIS LockChamber export for a country code."""
    pattern = os.path.join(euris_dir, f"LockChamber_{country}_*.geojson")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No EURIS lock chamber files match: {pattern}")
    return max(files, key=os.path.getmtime)


def oriented_bbox_dims(geom):
    """Length and width (m) of a chamber polygon's minimum rotated rectangle.

    This is an independent *geometric* measurement of the physical footprint,
    derived from the chamber polygon rather than the FIS attribute. It runs
    systematically ~8-15% larger than the FIS structural length because it
    includes the lock walls, so it is used as a per-chamber cross-check and
    gross-error detector, not as an exact match against FIS.
    """
    from shapely import minimum_rotated_rectangle

    if geom is None or geom.is_empty:
        return None, None
    rect = minimum_rotated_rectangle(geom)
    if not hasattr(rect, "exterior") or rect.exterior is None:
        return None, None
    xs, ys = rect.exterior.coords.xy
    edges = [math.hypot(xs[i + 1] - xs[i], ys[i + 1] - ys[i]) for i in range(4)]
    return max(edges), min(edges)


def parse_note_sill_nap(note_text, side):
    """Parse FIS Note for explicit NAP sill heights.

    side: 'bobi' (boven/binnen) or 'bebu' (beneden/buiten).
    Returns (nap_float, source_str) or (None, None).
    """
    if not note_text or not isinstance(note_text, str):
        return None, None
    note = note_text.replace("\r\n", " ").replace("\n", " ")

    side_kw = r"boven" if side == "bobi" else r"beneden"

    # Pattern 1: "Drempeldiepte boven... Drempelhoogte NAP+ X,XX m" (most common)
    m = re.search(
        rf"[Dd]rempeldiepte\s+{side_kw}.{{0,80}}?Drempelhoogte\s+NAP\+\s*([\d,]+)",
        note,
        re.IGNORECASE,
    )
    if m:
        try:
            return float(m.group(1).replace(",", ".")), "Note (NAP+ expliciet)"
        except ValueError:
            pass

    # Pattern 2: "Drempeldiepte boven... Drempelhoogte X,XX m+NAP" (Born variant)
    m2 = re.search(
        rf"[Dd]rempeldiepte\s+{side_kw}.{{0,80}}?Drempelhoogte\s+([\d,]+)\s*m\+NAP",
        note,
        re.IGNORECASE,
    )
    if m2:
        try:
            return float(m2.group(1).replace(",", ".")), "Note (m+NAP expliciet)"
        except ValueError:
            pass

    # Pattern 3: "Drempels SP-X,XX m=NAP+Y,YY m" (applies to both sides equally)
    m3 = re.search(r"[Dd]rempel[s]?\s+\S+-[\d,]+\s*m?\s*=\s*NAP\+([\d,]+)", note)
    if m3:
        try:
            return float(m3.group(1).replace(",", ".")), "Note (NAP= expliciet)"
        except ValueError:
            pass

    return None, None


def resolve_sill_nap(
    raw_val, height_ref_str, fairway_ref_level, peil_side, note_text, side
):
    """Convert a FIS sill value to an absolute NAP height with explicit source tracking.

    Precedence (highest to lowest confidence):
    1. Explicit "Drempelhoogte NAP+X" in the Note field
    2. HeightReferenceLevel = 'NAP' → value is already a NAP height
    3. Positive value + known streefpeil → depth below KP/SP, compute NAP
    4. Negative value → probably a NAP height, but flag as uncertain

    Returns (nap_height_or_None, source_str, is_uncertain: bool).
    """
    # 1. Note field
    note_val, note_src = parse_note_sill_nap(note_text, side)
    if note_val is not None:
        return note_val, note_src, False

    if raw_val is None:
        return None, "Geen waarde in FIS", True
    try:
        val = float(raw_val)
        if pd.isna(val):
            return None, "Geen waarde in FIS", True
    except (ValueError, TypeError):
        return None, "Geen geldige waarde", True

    # 2. Explicit NAP reference
    ref = str(height_ref_str).strip().upper() if height_ref_str else ""
    if ref == "NAP":
        return val, "FIS (HeightReferenceLevel=NAP, direct)", False

    # 3. Positive value → depth below local KP/SP reference
    if val > 0 and peil_side is not None:
        try:
            peil = float(peil_side)
            ref_label = fairway_ref_level if fairway_ref_level else "KP/SP"
            return (
                peil - val,
                f"FIS (berekend: streefpeil {peil:.2f} − {val:.2f}m via {ref_label})",
                False,
            )
        except (ValueError, TypeError):
            pass

    # 4. Negative value → probably already a NAP height, but we cannot be sure
    if val < 0:
        return val, "FIS (vermoedelijk NAP-hoogte, negatieve waarde — onzeker)", True

    return None, "Onbepaald (waarde=0 of onbekend)", True


IMAGES_DIR = os.path.join(OUTPUT_DIR, "images")
AERIALS_DIR = os.path.join(IMAGES_DIR, "aerials")
CHARTS_DIR = os.path.join(IMAGES_DIR, "charts")
MANUAL_CHECKS_DIR = os.path.join(OUTPUT_DIR, "manual_checks")

# Create output dirs
os.makedirs(AERIALS_DIR, exist_ok=True)
os.makedirs(CHARTS_DIR, exist_ok=True)
os.makedirs(MANUAL_CHECKS_DIR, exist_ok=True)


CONCEPT_DIAGRAM_PATH = os.path.join(IMAGES_DIR, "concept_diagram.png")


def generate_concept_diagram():
    """Static explanatory figure defining all sill/depth terminology.

    Uses illustrative (not real) values so every label can be shown clearly.
    Generated once; cached by file existence.
    """
    if os.path.exists(CONCEPT_DIAGRAM_PATH):
        return "images/concept_diagram.png"

    # ── illustrative geometry ───────────────────────────────────────────────
    # x-coordinates: approach Bo | Bo gate | chamber | Be gate | approach Be
    x_bo_start, x_bo_gate = 0, 30
    x_be_gate, x_be_end = 230, 260
    x_mid = (x_bo_gate + x_be_gate) / 2

    peil_bo = 5.0  # streefpeil Bovenhoofd (NAP m)
    peil_be = 2.0  # streefpeil Benedenhoofd (NAP m)
    sill_bo = -2.5  # drempelkruin Bo (NAP m)
    sill_be = -1.5  # drempelkruin Be (NAP m)
    floor_chamber = -4.5  # chamber floor
    floor_approach = -5.5  # approach channel floor

    c_bo = "#2b5c8f"
    c_be = "#20b2aa"
    c_floor = "#5b4a2e"
    c_sill = "#c0392b"

    fig, ax = plt.subplots(figsize=(13, 5.5))
    ax.set_xlim(-10, 270)
    ax.set_ylim(-6.5, 8.5)
    ax.set_aspect("auto")
    ax.axis("off")

    # ── NAP datum ───────────────────────────────────────────────────────────
    ax.axhline(0, color="black", lw=0.8, linestyle="--", alpha=0.5, zorder=1)
    ax.text(263, 0.1, "NAP = 0 m", fontsize=7.5, va="bottom", color="black", alpha=0.7)

    # ── approach-channel floor ───────────────────────────────────────────────
    ax.fill_between(
        [x_bo_start, x_bo_gate],
        floor_approach,
        -7,
        color="#c8a87a",
        alpha=0.35,
        zorder=2,
    )
    ax.fill_between(
        [x_be_gate, x_be_end], floor_approach, -7, color="#c8a87a", alpha=0.35, zorder=2
    )
    ax.plot(
        [x_bo_start, x_bo_gate],
        [floor_approach, floor_approach],
        color=c_floor,
        lw=1.5,
        zorder=3,
    )
    ax.plot(
        [x_be_gate, x_be_end],
        [floor_approach, floor_approach],
        color=c_floor,
        lw=1.5,
        zorder=3,
    )

    # ── sill crests (raised threshold at each gate) ───────────────────────
    sill_w = 8
    # Bo sill block
    ax.fill_between(
        [x_bo_gate - sill_w, x_bo_gate],
        floor_approach,
        sill_bo,
        color="#c8a87a",
        alpha=0.6,
        zorder=4,
    )
    ax.plot(
        [x_bo_start, x_bo_gate - sill_w],
        [floor_approach, floor_approach],
        color=c_floor,
        lw=1.5,
        zorder=5,
    )
    ax.plot(
        [x_bo_gate - sill_w, x_bo_gate - sill_w, x_bo_gate],
        [floor_approach, sill_bo, sill_bo],
        color=c_floor,
        lw=1.5,
        zorder=5,
    )
    # Be sill block
    ax.fill_between(
        [x_be_gate, x_be_gate + sill_w],
        floor_approach,
        sill_be,
        color="#c8a87a",
        alpha=0.6,
        zorder=4,
    )
    ax.plot(
        [x_be_gate + sill_w, x_be_end],
        [floor_approach, floor_approach],
        color=c_floor,
        lw=1.5,
        zorder=5,
    )
    ax.plot(
        [x_be_gate, x_be_gate + sill_w, x_be_gate + sill_w],
        [sill_be, sill_be, floor_approach],
        color=c_floor,
        lw=1.5,
        zorder=5,
    )

    # ── chamber floor ────────────────────────────────────────────────────────
    ax.fill_between(
        [x_bo_gate, x_be_gate], floor_chamber, -7, color="#c8a87a", alpha=0.35, zorder=2
    )
    ax.plot(
        [x_bo_gate, x_be_gate],
        [floor_chamber, floor_chamber],
        color=c_floor,
        lw=1.5,
        zorder=3,
    )

    # ── water surfaces ───────────────────────────────────────────────────────
    # Bo approach water
    ax.fill_between(
        [x_bo_start, x_bo_gate], sill_bo, peil_bo, color=c_bo, alpha=0.18, zorder=3
    )
    ax.plot([x_bo_start, x_bo_gate], [peil_bo, peil_bo], color=c_bo, lw=2.0, zorder=4)
    # Be approach water
    ax.fill_between(
        [x_be_gate, x_be_end], sill_be, peil_be, color=c_be, alpha=0.18, zorder=3
    )
    ax.plot([x_be_gate, x_be_end], [peil_be, peil_be], color=c_be, lw=2.0, zorder=4)

    # ── gate symbols (vertical thick lines) ──────────────────────────────────
    for x in [x_bo_gate, x_be_gate]:
        ax.plot(
            [x, x],
            [floor_chamber - 0.2, max(peil_bo, peil_be) + 0.3],
            color="#1a1a2e",
            lw=3.5,
            zorder=6,
            solid_capstyle="butt",
        )

    # ── annotations: gate labels ─────────────────────────────────────────────
    ax.text(
        x_bo_gate,
        max(peil_bo, peil_be) + 0.6,
        "Bovenhoofd (Bo)",
        ha="center",
        va="bottom",
        fontsize=9,
        fontweight="bold",
        color=c_bo,
    )
    ax.text(
        x_be_gate,
        max(peil_bo, peil_be) + 0.6,
        "Benedenhoofd (Be)",
        ha="center",
        va="bottom",
        fontsize=9,
        fontweight="bold",
        color=c_be,
    )
    ax.text(
        x_mid,
        max(peil_bo, peil_be) + 0.6,
        "Sluiskolk",
        ha="center",
        va="bottom",
        fontsize=9,
        color="#475569",
    )

    # ── streefpeil labels ────────────────────────────────────────────────────
    ax.annotate(
        f"Streefpeil Bo\n{peil_bo:+.1f} m NAP",
        xy=(x_bo_gate / 2, peil_bo),
        xytext=(x_bo_gate / 2, peil_bo + 1.2),
        ha="center",
        va="bottom",
        fontsize=8,
        color=c_bo,
        arrowprops=dict(arrowstyle="-", color=c_bo, lw=0.8),
    )
    ax.annotate(
        f"Streefpeil Be\n{peil_be:+.1f} m NAP",
        xy=((x_be_gate + x_be_end) / 2, peil_be),
        xytext=((x_be_gate + x_be_end) / 2, peil_be + 1.2),
        ha="center",
        va="bottom",
        fontsize=8,
        color=c_be,
        arrowprops=dict(arrowstyle="-", color=c_be, lw=0.8),
    )

    # ── drempelkruin labels ──────────────────────────────────────────────────
    ax.annotate(
        f"Drempelkruin Bo\n{sill_bo:+.1f} m NAP",
        xy=(x_bo_gate - sill_w / 2, sill_bo),
        xytext=(x_bo_gate - sill_w / 2 - 20, sill_bo - 1.2),
        ha="right",
        va="top",
        fontsize=8,
        color=c_sill,
        arrowprops=dict(arrowstyle="->", color=c_sill, lw=0.9),
    )
    ax.annotate(
        f"Drempelkruin Be\n{sill_be:+.1f} m NAP",
        xy=(x_be_gate + sill_w / 2, sill_be),
        xytext=(x_be_gate + sill_w / 2 + 20, sill_be - 1.2),
        ha="left",
        va="top",
        fontsize=8,
        color=c_sill,
        arrowprops=dict(arrowstyle="->", color=c_sill, lw=0.9),
    )

    # ── waterdiepte boven drempel: double-headed arrows ──────────────────────
    def depth_arrow(x, y_bottom, y_top, color, label, side="left"):
        ax.annotate(
            "",
            xy=(x, y_top),
            xytext=(x, y_bottom),
            arrowprops=dict(arrowstyle="<->", color=color, lw=1.2),
        )
        dx = -12 if side == "left" else 12
        ha = "right" if side == "left" else "left"
        ax.text(
            x + dx,
            (y_bottom + y_top) / 2,
            label,
            ha=ha,
            va="center",
            fontsize=7.5,
            color=color,
            bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.85),
        )

    depth_arrow(
        x_bo_gate - sill_w / 2 - 2,
        sill_bo,
        peil_bo,
        c_bo,
        f"Waterdiepte\nboven drempel Bo\n= {peil_bo - sill_bo:.1f} m",
        "left",
    )
    depth_arrow(
        x_be_gate + sill_w / 2 + 2,
        sill_be,
        peil_be,
        c_be,
        f"Waterdiepte\nboven drempel Be\n= {peil_be - sill_be:.1f} m",
        "right",
    )

    # ── "zee-sluis" variant label ────────────────────────────────────────────
    ax.text(
        x_mid,
        floor_chamber - 0.6,
        "Zeesluis variant: Binnenhoofd (Bi) = Bo  |  Buitenhoofd (Bu) = Be",
        ha="center",
        va="top",
        fontsize=7.5,
        color="#64748b",
        style="italic",
    )

    ax.set_title(
        "Terminologie sluisdrempels — uitlegfiguur (illustratieve waarden)",
        fontsize=11,
        fontweight="bold",
        pad=12,
    )
    fig.tight_layout()
    plt.savefig(CONCEPT_DIAGRAM_PATH, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return "images/concept_diagram.png"


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
        r = requests.get(url, timeout=30)
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


FOOTPRINTS_DIR = os.path.join(IMAGES_DIR, "footprints")
os.makedirs(FOOTPRINTS_DIR, exist_ok=True)


def generate_footprint_map(
    sluis_clean,
    chamber_clean,
    geom_rd,
    fis_struct_len,
    fis_struct_wid,
    centroid_rd,
    obb_len=None,
    obb_wid=None,
    profile_line_rd=None,
):
    """Plot FIS chamber polygon + minimum rotated rectangle on the PDOK aerial background."""
    filename = f"{sluis_clean}_{chamber_clean}_footprint.png"
    path = os.path.join(FOOTPRINTS_DIR, filename)
    if os.path.exists(path):
        return f"images/footprints/{filename}"

    aerial_rel = download_aerial_photo(sluis_clean, chamber_clean, centroid_rd)
    aerial_abs = os.path.join(OUTPUT_DIR, aerial_rel) if aerial_rel else None
    if not aerial_abs or not os.path.exists(aerial_abs):
        return None

    half_size = 350
    xmin = centroid_rd.x - half_size
    xmax = centroid_rd.x + half_size
    ymin = centroid_rd.y - half_size
    ymax = centroid_rd.y + half_size

    try:
        img_arr = plt.imread(aerial_abs)
    except Exception:
        return None

    fig, ax = plt.subplots(figsize=(5, 5))
    ax.imshow(img_arr, extent=[xmin, xmax, ymin, ymax], origin="upper")

    if geom_rd is not None and not geom_rd.is_empty:
        try:
            xs, ys = geom_rd.exterior.coords.xy
            ax.plot(
                list(xs), list(ys), color="#00aaff", linewidth=2.5, label="FIS kolk"
            )
            ax.fill(list(xs), list(ys), color="#00aaff", alpha=0.25)
        except Exception:
            pass

        # Draw minimum rotated rectangle (OBB) as dashed orange line.
        # Skip if geometry is near-circular (aspect ratio < 3): the FIS polygon
        # is then not a chamber shape and the OBB would be misleading.
        try:
            obb_geom = geom_rd.minimum_rotated_rectangle
            if hasattr(obb_geom, "exterior") and obb_geom.exterior is not None:
                ox, oy = obb_geom.exterior.coords.xy
                edges = [
                    math.hypot(ox[i + 1] - ox[i], oy[i + 1] - oy[i]) for i in range(4)
                ]
                L_obb, W_obb = max(edges), min(edges)
                if W_obb > 0 and L_obb / W_obb >= 3:
                    ax.plot(
                        list(ox),
                        list(oy),
                        color="#ff8800",
                        linewidth=1.5,
                        linestyle="--",
                        label="Min. rechthoek",
                    )
                else:
                    ax.text(
                        xmin + 8,
                        ymin + 8,
                        "⚠ Geometrie niet-rechthoekig\n(polygoon ≠ sluiskolk)",
                        fontsize=7,
                        va="bottom",
                        color="#ff8800",
                        bbox=dict(
                            boxstyle="round,pad=0.2", facecolor="#0f172a", alpha=0.8
                        ),
                    )
        except Exception:
            pass

    # Draw bathymetry profile centreline as thin white line
    if profile_line_rd is not None:
        try:
            px, py = profile_line_rd.coords.xy
            ax.plot(
                list(px),
                list(py),
                color="white",
                linewidth=1.2,
                linestyle="-",
                alpha=0.75,
                label="Profiel-as",
                zorder=5,
            )
        except Exception:
            pass

    ann = []
    if fis_struct_len is not None:
        ann.append(
            f"FIS: {fis_struct_len:.0f} × {fis_struct_wid:.0f} m"
            if fis_struct_wid
            else f"FIS L: {fis_struct_len:.0f} m"
        )
    if obb_len is not None:
        ann.append(
            f"Polygoon: {obb_len:.0f} × {obb_wid:.0f} m"
            if obb_wid
            else f"Polygoon L: {obb_len:.0f} m"
        )
    if ann:
        ax.text(
            xmin + 8,
            ymax - 8,
            "\n".join(ann),
            fontsize=8,
            va="top",
            color="white",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#0f172a", alpha=0.8),
        )

    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.legend(loc="lower right", fontsize=7, framealpha=0.85)
    ax.set_title(
        f"Sluiskolk: {sluis_clean} ({chamber_clean})", fontsize=9, fontweight="bold"
    )
    fig.tight_layout(pad=0.3)
    plt.savefig(path, dpi=150)
    plt.close(fig)
    return f"images/footprints/{filename}"


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


SIDEVIEWS_DIR = os.path.join(IMAGES_DIR, "sideviews")
DECISIONS_DIR = os.path.join(IMAGES_DIR, "decisions")
os.makedirs(SIDEVIEWS_DIR, exist_ok=True)
os.makedirs(DECISIONS_DIR, exist_ok=True)


def generate_sideview_chart(
    sluis_clean,
    chamber_clean,
    waterway_hoog,
    peil_hoog,
    waterway_laag,
    peil_laag,
    sill_bobi_nap,
    sill_bobi_uncertain,
    sill_bebu_nap,
    sill_bebu_uncertain,
    fis_struct_len=None,
    bobi_measured=None,
    bebu_measured=None,
    lock_type="river",
    bathy_result=None,
):
    """Engineering cross-section with optional bottom-profile panel below."""
    filename = f"{sluis_clean}_{chamber_clean}_sideview.png"
    path = os.path.join(SIDEVIEWS_DIR, filename)
    if os.path.exists(path):
        return f"images/sideviews/{filename}"

    levels = [
        v
        for v in [
            peil_hoog,
            peil_laag,
            sill_bobi_nap,
            sill_bebu_nap,
            bobi_measured,
            bebu_measured,
        ]
        if v is not None
    ]
    if not levels:
        return None

    # Side terminology: river/canal vs sea lock
    if lock_type == "sea":
        label_bobi = "Binnenhoofd (Bi)"
        label_bebu = "Buitenhoofd (Bu)"
        abbr_bobi = "Bi"
        abbr_bebu = "Bu"
    else:
        label_bobi = "Bovenhoofd (Bo)"
        label_bebu = "Benedenhoofd (Be)"
        abbr_bobi = "Bo"
        abbr_bebu = "Be"

    # Normalized x layout (total width = 20)
    # 0-2: BoBi water zone | 2-3: left wall | 3-17: chamber | 17-18: right wall | 18-20: BeBu water zone
    XL_W0, _XL_W1 = 0, 2  # BoBi water
    XL_WL0, XL_WL1 = 2, 3  # left wall
    XCH0, XCH1 = 3, 17  # chamber
    XR_WL0, XR_WL1 = 17, 18  # right wall
    _XR_W0, XR_W1 = 18, 20  # BeBu water

    lowest = min(levels)
    highest = max(levels)
    floor_nap = lowest - 1.5
    wall_top = highest + 1.0

    # Determine if we have a valid profile to show as a second panel
    profile_data = None
    if bathy_result:
        _prof = bathy_result.get("profile", [])
        _ys = [v for _, v in _prof if v is not None]
        if _ys:
            profile_data = bathy_result

    if profile_data is not None:
        import matplotlib.gridspec as gridspec

        fig = plt.figure(figsize=(10, 7), constrained_layout=True)
        gs = gridspec.GridSpec(2, 1, height_ratios=[2, 1], figure=fig)
        ax = fig.add_subplot(gs[0])
        ax_prof = fig.add_subplot(gs[1])
    else:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax_prof = None

    wall_color = "#6b7280"
    c_bobi = "#2b5c8f"
    c_bebu = "#20b2aa"
    c_sill_bobi = "#c0392b"
    c_sill_bebu = "#e67e22"

    # Floor slab
    ax.fill_between(
        [XL_WL0, XR_WL1], floor_nap, floor_nap + 0.4, color=wall_color, zorder=3
    )

    # Left wall
    ax.fill_betweenx(
        [floor_nap, wall_top], XL_WL0, XL_WL1, color=wall_color, alpha=0.85, zorder=3
    )
    # Right wall
    ax.fill_betweenx(
        [floor_nap, wall_top], XR_WL0, XR_WL1, color=wall_color, alpha=0.85, zorder=3
    )

    # BoBi water body
    if peil_hoog is not None:
        ax.fill_between(
            [XL_W0, XL_WL0], floor_nap, peil_hoog, color=c_bobi, alpha=0.2, zorder=2
        )
        ax.plot([XL_W0, XL_WL0], [peil_hoog, peil_hoog], color=c_bobi, lw=2, zorder=4)
        name_str = (waterway_hoog or label_bobi)[:22]
        ax.text(
            -0.2,
            peil_hoog + 0.05,
            f"{name_str}\nStreefpeil: {peil_hoog:+.2f} m NAP",
            fontsize=7,
            ha="right",
            va="bottom",
            color=c_bobi,
        )

    # BeBu water body
    if peil_laag is not None:
        ax.fill_between(
            [XR_WL1, XR_W1], floor_nap, peil_laag, color=c_bebu, alpha=0.2, zorder=2
        )
        ax.plot([XR_WL1, XR_W1], [peil_laag, peil_laag], color=c_bebu, lw=2, zorder=4)
        name_str = (waterway_laag or label_bebu)[:22]
        ax.text(
            20.2,
            peil_laag + 0.05,
            f"{name_str}\nStreefpeil: {peil_laag:+.2f} m NAP",
            fontsize=7,
            ha="left",
            va="bottom",
            color=c_bebu,
        )

    # BoBi sill at left gate
    if sill_bobi_nap is not None:
        lstyle = ":" if sill_bobi_uncertain else "-"
        # Sill block: from floor to sill level, at gate position
        ax.fill_between(
            [XL_WL0 - 0.4, XL_WL1 + 0.4],
            floor_nap + 0.4,
            sill_bobi_nap,
            color=c_sill_bobi,
            alpha=0.5,
            zorder=4,
        )
        ax.hlines(
            sill_bobi_nap,
            XL_WL0 - 0.4,
            XL_WL1 + 0.4,
            colors=c_sill_bobi,
            lw=2.5,
            linestyles=lstyle,
            zorder=5,
        )
        unc = " (?)" if sill_bobi_uncertain else ""
        ax.text(
            (XL_WL0 + XL_WL1) / 2,
            sill_bobi_nap + 0.1,
            f"{abbr_bobi} (FIS): {sill_bobi_nap:+.2f} m NAP{unc}",
            fontsize=6.5,
            ha="center",
            va="bottom",
            color=c_sill_bobi,
            bbox=dict(boxstyle="round,pad=0.1", facecolor="white", alpha=0.75),
        )
        # Depth arrow from sill to water level
        if peil_hoog is not None and peil_hoog > sill_bobi_nap:
            mid_x = 1.5
            ax.annotate(
                "",
                xy=(mid_x, sill_bobi_nap),
                xytext=(mid_x, peil_hoog),
                arrowprops=dict(arrowstyle="<->", color=c_sill_bobi, lw=1.2),
            )
            depth = peil_hoog - sill_bobi_nap
            ax.text(
                mid_x - 0.15,
                (sill_bobi_nap + peil_hoog) / 2,
                f"{depth:.2f} m",
                fontsize=6.5,
                ha="right",
                va="center",
                color=c_sill_bobi,
            )

    # BeBu sill at right gate
    if sill_bebu_nap is not None:
        lstyle = ":" if sill_bebu_uncertain else "-"
        ax.fill_between(
            [XR_WL0 - 0.4, XR_WL1 + 0.4],
            floor_nap + 0.4,
            sill_bebu_nap,
            color=c_sill_bebu,
            alpha=0.5,
            zorder=4,
        )
        ax.hlines(
            sill_bebu_nap,
            XR_WL0 - 0.4,
            XR_WL1 + 0.4,
            colors=c_sill_bebu,
            lw=2.5,
            linestyles=lstyle,
            zorder=5,
        )
        unc = " (?)" if sill_bebu_uncertain else ""
        ax.text(
            (XR_WL0 + XR_WL1) / 2,
            sill_bebu_nap + 0.1,
            f"{abbr_bebu} (FIS): {sill_bebu_nap:+.2f} m NAP{unc}",
            fontsize=6.5,
            ha="center",
            va="bottom",
            color=c_sill_bebu,
            bbox=dict(boxstyle="round,pad=0.1", facecolor="white", alpha=0.75),
        )
        if peil_laag is not None and peil_laag > sill_bebu_nap:
            mid_x = 18.5
            ax.annotate(
                "",
                xy=(mid_x, sill_bebu_nap),
                xytext=(mid_x, peil_laag),
                arrowprops=dict(arrowstyle="<->", color=c_sill_bebu, lw=1.2),
            )
            depth = peil_laag - sill_bebu_nap
            ax.text(
                mid_x + 0.15,
                (sill_bebu_nap + peil_laag) / 2,
                f"{depth:.2f} m",
                fontsize=6.5,
                ha="left",
                va="center",
                color=c_sill_bebu,
            )

    # NAP datum line
    if floor_nap < 0 < wall_top:
        ax.axhline(0, color="black", lw=1.0, linestyle="--", zorder=3)
        ax.text(20.2, 0, "NAP", fontsize=7, va="center", ha="left", color="black")

    # Length annotation
    if fis_struct_len is not None:
        arr_y = floor_nap + 0.7
        ax.annotate(
            "",
            xy=(XCH1, arr_y),
            xytext=(XCH0, arr_y),
            arrowprops=dict(arrowstyle="<->", color="black", lw=1.2),
        )
        ax.text(
            (XCH0 + XCH1) / 2,
            arr_y + 0.15,
            f"Constructielengte: {fis_struct_len:.0f} m",
            ha="center",
            fontsize=8,
            color="black",
        )

    # Side labels at top
    ax.text(
        (XL_W0 + XL_WL0) / 2,
        wall_top + 0.2,
        f"{label_bobi}\n(hoge zijde)",
        ha="center",
        fontsize=7.5,
        color=c_bobi,
        fontweight="bold",
    )
    ax.text(
        (XR_WL1 + XR_W1) / 2,
        wall_top + 0.2,
        f"{label_bebu}\n(lage zijde)",
        ha="center",
        fontsize=7.5,
        color=c_bebu,
        fontweight="bold",
    )

    # Measured bottom at gate from bathymetry (1m raster)
    c_bathy = "#5b4a2e"
    if bobi_measured is not None:
        ax.hlines(
            bobi_measured,
            XL_WL0 - 0.6,
            XCH0 + 1.0,
            colors=c_bathy,
            lw=1.5,
            linestyles="--",
            zorder=5,
        )
        ax.text(
            XCH0 + 1.1,
            bobi_measured,
            f"Meting: {bobi_measured:+.2f} m",
            fontsize=6,
            va="center",
            color=c_bathy,
        )
    if bebu_measured is not None:
        ax.hlines(
            bebu_measured,
            XCH1 - 1.0,
            XR_WL1 + 0.6,
            colors=c_bathy,
            lw=1.5,
            linestyles="--",
            zorder=5,
        )
        ax.text(
            XCH1 - 1.1,
            bebu_measured,
            f"Meting: {bebu_measured:+.2f} m",
            fontsize=6,
            va="center",
            ha="right",
            color=c_bathy,
        )

    ax.set_xlim(-1.5, 21.5)
    ax.set_ylim(floor_nap - 0.3, wall_top + 0.8)
    ax.set_ylabel("Hoogte t.o.v. NAP (m)")
    ax.set_xticks([])
    ax.set_title(
        f"Doorsnede: {sluis_clean} ({chamber_clean})", fontsize=10, fontweight="bold"
    )
    ax.grid(True, axis="y", linestyle="--", alpha=0.25, zorder=1)

    # Bottom panel: bathymetry profile along chamber axis
    if ax_prof is not None and profile_data is not None:
        _prof = profile_data["profile"]
        _gate1_d = profile_data["gate1_distance"]
        _gate2_d = profile_data["gate2_distance"]
        _crest1 = profile_data.get("crest1")
        _crest2 = profile_data.get("crest2")

        _xs = [d for d, _ in _prof]
        _ys_raw = [v for _, v in _prof]
        _ys = [y if y is not None else float("nan") for y in _ys_raw]
        _valid = [v for v in _ys if v == v]

        ax_prof.plot(_xs, _ys, color="#5b4a2e", lw=1.5, label="Bodem 1m-kaart")
        _all_ref = _valid[:]
        if peil_hoog is not None:
            _all_ref.append(peil_hoog)
        if peil_laag is not None:
            _all_ref.append(peil_laag)
        _y_lo = min(_all_ref) - 0.5 if _all_ref else -1
        _y_hi = max(_all_ref) + 0.5 if _all_ref else 1
        ax_prof.fill_between(_xs, _ys, _y_lo, color="#c8a87a", alpha=0.25)
        ax_prof.set_ylim(_y_lo, _y_hi)

        ax_prof.axvline(
            _gate1_d, color=c_bobi, lw=1.2, linestyle="--", label=f"{abbr_bobi} deur"
        )
        ax_prof.axvline(
            _gate2_d, color=c_bebu, lw=1.2, linestyle="--", label=f"{abbr_bebu} deur"
        )

        if _crest1 is not None:
            ax_prof.scatter(
                [_gate1_d],
                [_crest1],
                color=c_bobi,
                zorder=6,
                s=35,
                label=f"Meting {abbr_bobi}: {_crest1:+.2f} m",
            )
        if _crest2 is not None:
            ax_prof.scatter(
                [_gate2_d],
                [_crest2],
                color=c_bebu,
                zorder=6,
                s=35,
                label=f"Meting {abbr_bebu}: {_crest2:+.2f} m",
            )

        if sill_bobi_nap is not None:
            ax_prof.axhline(
                sill_bobi_nap,
                color=c_bobi,
                lw=0.9,
                linestyle=":",
                label=f"FIS {abbr_bobi}: {sill_bobi_nap:+.2f} m",
            )
        if sill_bebu_nap is not None:
            ax_prof.axhline(
                sill_bebu_nap,
                color=c_bebu,
                lw=0.9,
                linestyle=":",
                label=f"FIS {abbr_bebu}: {sill_bebu_nap:+.2f} m",
            )

        if peil_hoog is not None:
            ax_prof.axhline(
                peil_hoog,
                color=c_bobi,
                lw=0.8,
                linestyle="-.",
                alpha=0.5,
                label=f"Peil {abbr_bobi}: {peil_hoog:+.2f} m",
            )
        if peil_laag is not None:
            ax_prof.axhline(
                peil_laag,
                color=c_bebu,
                lw=0.8,
                linestyle="-.",
                alpha=0.5,
                label=f"Peil {abbr_bebu}: {peil_laag:+.2f} m",
            )

        if _y_lo < 0 < _y_hi:
            ax_prof.axhline(0, color="black", lw=0.7, linestyle="--", alpha=0.4)
            ax_prof.text(
                _xs[-1], 0.05, "NAP", fontsize=6, ha="right", color="black", alpha=0.5
            )

        ax_prof.set_xlabel("Afstand langs kolk-as (m)", fontsize=8)
        ax_prof.set_ylabel("NAP (m)", fontsize=8)
        ax_prof.set_title("Bodemprofiel (bodemhoogte_1mtr)", fontsize=8)
        ax_prof.legend(fontsize=6, loc="upper right", ncol=2)
        ax_prof.grid(True, linestyle="--", alpha=0.2)
        ax_prof.tick_params(labelsize=7)

    if ax_prof is None:
        fig.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return f"images/sideviews/{filename}"


def generate_decision_figure(
    sluis_clean,
    chamber_clean,
    fis_len,
    osm_len,
    bivas_len,
    survey_len,
    euris_neq_fis,
    n_violations,
    n_checks,
    is_single_kolk,
):
    """Per-lock evidence panel: available sources and their agreement with FIS."""
    filename = f"{sluis_clean}_{chamber_clean}_decision.png"
    path = os.path.join(DECISIONS_DIR, filename)
    if os.path.exists(path):
        return f"images/decisions/{filename}"

    def _f(v):
        try:
            return float(v) if v is not None and pd.notna(v) else None
        except Exception:
            return None

    fis_val = _f(fis_len)
    sources = {
        "FIS": fis_val,
        "OSM": _f(osm_len),
        "BIVAS": _f(bivas_len) if is_single_kolk else None,
        "Enquête": _f(survey_len),
    }
    bivas_raw = _f(bivas_len)

    fig, ax = plt.subplots(figsize=(6, 3.5))

    colors = []
    labels = []
    values = []
    for src, val in sources.items():
        labels.append(src)
        if val is None:
            values.append(0)
            colors.append("#cccccc")
        else:
            values.append(val)
            if src == "FIS":
                colors.append("#2b5c8f")
            elif (
                fis_val is not None and abs(val - fis_val) / max(abs(fis_val), 1) < 0.05
            ):
                colors.append("#27ae60")
            else:
                colors.append("#e74c3c")

    x = range(len(labels))
    bars = ax.bar(list(x), values, color=colors, edgecolor="white", linewidth=0.5)

    for bar, val, src in zip(bars, values, labels):
        label_y = bar.get_height()
        if val > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                label_y + 0.5,
                f"{val:.0f} m",
                ha="center",
                va="bottom",
                fontsize=8,
            )
        else:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                1,
                "n.b.",
                ha="center",
                va="bottom",
                fontsize=8,
                color="#999",
            )

    # Note when BIVAS is excluded due to multi-kolk
    if bivas_raw is not None and not is_single_kolk:
        nonzero = [v for v in values if v]
        y_mid = (max(nonzero) * 0.5) if nonzero else 10
        ax.text(
            2,
            y_mid,
            f"BIVAS={bivas_raw:.0f}m\n(niet meegewogen:\nmulti-kolk complex)",
            ha="center",
            va="center",
            fontsize=6.5,
            color="#888",
            bbox=dict(boxstyle="round,pad=0.2", facecolor="#f5f5f5", alpha=0.8),
        )

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_ylabel("Lengte (m)")
    ax.set_title(
        f"Databronnen lengte: {sluis_clean} ({chamber_clean})",
        fontsize=9,
        fontweight="bold",
    )

    if euris_neq_fis:
        ax.text(
            0.01,
            0.97,
            "⚠ EURIS ≠ FIS (data-propagatiefout)",
            transform=ax.transAxes,
            fontsize=7,
            va="top",
            color="#c0392b",
            bbox=dict(boxstyle="round,pad=0.2", facecolor="#fdecea", alpha=0.9),
        )

    # Violations badge
    if n_checks > 0:
        conf_color = (
            "#27ae60"
            if n_violations == 0
            else ("#f39c12" if n_violations < n_checks else "#e74c3c")
        )
        ax.text(
            0.99,
            0.97,
            f"{n_violations}/{n_checks} checks afwijkend",
            transform=ax.transAxes,
            fontsize=8,
            va="top",
            ha="right",
            color=conf_color,
            fontweight="bold",
            bbox=dict(
                boxstyle="round,pad=0.3",
                facecolor="white",
                edgecolor=conf_color,
                alpha=0.9,
            ),
        )

    ax.grid(True, axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close(fig)
    return f"images/decisions/{filename}"


ISSUE_NUMBER = 58
ISSUE_REPO = "RWS-NL/fis-crawler"


def get_issue_body():
    """Fetch the GitHub issue markdown body.

    Prefers the GitHub REST API (works in CI/containers with GITHUB_TOKEN or
    GH_TOKEN) and falls back to the ``gh`` CLI. Returns "" on failure, in which
    case the survey columns simply stay empty.
    """
    api_url = f"https://api.github.com/repos/{ISSUE_REPO}/issues/{ISSUE_NUMBER}"
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        print(f"Fetching Issue {ISSUE_NUMBER} body via GitHub REST API...")
        resp = requests.get(api_url, headers=headers, timeout=15)
        if resp.status_code == 200:
            return resp.json().get("body", "") or ""
        print(f"GitHub API returned HTTP {resp.status_code}; falling back to gh CLI.")
    except Exception as e:
        print(f"GitHub API request failed ({e}); falling back to gh CLI.")

    try:
        res = subprocess.run(
            [
                "gh",
                "issue",
                "view",
                str(ISSUE_NUMBER),
                "--repo",
                ISSUE_REPO,
                "--json",
                "body",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return json.loads(res.stdout).get("body", "")
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


def parse_local_excel(excel_path=LOCAL_EXCEL):
    """Read target lock complexes and manual dimensions from Chamber_comparison.xlsx.

    The "Sluizen" sheet has three side-by-side source blocks below a header row at
    index 4: FIS (cols 0-10), wiki (cols 12-19) and disk (cols 21-28). Columns are
    addressed positionally below (e.g. ``length_18``), matching that layout.
    """
    print(f"Reading {excel_path}...")
    if not os.path.exists(excel_path):
        print(f"Excel file {excel_path} not found.")
        return pd.DataFrame()

    # Read Sluizen sheet
    df = pd.read_excel(excel_path, sheet_name="Sluizen")

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
    # Include query parameters and use higher coordinate precision so nearby
    # parallel chambers (e.g. Weurt Oost/West) don't collide on one cache entry.
    cache_key = f"{lat:.6f}_{lon:.6f}_{radius_m}_{normalize_name(chamber_name or '')}"
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


def normalize_name(value):
    """Lowercase and collapse whitespace for robust chamber-name comparison."""
    return re.sub(r"\s+", " ", str(value).strip().lower())


def match_chamber(matches_bivas, sluis_name, chamber_name):
    """Match a single Excel target to its specific FIS lock chamber.

    The Excel ``name`` column corresponds almost 1:1 with the FIS chamber name
    (e.g. "Oostkolk Volkeraksluizen"), and FIS assigns a distinct ISRS code per
    kolk, so we resolve each kolk individually. We deliberately do NOT match on
    the complex suffix (``chamber_name.split()[-1]``), which collapses every kolk
    of a complex onto a single row.
    """
    names = matches_bivas["name_fis"]
    target = normalize_name(chamber_name)

    # 1. Exact normalized name match (resolves each kolk uniquely).
    match = matches_bivas[names.apply(normalize_name) == target]
    if not match.empty:
        return match

    # 2. Full chamber name contained in the FIS name.
    match = matches_bivas[
        names.str.contains(re.escape(chamber_name), case=False, na=False)
    ]
    if not match.empty:
        return match

    # 3. Loose fallback: complex name + the distinguishing first word
    #    ("Oostkolk", "Westkolk", "Jachtensluis", ...), never the shared suffix.
    kolk_word = chamber_name.split()[0]
    return matches_bivas[
        names.str.contains(re.escape(sluis_name), case=False, na=False)
        & names.str.contains(re.escape(kolk_word), case=False, na=False)
    ]


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


def main(excel_path=LOCAL_EXCEL, euris_path=None):
    print("Starting validation report generator...")

    # 1. Load Survey Data and Target Locks Excel
    issue_body = get_issue_body()
    survey_df = parse_survey_table(issue_body)
    excel_df = parse_local_excel(excel_path)

    # 2. Load and normalize primary GIS datasets
    print("Loading FIS chambers...")
    fis = gpd.read_parquet(FIS_CHAMBERS)
    print("Loading FIS sections (vaarwegroutes)...")
    try:
        sections_rd = gpd.read_parquet(FIS_SECTIONS).to_crs(epsg=28992)
    except Exception as e:
        print(f"Warning: could not load sections ({e}); falling back to OBB axis.")
        sections_rd = None
    euris_path = euris_path or find_euris_chambers()
    print(f"Loading EURIS chambers from {euris_path}...")
    euris = gpd.read_file(euris_path)

    print("Normalizing attributes based on schema.toml...")
    schema = utils.load_schema()
    fis = utils.normalize_attributes(fis, "chambers", schema)
    euris = utils.normalize_attributes(euris, "chambers", schema)

    print("Loading BIVAS locks...")
    bivas_rd = load_bivas_locks()

    # 3. Load aimed water levels for vertical datum conversions
    print("Loading Aimed Levels...")
    aimed_levels = gpd.read_parquet(AIMED_LEVELS)
    # aimedwaterlevel carries the operating range (max +/- deviation) per fairway,
    # used for the water-level cross-section figure.
    aimed_waterlevels = None
    if os.path.exists(AIMED_WATERLEVELS):
        aimed_waterlevels = gpd.read_parquet(AIMED_WATERLEVELS)

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

    # Independent geometric measurement from the chamber footprint polygon.
    obb = fis_rd.geometry.apply(oriented_bbox_dims)
    fis_rd["obb_length"] = obb.apply(lambda t: t[0])
    fis_rd["obb_width"] = obb.apply(lambda t: t[1])

    # Spatially join nearest fairwaydepth ReferenceLevel to each chamber.
    # This tells us whether the sill depth is relative to KP, SP, or NAP.
    FAIRWAY_DEPTH = "output/fis-export/fairwaydepth.geoparquet"
    if os.path.exists(FAIRWAY_DEPTH):
        fd = gpd.read_parquet(FAIRWAY_DEPTH)
        if fd.crs is None:
            fd = fd.set_crs(epsg=4326)
        fd_rd = fd.to_crs(epsg=28992)
        fis_centroids_fd = gpd.GeoDataFrame(
            fis_rd[["id"]], geometry=fis_rd.geometry.centroid, crs="EPSG:28992"
        )
        fd_joined = gpd.sjoin_nearest(
            fis_centroids_fd,
            fd_rd[["ReferenceLevel", "geometry"]],
            how="left",
            max_distance=500,
        ).drop_duplicates(subset=["id"])
        fis_rd = fis_rd.merge(
            fd_joined[["id", "ReferenceLevel"]].rename(
                columns={"ReferenceLevel": "fairway_ref_level"}
            ),
            on="id",
            how="left",
        )
    else:
        fis_rd["fairway_ref_level"] = None

    # Nearest operating-range deviations per chamber (for the cross-section figure).
    if aimed_waterlevels is not None:
        if aimed_waterlevels.crs is None:
            aimed_waterlevels.set_crs(epsg=4326, inplace=True)
        awl_rd = aimed_waterlevels.to_crs(epsg=28992)
        awl_cols = ["MaximumNegativeDeviation", "MaximumPositiveDeviation", "geometry"]
        awl_centroids = gpd.GeoDataFrame(
            fis_rd[["id"]], geometry=fis_rd.geometry.centroid, crs="EPSG:28992"
        )
        joined_range = gpd.sjoin_nearest(
            awl_centroids, awl_rd[awl_cols], how="left", max_distance=1000
        ).drop_duplicates(subset=["id"])
        fis_rd = fis_rd.merge(
            joined_range[
                ["id", "MaximumNegativeDeviation", "MaximumPositiveDeviation"]
            ].rename(
                columns={
                    "MaximumNegativeDeviation": "level_dev_neg",
                    "MaximumPositiveDeviation": "level_dev_pos",
                }
            ),
            on="id",
            how="left",
        )

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

    # Left merge: FIS is the authoritative source; EURIS columns are supplementary.
    # Inner join would silently drop FIS chambers with no EURIS match, hiding their data.
    gis_merged = fis_rd.merge(
        euris_rd, on="isrs_clean", suffixes=("_fis", "_euris"), how="left"
    )
    gis_merged = gpd.GeoDataFrame(gis_merged, geometry="geometry_fis", crs="EPSG:28992")
    n_euris = gis_merged["id_euris"].notna().sum()
    print(f"FIS chambers: {len(gis_merged)}, of which {n_euris} matched to EURIS.")

    # 6. Spatial match to BIVAS
    print("Matching to BIVAS locks spatially...")
    # Buffer BIVAS lines by 150m for matching locks
    bivas_to_join = bivas_rd.rename(
        columns={"id": "bivas_id_orig", "name": "bivas_name_orig"}
    )
    # Perform spatial join using a temporary buffer column so geometry_fis is preserved.
    # Overwriting .geometry would replace the geometry_fis column with circles.
    gis_merged_buffered = gis_merged.copy()
    gis_merged_buffered["_bivas_buffer"] = gis_merged_buffered[
        "geometry_fis"
    ].centroid.buffer(150)
    gis_merged_buffered = gis_merged_buffered.set_geometry(
        "_bivas_buffer", crs="EPSG:28992"
    )
    matches_bivas = gpd.sjoin(
        gis_merged_buffered, bivas_to_join, how="left", rsuffix="bivas"
    )
    # Restore geometry_fis as the active geometry after the join
    matches_bivas = matches_bivas.set_geometry("geometry_fis", crs="EPSG:28992").drop(
        columns=["_bivas_buffer"], errors="ignore"
    )
    # Drop duplicates in case multiple BIVAS arcs matched
    matches_bivas = matches_bivas.drop_duplicates(subset=["id_fis"]).copy()

    # 7. Query OSM dimensions and generate final stats for target locks
    # 7. Query OSM dimensions and generate final stats for target locks
    results_list = []
    bathy_cache = bathy_mod.load_cache()

    # Count chambers per complex to determine if BIVAS check is applicable.
    # BIVAS averages parallel chambers, so BIVAS divergence is only meaningful
    # for single-chamber complexes.
    kolk_count = excel_df.groupby("Sluis_0").size().to_dict()

    total_chambers = len(excel_df)
    print(f"\nValidating dimensions for target locks ({total_chambers} chambers)...")
    from tqdm import tqdm

    for _, row in tqdm(
        excel_df.iterrows(), total=total_chambers, desc="Validating locks"
    ):
        sluis_name = row["Sluis_0"]
        chamber_name = row["name_1"]
        tqdm.write(f"Processing {sluis_name} - {chamber_name}...")
        # Match this Excel target to its specific FIS chamber (per kolk).
        match = match_chamber(matches_bivas, sluis_name, chamber_name)

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

            # EURIS dimensions are already in meters here: schema.toml maps the
            # centimeter source fields (mlengthcm, mwidthcm, avl_length, cl_width)
            # to *_cm canonical names, and normalize_attributes converts them.
            euris_len = m_row.get("dim_usable_length_euris")
            euris_wid = m_row.get("dim_gate_width_euris")
            euris_struct_len = m_row.get("dim_structural_length_euris")
            euris_struct_wid = m_row.get("dim_structural_width_euris")

            # Independent geometric footprint measurement (per chamber).
            # Suppress if geometry is near-circular (L/W < 3) — the FIS polygon is
            # then not a proper chamber shape and the OBB would be meaningless.
            obb_len = m_row.get("obb_length")
            obb_wid = m_row.get("obb_width")
            geom_circular = False
            if obb_len is not None and obb_wid is not None and obb_wid > 0:
                if obb_len / obb_wid < 3:
                    geom_circular = True
                    obb_len = None
                    obb_wid = None

            # EURIS is a derivative of FIS, so agreement is expected and adds no
            # confidence. Only a *mismatch* is a signal (data-propagation error).
            def _diff(a, b, tol):
                return pd.notna(a) and pd.notna(b) and abs(float(a) - float(b)) > tol

            euris_neq_fis = (
                _diff(fis_struct_len, euris_struct_len, 0.5)
                or _diff(fis_struct_wid, euris_struct_wid, 0.1)
                or _diff(fis_len, euris_len, 0.5)
                or _diff(fis_wid, euris_wid, 0.1)
            )

            bivas_len = m_row.get("bivas_length")
            bivas_wid = m_row.get("bivas_width")

            # Sill depth from FIS
            # normalize_attributes converts CamelCase → snake_case, so
            # SillDepthBoBi → sill_depth_bo_bi, SillDepthBeBu → sill_depth_be_bu.
            # ThresholdLowerLevel/ThresholdUpperLevel → dim_threshold_lower/upper (schema.toml).
            sill_raw_bobi = get_val(m_row, "sill_depth_bo_bi")
            if pd.isna(sill_raw_bobi):
                sill_raw_bobi = get_val(m_row, "dim_threshold_lower")
            sill_raw_bebu = get_val(m_row, "sill_depth_be_bu")
            if pd.isna(sill_raw_bebu):
                sill_raw_bebu = get_val(m_row, "dim_threshold_upper")

            height_ref_str = m_row.get("HeightReferenceLevel") or m_row.get(
                "height_reference_level"
            )
            fairway_ref = m_row.get("fairway_ref_level")
            note_text = get_val(m_row, "Note") or get_val(m_row, "note")

            # Retrieve waterway levels for high/low sides
            waterway_hoog, peil_hoog, waterway_laag, peil_laag = get_waterway_levels(
                sluis_name
            )

            # Resolve sill heights to NAP with explicit source tracking
            sill_bobi_nap, sill_bobi_source, sill_bobi_uncertain = resolve_sill_nap(
                sill_raw_bobi, height_ref_str, fairway_ref, peil_hoog, note_text, "bobi"
            )
            sill_bebu_nap, sill_bebu_source, sill_bebu_uncertain = resolve_sill_nap(
                sill_raw_bebu, height_ref_str, fairway_ref, peil_laag, note_text, "bebu"
            )

            # Legacy aliases (used in report)
            threshold_height_bobi = sill_bobi_nap
            threshold_height_bebu = sill_bebu_nap
            ref_bobi = sill_bobi_source
            ref_bebu = sill_bebu_source
            sill_depth_bobi = sill_raw_bobi
            sill_depth_bebu = sill_raw_bebu
            streefpeil = m_row.get("target_water_level_nap")

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
            f_wid_val = to_f(fis_wid)  # deuropening (gate width)
            f_struct_wid_val = to_f(fis_struct_wid)  # kolkbreedte (structural)
            b_wid_val = to_f(bivas_wid)

            # Selection Rule logic for Width (selected = deuropening = FIS GateWidth)
            selected_wid = fis_wid
            selection_method_wid = "Onzeker: Handmatige controle vereist"
            if (
                s_wid_val is not None
                and f_struct_wid_val is not None
                and abs(s_wid_val - f_struct_wid_val) < 0.1
            ):
                # Survey kolkbreedte agrees with FIS structural width — add confidence
                selected_wid = fis_wid
                selection_method_wid = "Enquête kolkbreedte ≈ FIS kolkbreedte"
            elif (
                f_wid_val is not None
                and b_wid_val is not None
                and abs(f_wid_val - b_wid_val) < 0.1
            ):
                # BIVAS deuropening agrees with FIS deuropening
                selected_wid = fis_wid
                selection_method_wid = "FIS deuropening ≈ BIVAS"

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

            # Kolk count for this complex (determines if BIVAS check applies)
            n_kolken = kolk_count.get(sluis_name, 1)
            is_single_kolk = n_kolken == 1

            # Violation counter: track available checks and how many fail
            n_checks = 0
            n_violations = 0
            check_details = []

            def _check(label, val_a, val_b, tol):
                nonlocal n_checks, n_violations
                a, b = to_f(val_a), to_f(val_b)
                if a is not None and b is not None:
                    n_checks += 1
                    if abs(a - b) > tol:
                        n_violations += 1
                        check_details.append(f"✗ {label}")
                    else:
                        check_details.append(f"✓ {label}")

            _check("Enquête≠FIS lengte", survey_len, fis_len, 2.0)
            # Survey "Kolkbreedte" is the structural chamber width; compare with fis_struct_wid
            _check("Enquête≠FIS breedte", survey_wid, fis_struct_wid, 0.1)
            if is_single_kolk:
                _check("BIVAS≠FIS lengte", bivas_len, fis_len, 2.0)
                # BIVAS models navigable width ≈ gate width (deuropening)
                _check("BIVAS≠FIS deuropening", bivas_wid, fis_wid, 0.1)
            disk_len = (
                row.get("schut_lengte_25")
                if pd.notna(row.get("schut_lengte_25", float("nan")))
                else row.get("length_27")
            )
            _check("DISK≠FIS lengte", disk_len, fis_len, 2.0)

            violations_str = (
                f"{n_violations}/{n_checks} checks afwijkend"
                if n_checks > 0
                else "Geen checks beschikbaar"
            )

            # Calculate discrepancy flags
            mismatch_fis_euris = euris_neq_fis  # already computed above

            outlier_survey = False
            if to_f(survey_len) is not None and to_f(fis_len) is not None:
                if abs(to_f(survey_len) - to_f(fis_len)) > 2.0:
                    outlier_survey = True
            if to_f(survey_wid) is not None and to_f(fis_struct_wid) is not None:
                if abs(to_f(survey_wid) - to_f(fis_struct_wid)) > 0.2:
                    outlier_survey = True

            # BIVAS divergence is only meaningful for single-kolk complexes
            outlier_bivas = False
            if is_single_kolk:
                if to_f(fis_len) is not None and to_f(bivas_len) is not None:
                    if abs(to_f(fis_len) - to_f(bivas_len)) > 2.0:
                        outlier_bivas = True
                if to_f(fis_wid) is not None and to_f(bivas_wid) is not None:
                    if abs(to_f(fis_wid) - to_f(bivas_wid)) > 0.2:
                        outlier_bivas = True

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

            # Sample bottom profile from bodemhoogte_1mtr (1m raster, NAP, EPSG:28992)
            bathy_result = bathy_mod.measure_sill_crests(
                m_row["geometry_fis"], bathy_cache, sections_rd=sections_rd
            )
            bathy_mod.save_cache(bathy_cache)

            bobi_measured = None
            bebu_measured = None
            profile_line_rd = None
            if bathy_result is not None:
                bobi_measured = bathy_result["crest1"]
                bebu_measured = bathy_result["crest2"]
                profile_line_rd = bathy_result.get("line")

            footprint_path = generate_footprint_map(
                sluis_clean,
                chamber_clean,
                m_row["geometry_fis"],
                fis_struct_len,
                fis_struct_wid,
                m_row["geometry_fis"].centroid,
                obb_len=obb_len,
                obb_wid=obb_wid,
                profile_line_rd=profile_line_rd,
            )

            # Generate side-view (with embedded profile panel when bathymetry available)
            sideview_path = generate_sideview_chart(
                sluis_clean,
                chamber_clean,
                waterway_hoog,
                peil_hoog,
                waterway_laag,
                peil_laag,
                sill_bobi_nap,
                sill_bobi_uncertain,
                sill_bebu_nap,
                sill_bebu_uncertain,
                fis_struct_len=fis_struct_len,
                bobi_measured=bobi_measured,
                bebu_measured=bebu_measured,
                bathy_result=bathy_result,
            )
            decision_path = generate_decision_figure(
                sluis_clean,
                chamber_clean,
                fis_len,
                osm_dims.get("osm_length"),
                bivas_len,
                survey_len,
                euris_neq_fis,
                n_violations,
                n_checks,
                is_single_kolk,
            )

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
                    "footprint_path": footprint_path,
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
                    # Independent geometric footprint measurement + integrity flag.
                    "obb_len": obb_len,
                    "obb_wid": obb_wid,
                    "geom_circular": geom_circular,
                    "euris_neq_fis": euris_neq_fis,
                    "level_dev_neg": m_row.get("level_dev_neg"),
                    "level_dev_pos": m_row.get("level_dev_pos"),
                    "sideview_path": sideview_path,
                    "decision_path": decision_path,
                    "bobi_measured": bobi_measured,
                    "bebu_measured": bebu_measured,
                    "violations_str": violations_str,
                    "n_violations": n_violations,
                    "n_checks": n_checks,
                    "is_single_kolk": is_single_kolk,
                    "sill_bobi_source": sill_bobi_source,
                    "sill_bebu_source": sill_bebu_source,
                    "sill_bobi_uncertain": sill_bobi_uncertain,
                    "sill_bebu_uncertain": sill_bebu_uncertain,
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

### 2. Afmetingen (terminologie per Ontwerp Schutsluizen / Richtlijn Vaarwegen)
* **Constructieve lengte**: Bouwkundige lengte van de kolk (betonconstructie) — FIS `Length`.
* **Schutlengte**: Bruikbare lengte voor schutting, na aftrek van deurruggen en veiligheidsmarges — FIS `SchutLengte`. Dit is de enige erkende "schut*"-term voor lengte.
* **Kolkbreedte**: Bouwkundige breedte van de kolk — FIS `Width`. In het rapport als "Kolkbreedte (m)".
* **Deuropening**: Vrije breedte van de deuropening (afstand tussen de sponningen) — FIS `GateWidth`. Dit is de **beperkende maat voor de scheepsbreedte**, kleiner dan of gelijk aan de kolkbreedte. De term "schutbreedte" bestaat niet in de Nederlandse richtlijnen.

### 3. BIVAS Modellering Beperking
* **BIVAS Netwerkgemiddelde**: In het BIVAS-netwerkmodel (gebruikt voor transportanalyses) worden parallelle kolken van één complex vaak gemodelleerd als één geaggregeerde verbinding met gemiddelde afmetingen. BIVAS maakt geen functioneel onderscheid tussen de individuele parallelle kolken, wat kan leiden tot afwijkingen ten opzichte van de specifieke kolkgegevens in FIS en EURIS.

## 1. Overzicht van Afmetingen

Dit deel toont de geselecteerde canonieke afmetingen, referentieniveaus en drempels per sluiskolk.

### Vergelijking Afmetingen per Sluiskolk (meters)
*Kolkbreedte = bouwkundige breedte (FIS `Width`). Deuropening = vrije breedte deuropening (FIS `GateWidth`) = beperkende maat voor scheepsbreedte. Polygoon lengte = afgeleid van FIS-geometrie via minimale omschreven rechthoek (niet onafhankelijk van FIS).*

| Sluis | Kolknaam | ISRS-code | Constr. Lengte (m) | Polygoon L (m) | Kolkbreedte (m) | Schutlengte (m) | Deuropening (m) | BIVAS Lengte | Enquête Lengte | DISK Lengte | Checks |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
"""
    for r in results_list:
        bivas_note = "" if r.get("is_single_kolk", True) else " (multi-kolk)"
        obb_l = r.get("obb_len")
        fis_l = r.get("fis_struct_len")
        if r.get("geom_circular"):
            obb_str = "⚠ circulaire geom."
        elif obb_l is not None:
            # Flag large geometry↔attribute deviation (>15% suggests transcription error)
            obb_flag = ""
            if fis_l is not None and fis_l > 0 and abs(obb_l - fis_l) / fis_l > 0.15:
                obb_flag = " ⚠"
            obb_str = f"{obb_l:.0f}{obb_flag}"
        else:
            obb_str = "nan"
        report_content += (
            f"| **{r['Sluis']}** | {r['name']} | `{r['isrs']}` | "
            f"{r['fis_struct_len'] or 'nan'} | {obb_str} | {r['fis_struct_wid'] or 'nan'} | "
            f"{r['fis_len'] or 'nan'} | {r['fis_wid'] or 'nan'} | "
            f"{r['bivas_len'] or 'nan'}{bivas_note} | {r['survey_len'] or 'nan'} | "
            f"{r.get('disk_len') or 'nan'} | {r.get('violations_str', 'n.v.t.')} |\n"
        )

    report_content += """
### Drempelhoogtes & Referentiewaterstanden per Sluiszijde
*Drempelkruin FIS = absolute drempelhoogte t.o.v. NAP afgeleid uit FIS. Bron: **Note** = FIS Note-veld (meest betrouwbaar), **KP-peil** / **SP-peil** = berekend uit streefpeil minus FIS drempeldiepte, **NAP (FIS)** = FIS HeightReferenceLevel=NAP. ⚠ = onzeker. Meting 1m = drempelkruin lokaal maximum uit RWS bodemhoogte_1mtr.*

| Sluis | Kolknaam | Zijde | Waterweg | Streefpeil (NAP) | Drempelkruin FIS (m NAP) | Meting 1m-kaart (m NAP) | Waterdiepte boven drempel (m) | Enquête |
| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
"""
    for r in results_list:
        peil_h = f"{r['peil_hoog']:.2f}" if pd.notna(r["peil_hoog"]) else "—"
        peil_l = f"{r['peil_laag']:.2f}" if pd.notna(r["peil_laag"]) else "—"

        def _sill_cell(nap_val, source, uncertain):
            if pd.isna(nap_val) or nap_val is None:
                return "—"
            flag = " ⚠" if uncertain else ""
            src = source or "?"
            return f"{nap_val:.2f} ({src}){flag}"

        def _water_depth(peil, sill):
            try:
                if peil is not None and sill is not None:
                    d = float(peil) - float(sill)
                    return f"{d:.2f}"
            except Exception:
                pass
            return "—"

        def _meting(v):
            return f"{v:.2f}" if v is not None else "—"

        bobi_cell = _sill_cell(
            r["threshold_height_bobi"], r["ref_bobi"], r.get("sill_bobi_uncertain")
        )
        bebu_cell = _sill_cell(
            r["threshold_height_bebu"], r["ref_bebu"], r.get("sill_bebu_uncertain")
        )

        report_content += (
            f"| **{r['Sluis']}** | {r['name']} | Bo/Bi | "
            f"{r['waterway_hoog']} | {peil_h} | {bobi_cell} | {_meting(r.get('bobi_measured'))} | "
            f"{_water_depth(r.get('peil_hoog'), r.get('threshold_height_bobi'))} | "
            f"{r['survey_drempel_bobi'] or '—'} |\n"
        )
        report_content += (
            f"| | | Be/Bu | "
            f"{r['waterway_laag']} | {peil_l} | {bebu_cell} | {_meting(r.get('bebu_measured'))} | "
            f"{_water_depth(r.get('peil_laag'), r.get('threshold_height_bebu'))} | "
            f"{r['survey_drempel_bebu'] or '—'} |\n"
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

    concept_diagram_path = generate_concept_diagram()

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
            <div class="section-title">Terminologie sluisdrempels</div>
            <div style="text-align:center; margin: 0.5rem 0 0.25rem;">
                <img src="{concept_diagram_path}" alt="Uitlegfiguur drempelterminologie"
                     style="max-width:100%; border:1px solid #e2e8f0; border-radius:0.375rem;">
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
                <input type="checkbox" id="toggle-consistent" onchange="toggleConsistentRows()" style="width: 1rem; height: 1rem; cursor: pointer;">
                Toon alleen sluiskolken met afwijkingen
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
            <p style="font-size:0.8rem;color:#64748b;">Drempelkruin FIS = absolute hoogte afgeleid uit FIS (Note, KP/SP-peil, of HeightReferenceLevel). Meting 1m = lokaal maximum uit RWS bodemhoogte_1mtr langs kolk-as. Waterdiepte = streefpeil − drempelkruin. ⚠ = onzeker.</p>
            <div class="table-responsive">
                <table>
                    <thead>
                        <tr>
                            <th rowspan="2">Sluis</th>
                            <th rowspan="2">Kolknaam</th>
                            <th colspan="5" style="text-align:center; border-bottom: 1px solid #e2e8f0;">Bovenhoofd/Binnenhoofd (Bo/Bi) — hoge zijde</th>
                            <th colspan="5" style="text-align:center; border-bottom: 1px solid #e2e8f0;">Benedenhoofd/Buitenhoofd (Be/Bu) — lage zijde</th>
                        </tr>
                        <tr>
                            <th>Waterweg</th>
                            <th>Streefpeil (m NAP)</th>
                            <th>Drempelkruin FIS (m NAP)</th>
                            <th>Meting 1m-kaart (m NAP)</th>
                            <th>Waterdiepte (m)</th>
                            <th>Waterweg</th>
                            <th>Streefpeil (m NAP)</th>
                            <th>Drempelkruin FIS (m NAP)</th>
                            <th>Meting 1m-kaart (m NAP)</th>
                            <th>Waterdiepte (m)</th>
                        </tr>
                    </thead>
                    <tbody>
"""
    for r in results_list:
        calc_bobi = (
            f"{r['threshold_height_bobi']:.2f}"
            if pd.notna(r["threshold_height_bobi"])
            else "—"
        )
        calc_bebu = (
            f"{r['threshold_height_bebu']:.2f}"
            if pd.notna(r["threshold_height_bebu"])
            else "—"
        )
        peil_h = f"{r['peil_hoog']:.2f}" if pd.notna(r["peil_hoog"]) else "—"
        peil_l = f"{r['peil_laag']:.2f}" if pd.notna(r["peil_laag"]) else "—"
        row_class = (
            "row-consistent" if r["status_str"] == "Consistent" else "row-discrepancy"
        )

        def _wd(peil_val, sill_val):
            try:
                if peil_val is not None and sill_val is not None and pd.notna(sill_val):
                    return f"{float(peil_val) - float(sill_val):.2f}"
            except Exception:
                pass
            return "—"

        meting_bobi = (
            f"{r['bobi_measured']:.2f}" if r.get("bobi_measured") is not None else "—"
        )
        meting_bebu = (
            f"{r['bebu_measured']:.2f}" if r.get("bebu_measured") is not None else "—"
        )
        wd_bobi = _wd(r.get("peil_hoog"), r.get("threshold_height_bobi"))
        wd_bebu = _wd(r.get("peil_laag"), r.get("threshold_height_bebu"))

        html_content += f"""
                        <tr class="{row_class}">
                            <td><strong>{r["Sluis"]}</strong></td>
                            <td>{r["name"]}</td>
                            <td><span style="font-size:0.75rem;">{r["waterway_hoog"]}</span></td>
                            <td>{peil_h}</td>
                            <td><strong>{calc_bobi}</strong><br><span style="font-size:0.7rem;color:#64748b;">{r.get("sill_bobi_source", "?")}</span>{"⚠" if r.get("sill_bobi_uncertain") else ""}</td>
                            <td>{meting_bobi}</td>
                            <td>{wd_bobi}</td>
                            <td><span style="font-size:0.75rem;">{r["waterway_laag"]}</span></td>
                            <td>{peil_l}</td>
                            <td><strong>{calc_bebu}</strong><br><span style="font-size:0.7rem;color:#64748b;">{r.get("sill_bebu_source", "?")}</span>{"⚠" if r.get("sill_bebu_uncertain") else ""}</td>
                            <td>{meting_bebu}</td>
                            <td>{wd_bebu}</td>
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

        _no_img = '<div style="background:#e2e8f0; height:200px; display:flex; align-items:center; justify-content:center; border-radius:0.375rem; border:1px solid #cbd5e1; color:#94a3b8;">Niet beschikbaar</div>'
        aerial_html = (
            f'<img src="{r["aerial_path"]}" alt="PDOK Luchtfoto">'
            if r.get("aerial_path")
            else _no_img
        )
        footprint_html = (
            f'<img src="{r["footprint_path"]}" alt="Voetafdruk met OBB">'
            if r.get("footprint_path")
            else aerial_html
        )
        (
            f'<img src="{r["chart_path"]}" alt="Afmetingen vergelijking">'
            if r.get("chart_path")
            else _no_img
        )
        sideview_html = (
            f'<img src="{r["sideview_path"]}" alt="Zijaanzicht waterstanden">'
            if r.get("sideview_path")
            else _no_img
        )
        decision_html = (
            f'<img src="{r["decision_path"]}" alt="Databronnen betrouwbaarheid">'
            if r.get("decision_path")
            else _no_img
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

        r["selection_method_len"]

        (
            "style Calc1 fill:#dcfce7,stroke:#15803d,stroke-width:2px"
            if pd.notna(r["streefpeil"])
            else ""
        )
        (
            "style Calc2 fill:#dcfce7,stroke:#15803d,stroke-width:2px"
            if pd.isna(r["streefpeil"])
            else ""
        )

        html_content += f"""
                <div class="lock-card {card_class}" id="{r["Sluis"]}_{r["name"]}">
                    <div class="lock-header">
                        <h3>{r["Sluis"]} - {r["name"]}</h3>
                        <div>
                            {card_badge}
                            <span class="isrs" style="margin-left: 0.5rem;">ISRS: {r["isrs"]}</span>
                            <span style="margin-left: 0.5rem; font-size: 0.75rem; color: #64748b;">{r.get("violations_str", "n.v.t.")}</span>
                        </div>
                    </div>
                    <div class="lock-body">
                        <div class="data-panel">
                            <div>
                                <h4>Vergelijkingsgegevens</h4>
                                <table class="lock-meta-table">
                                    <tr><td>Geselecteerde Schutlengte</td><td><strong>{r["selected_len"] or "nan"} m</strong> ({r["selection_method_len"]})</td></tr>
                                    <tr><td>Geselecteerde Deuropening</td><td><strong>{r["selected_wid"] or "nan"} m</strong> ({r["selection_method_wid"]})</td></tr>
                                    <tr><td>Fysieke Lengte (FIS)</td><td>{r["fis_struct_len"] or "nan"} m</td></tr>
                                    <tr><td>Kolkbreedte (FIS)</td><td>{r["fis_struct_wid"] or "nan"} m</td></tr>
                                    <tr><td>Bovenhoofd/Binnenhoofd (Bo/Bi) — {r["waterway_hoog"]}</td><td>Streefpeil: {peil_h} | Drempelkruin FIS: <strong>{calc_bobi} m NAP</strong> [{r.get("sill_bobi_source") or ""}] | Meting 1m-kaart: {f"{r['bobi_measured']:.2f} m" if r.get("bobi_measured") is not None else "—"} | Enquête: {r["survey_drempel_bobi"] or "—"}</td></tr>
                                    <tr><td>Benedenhoofd/Buitenhoofd (Be/Bu) — {r["waterway_laag"]}</td><td>Streefpeil: {peil_l} | Drempelkruin FIS: <strong>{calc_bebu} m NAP</strong> [{r.get("sill_bebu_source") or ""}] | Meting 1m-kaart: {f"{r['bebu_measured']:.2f} m" if r.get("bebu_measured") is not None else "—"} | Enquête: {r["survey_drempel_bebu"] or "—"}</td></tr>
                                </table>
                            </div>
                            {f'<div class="note-text"><strong>Opmerking:</strong> {r["note"]}</div>' if pd.notna(r["note"]) else ""}
                        </div>
                        <div class="visuals-panel">
                            <h4>Voetafdruk (FIS kolk)</h4>
                            {footprint_html}
                            <h5>Bron: PDOK Actueel_ortho25 + FIS geometrie</h5>
                        </div>
                        <div class="visuals-panel">
                            <h4>Zijaanzicht & Bodemprofiel</h4>
                            {sideview_html}
                            <h5>Bron: FIS streefpeil + drempelhoogte (NAP) + RWS bodemhoogte_1mtr</h5>
                        </div>
                        <div class="visuals-panel">
                            <h4>Databronnen lengte ({r.get("violations_str", "n.v.t.")})</h4>
                            {decision_html}
                            <h5>Bron: FIS / OSM / BIVAS / Enquête</h5>
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
    parser = argparse.ArgumentParser(
        description="Validate Dutch lock dimensions across FIS/EURIS/BIVAS/survey/OSM."
    )
    parser.add_argument(
        "--excel",
        default=LOCAL_EXCEL,
        help="Path to Chamber_comparison.xlsx (default: committed copy; "
        "env LOCK_VALIDATION_EXCEL also honored).",
    )
    parser.add_argument(
        "--euris-chambers",
        default=None,
        help="Path to a EURIS LockChamber_*.geojson (default: newest NL export).",
    )
    args = parser.parse_args()
    main(excel_path=args.excel, euris_path=args.euris_chambers)
