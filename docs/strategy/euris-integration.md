# EURIS Integration Strategy for Issue #18

## Data Analysis Results
The analysis of EURIS structure data (locks, bridges, terminals, berths) reveals significant differences from FIS DISK data:

1. **Geometry Types:**
   - **Locks:** While `LockComplex` and `LockChamber` are `Point` geometries, many countries provide detailed `LockChamberArea` polygons (e.g., AT, BE, DE, FR, NL). However, several countries (RS, HR, BG) only provide points.
   - **Bridges:** Both `BridgeArea` and `BridgeOpening` are almost exclusively `Point` geometries across all countries in EURIS.
   
2. **Schema Mapping (EURIS to Canonical):**
   - **Dimensions:** EURIS uses `mlengthcm`, `mwidthcm`, `mheightcm`. These must be divided by 100 to map to canonical `dim_length`, `dim_width`, `dim_height`.
   - **Links:** 
     - `LockComplex.locode` links to `LockChamber.slslocode`.
     - `BridgeArea.locode` links to `BridgeOpening.brgalocode`.
     - Both `LockComplex` and `BridgeArea` have a `sectionref` linking to `FairwaySection.code`.

## Proposed Architecture

### 1. Unified Loader for EURIS structures
We propose a new loader in `fis/dropins/euris_io.py` that handles the extraction and grouping of EURIS structures into a format compatible with the existing `dropins` pipeline. It will:
- Load all `LockComplex_*.geojson`, `LockChamber_*.geojson`, and `LockChamberArea_*.geojson`.
- Group them into `lock_complexes` with associated `chambers`.
- Match polygons from `LockChamberArea` to their respective `LockChamber` points via `locode`.
- **Topological Linking:** Use the `sectionref` provided in `LockComplex` and `BridgeArea` to explicitly link structures to `FairwaySection` codes. If missing, use spatial snapping of the `Point` geometry (as feedback suggests points are often already snapped to the fairway).

### 2. Splicing Fallback Mechanism
Since many EURIS structures are only points, and some polygons may not directly intersect the fairway centerline (e.g., parallel chambers):
- **Case Polygon:** 
    - First, try to intersect the polygon with the linked `FairwaySection`. 
    - If it doesn't intersect (e.g., parallel chambers), use the `Point` location (which is snapped to the fairway) as the "cut point" to perform the splicing.
- **Case Point:** If only a point is available, use the `generate_simplified_passages` logic to split the `FairwaySection` at the point and insert a single logical "passage" edge with the structure's constraints.

### 3. Snapping for Non-Snapped Points
For countries where points are "close" but not exactly on the line (e.g., RS), implement a configurable snapping distance (e.g., 50m) to ensure they connect to the nearest `FairwaySection`.

### 3. CLI Extension
Extend the `fis.cli dropins` command to support an `--source euris` flag, which will trigger the EURIS-specific loaders and coordinate the integrated drop-ins build.

## Validation and Inspection
We have generated validation datasets in `output/euris-analysis/`:
- `validation_structures_point.geoparquet`: Points for all structures.
- `validation_structures_polygon.geoparquet`: Polygons where available (mainly locks).

### Guidance for Visual Inspection in QGIS:
1. **Topological Alignment:** Check if `LockChamberArea` polygons from Germany and France properly overlap the `FairwaySection` geometries in the EURIS graph.
2. **Point Snapping:** Inspect if `BridgeArea` points for smaller countries (e.g., RS, BG) are situated exactly on the fairway line or require a search buffer.
3. **Completeness:** Verify if the number of `LockChamber` points matches the number of chambers in the resulting graph for a sample area (e.g., the Danube).
