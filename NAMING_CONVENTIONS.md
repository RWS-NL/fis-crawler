# Naming Conventions and Schema Strategy

## 1. The "Source of Truth"
The project uses **EURIS-style (snake_case)** as the canonical internal terminology for all graph attributes and processed data structures.

- **Base Terminology:** EURIS (lowercase, snake_case).
- **Source Terminology:** FIS (CamelCase).
- **Mapping Direction:** Source (FIS) → Canonical (EURIS).

### Why EURIS?
EURIS provides a standardized, lowercased schema that is more idiomatic for Python internal processing and matches the target format for many of our downstream integrations.

## 2. Canonical Attribute Mapping
All mappings are defined in `config/schema.toml`. The format is:
`"FIS_Column_Name" = "euris_canonical_name"`

### Key Entities
| Entity | FIS Name (Source) | Canonical Name (Truth) |
| :--- | :--- | :--- |
| **Identification** | `Id` | `id` |
| | `ParentId` | `parent_id` |
| **Structure** | `Name` | `name` |
| | `FairwaySectionId` | `section_id` |
| | `FairwayId` | `fairway_id` |
| **Dimensions** | `Length` | `dim_length` |
| | `Width` | `dim_width` |
| | `GeneralHeight` | `dim_height` |
| **Geography** | `RouteKmBegin` | `route_km_begin` |
| | `RouteKmEnd` | `route_km_end` |
| | `Geometry` | `geometry` |

### Special Case: Geometry
In accordance with GIS standards, the project standardizes on a lowercase **`geometry`** column for all spatial data.
- **Ingestion:** Raw FIS sources use `Geometry` (uppercase). This is converted to a lowercase `geometry` column containing Shapely objects immediately upon loading in `read_geo_or_parquet`.
- **Processing:** All processing functions (grouping, routing, enrichment) expect a `GeoDataFrame` with an active geometry column named `geometry`.
- **Export:** Final GeoParquet/GeoJSON exports use `geometry`.

## 3. Implementation Strategy: "Normalize Early"
To maintain consistency, we normalize data immediately after loading from raw sources (Parquet/GeoParquet).

1. **Loading:** Use `load_data` in `fis/lock/core.py`.
2. **Normalization:** Call `utils.normalize_attributes(df, section_name)` using the mappings in `schema.toml`.
3. **Internal Logic:** Once normalized, ALL subsequent code MUST use the canonical names (e.g., `row["id"]` instead of `row["Id"]`).

## 4. Current Inconsistencies & Solutions

### Inconsistency: Mixed Access
Some functions were still using `lock["Id"]` while others used `lock["id"]`.
- **Solution:** Completed a bulk migration to canonical names in `fis/lock/core.py` and `fis/bridge/core.py`.

### Inconsistency: Duplicate Logic
Functions like `process_fairway_geometry` were duplicated across modules with different parameter names and attribute access styles.
- **Solution:** Centralized shared logic in `fis/utils.py` and strictly enforced canonical attribute access.

### Inconsistency: Enrichment Prefixes
Enrichment modules added prefixes like `speed_Speed` which created "Double CamelCase".
- **Solution:** `apply_schema_mapping` in `fis/graph/schema.py` renames these to flat canonical names (e.g., `speed_Speed` → `maxspeed`) before final graph export.

## 5. Lock-Specific Refinements
The splitting of fairways for lock complexes involves specialized logic to ensure topological integrity when bridge openings are present:
- **Dynamic Buffering:** If bridge openings are parented to a lock or its chambers, the `buffer_dist` is automatically expanded to encompass the farthest opening plus a 100m safety margin.
- **Metric Projection:** All calculations for splitting and buffering use the **RD New (EPSG:28992)** projected coordinate system to ensure accurate distances in meters.
- **Centralization:** This logic is implemented in `utils.process_fairway_geometry` and shared across lock and bridge modules.

## 6. Maintenance
When adding new FIS data sources:
1. Add the FIS column names to the appropriate section in `config/schema.toml`.
2. Define their canonical snake_case equivalents.
3. Ensure the ingestion logic calls `normalize_attributes`.
