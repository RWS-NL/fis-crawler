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

### Special Case: Identifiers (IDs)
To support the mix of numeric FIS IDs, country-prefixed EURIS IDs, and complex spliced IDs (e.g., `123_split`), the project standardizes on **strings** for all identifier columns.

#### Why we do this
- **Float Ambiguity:** Pandas often reads numeric columns as floats if they contain nulls. This leads to IDs like `123.0`, which cause lookup failures when compared against the integer `123`.
- **Consistency:** Standardizing on strings ensures that a section ID from a CSV, a lock ID from a Parquet file, and a spliced node ID like `lock_123_split` can all be handled by the same logic.
- **Nullable IDs:** Numeric columns in Pandas are traditionally non-nullable unless using specific extension types. String columns handle `None` and `NaN` consistently.

#### When and Where it happens
1. **Early Normalization:** `utils.normalize_attributes` automatically converts all columns listed in the `[identifiers]` section of `config/schema.toml` to strings immediately after loading.
2. **Graph Construction:** When building internal graph features (in `fis/lock/graph.py` and `fis/bridge/graph.py`), all generated node and edge IDs are passed through `utils.stringify_id`.
3. **Data Export:** The `_export_dataframes` function in `fis/dropins/core.py` ensures all ID columns are stringified before writing to Parquet/GeoJSON to prevent schema mismatches in downstream tools.
4. **Synthetic IDs:**
   - **Why:** Some FIS structures lack explicit sub-component records (e.g., a bridge with no openings). To create a valid traversal path in the final graph, we must generate a placeholder element.
   - **How:** We use a `virtual_` prefix followed by the parent structure's ID (e.g., `virtual_123`). This replaces the older convention of using negative integers, making synthetic elements explicitly identifiable and searchable.
   - **Where:** This logic is applied during internal graph construction in `fis/bridge/graph.py` and `fis/lock/graph.py` whenever source data is incomplete.

#### How it is implemented
We use `utils.stringify_id(val)` which:
- Returns `None` for any `NaN` or null value (avoiding the `"nan"` string).
- Strips trailing `.0` from float-like IDs (e.g., `123.0` -> `"123"`).
- Preserves existing string identifiers (like ISRS codes) as-is.

#### Usage Guidelines
- **Strictness:** Internal logic should treat IDs as strings. Avoid wrapping IDs in `int()` calls.
- **Comparison:** Always use `utils.stringify_id` when comparing an external ID against an internal one to ensure a "clean" match.
- **Graph Building:** When adding nodes or edges to a NetworkX graph, ensure the identifiers are strings.


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

