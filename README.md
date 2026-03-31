## Repository Overview

This repository crawls, processes, and exports inland waterway network data for transport modelling of Dutch and European inland waterways. It produces consistent, ready-to-use network graphs and lock schematizations for use in navigation and traffic simulation tools.

## Prerequisites & Installation

This project uses [`uv`](https://docs.astral.sh/uv/) for fast Python package and environment management.

1. **Install `uv`**: Follow the [official installation guide](https://docs.astral.sh/uv/getting-started/installation/) or run:
   ```bash
   # On macOS and Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # On Windows
   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

2. **Sync the environment**: From the repository root, install dependencies and set up the virtual environment:
   ```bash
   uv sync
   ```
   This ensures all required packages are installed. You can now use `uv run` to execute commands automatically within this environment.

## Use Cases

### Route Assignment (Traffic Modelling)
Locks are modelled as **delay elements** in a network graph: ships traverse the lock as a weighted edge, with the delay representing average processing time. Used in macroscopic traffic assignment studies where individual vessel queues are not simulated. Models: **BIVAS**, **OpenTNSim**.

### Detailed Lock Analysis (Discrete Event Simulation)
Locks are modelled in full detail: individual chambers, approach segments, waiting berths, and door positions. Ships queue, request a chamber, and transit step by step. Used for capacity studies and bottleneck analysis. Models: **SIVAK**, **OpenTNSim**.

## Network Coverage

| Network | Crawler | Description |
|---------|---------|-------------|
| FIS | `scrapy crawl dataservice` | Dutch inland waterways (Rijkswaterstaat) |
| DISK | `scrapy crawl disk` | Dutch lock & bridge details (Rijkswaterstaat) |
| EURIS | `scrapy crawl euris` | European inland waterways |
| Merged | `fis.cli graph merge` | Combined FIS + EURIS cross-border network |

Studies may use FIS only, EURIS only, or the merged network depending on scope.

## Pipeline Architecture

```
CRAWL → NETWORKS → ENRICH → SCHEMATIZE → MERGE
```

| Stage | Command | Output |
|-------|---------|--------|
| Crawl FIS | `scrapy crawl dataservice` | `output/fis-export/` |
| Crawl DISK | `scrapy crawl disk` | `output/disk-export/` |
| Crawl EURIS | `scrapy crawl euris` | `output/euris-export/` |
| Build Networks | `fis.cli graph {fis,euris}` | `output/{fis,euris}-graph/` |
| Enrich | `fis.cli graph enrich-{fis,euris}` | `output/{fis,euris}-enriched/` |
| Schematize Dropins | `fis.cli dropins schematize --source {fis,euris}` | `output/dropins-{fis,euris}-{detailed,simplified}/` |
| Merge | `fis.cli graph merge` | `output/merged-graph/` |

### Drop-in Schematization (Locks and Bridges)

The drop-in schematization step produces **drop-in replacement subgraphs** for the sections of the network that contain locks or bridges. It processes all structures simultaneously using a `FairwaySplicer` to guarantee that overlapping structures (e.g. a bridge physically sitting on top of a lock complex) are properly topologically connected.
The nodes and edges it generates replace the corresponding fairway stretch in the routing network, adding chamber-level routing and bridge passage constraints for discrete event simulation use cases.

## Configuration & Parameters

The behavior of the graph generation, structure splicing, and spatial matching is controlled by centralized parameters in `fis/settings.py`. These ensure consistent topology across the network.

### Coordinate Reference System
- **`PROJECTED_CRS`** (`EPSG:28992`): The coordinate system used for all metric calculations (meters). Currently hardcoded to the Dutch RD New system.

### Splicing & Topology
Determines how much of the original fairway is removed to accommodate a lock or bridge "drop-in" subgraph.
- **`DETAILED_LOCK_SPLICING_BUFFER_M`** (50m): Safety margin added to lock chamber halves in detailed mode.
- **`BRIDGE_SPLICING_BUFFER_M`** (10m): Standard distance used to cut fairways for bridge passages.
- **`SIMPLIFIED_LOCK_SPLICING_BUFFER_M`** (10m): Minimal cut used in simplified mode to prevent overlapping nearby structures.

### Spatial Matching
Used to associate distinct data sources (FIS records vs. DISK geometries) and connect structures to the network.
- **`BRIDGE_SECTION_MATCH_BUFFER_M`** (20m): Search radius for finding the parent fairway section of a bridge.
- **`DISK_MATCH_BUFFER_LOCK_M`** (50m): Radius for matching FIS locks to physical DISK geometries.
- **`EMBEDDED_STRUCTURE_MAX_DIST_M`** (500m): Max distance to consider a bridge part of a larger lock complex.

## Graph Schema & Harmonization

The network graph uses a harmonized schema based on the **EURIS** naming conventions. Attribute mapping and naming consistency are controlled via:
- **`config/schema.toml`**: Defines the source-to-canonical mapping for edge and node attributes (e.g., mapping FIS `speed_MaxSpeedUp` to `maxspeed_up`). This is part of an ongoing audit to standardize naming across all modules ([Issue #83](https://github.com/RWS-NL/fis-crawler/issues/83)).
- **Identifier Standardization**: All identifier columns (e.g., `id`, `section_id`, `node_id`) are automatically converted to **strings** to prevent float/integer ambiguity and ensure consistent null handling across data sources. See [NAMING_CONVENTIONS.md](NAMING_CONVENTIONS.md#special-case-identifiers-ids) for details.
- **Validation**: The `fis.cli graph validate` command checks generated graphs for schema compliance and attribute completeness.

## BIVAS vs. FIS Network Comparison

The repository includes tools and documentation for comparing the generated FIS network with the **BIVAS** (Binnenvaart Analyse Systeem) macroscopic assignment model.

### 1. Schematization Approach

| Feature | FIS Network (vaarweginformatie.nl) | BIVAS Network |
| :--- | :--- | :--- |
| **Geometry** | Detailed, geographically correct LineStrings. | Topological abstraction (straight lines between nodes). |
| **Nodes** | Exact spatial junctions (`sectionjunction`). | Model nodes with RD coordinates. |
| **Granularity** | High (captures physical curves and all regional waterways). | Macroscopic (focuses on primary transport corridors). |
| **Length** | Derived from geographic shape (`geometry.length`). | Logical attribute (`Length__m`). |

### 2. Terminology Mapping

| FIS (Source) | Canonical (Project) | BIVAS Term | Note |
| :--- | :--- | :--- | :--- |
| `Section` | `fairway_segment` | `arcs` / `segment` | BIVAS uses 'arcs' for topological links. |
| `SectionJunction` | `node` | `nodes` | Connects the segments/arcs. |
| `StartJunctionId` | `source_node` | `FromNodeID` | |
| `EndJunctionId` | `target_node` | `ToNodeID` | |
| `VinCode` | `vincode` | `Code` (in segment) | Linked via trajectory mappings. |

### 3. Attribute Equivalents

The project maps FIS attributes to canonical names that align with BIVAS model requirements:

| BIVAS Attribute (`arcs`) | Canonical Property | FIS Source Column |
| :--- | :--- | :--- |
| `MaximumWidth__m` | `dim_width` | `GeneralWidth` |
| `MaximumDepth__m` | `dim_depth` | `GeneralDepth` |
| `MaximumLength__m` | `dim_length` | `GeneralLength` |
| `MaximumHeightClosed__m` | `dim_height` | `GeneralHeight` |
| `MaximumSpeedEmpty__km_h` | `maxspeed` | `Speed` |
| `CemtClassId` | `cemt_class` | `nav_Code` |

### 4. Comparison Logic
A dedicated script (`scripts/bivas/compare_networks.py`) provides spatial matching between the two networks using a 50m buffer. This allows for:
- Validation of FIS network coverage against the BIVAS baseline.
- Identification of secondary waterways present in FIS but absent in BIVAS.
- Statistical reporting on network length and segment counts.

**Prerequisites:** The BIVAS SQLite database is not stored in this repository (it is gitignored), so it will not be available in a fresh checkout. Obtain a BIVAS network export (SQLite) from your usual data source (e.g. internal data distribution or official BIVAS delivery) and store it locally. When running `scripts/bivas/compare_networks.py`, pass the path to this file via `--bivas-db /path/to/bivas.sqlite` to override the default database location.

### Standard Edge Attributes
Every edge in the final network graph strictly follows these standardized naming conventions:

| Attribute | Description |
|-----------|-------------|
| `id` | Unique identifier for the edge. |
| `feature_type` | Always `fairway_segment`. |
| `segment_type` | Category of segment (e.g., `clear`, `bridge_passage`, `chamber_route`). |
| `name` | Human-readable name of the fairway or structure. |
| `source_node` / `target_node` | IDs of the start and end nodes. |
| `section_id` | ID of the parent FIS fairway section. |
| `fairway_id` | ID of the parent FIS fairway route. |
| `length_m` | Geometric length of the segment in meters. |
| `structure_type` | Type of structure if applicable (`lock`, `bridge`). |
| `structure_id` | ID of the parent lock or bridge complex. |
| `dim_width` | Minimum navigable width (meters). |
| `dim_height` | Minimum navigable height (meters, for bridges). |
| `dim_length` | Maximum navigable length (meters, for locks). |

## Output File Formats

All spatial outputs are produced in two formats:

| Format | Extension | Use |
|--------|-----------|-----|
| GeoJSON | `.geojson` | Interoperability, GIS tools, human-readable |
| GeoParquet | `.geoparquet` | Efficient storage and loading in Python (GeoPandas) |

Graph outputs follow the `nodes.geoparquet` / `edges.geoparquet` convention used across all pipeline stages.

## CLI Usage

The main entry point is `fis.cli`:

```bash
# Show all commands
uv run python -m fis.cli --help

# Graph pipeline commands
uv run python -m fis.cli graph --help
uv run python -m fis.cli graph all          # Run full pipeline

# Integrated Lock & Bridge schematization
uv run python -m fis.cli dropins --help
uv run python -m fis.cli dropins schematize    # Process lock and bridge complexes
```

## Data Crawling

```bash
# FIS (Dutch fairways) → output/fis-export/
uv run scrapy crawl dataservice -L INFO

# DISK (Dutch Lock Details) → output/disk-export/
uv run scrapy crawl disk -L INFO

# EURIS (European fairways) → output/euris-export/
uv run scrapy crawl euris -L INFO
```


## Module Documentation

- [Graph Pipeline](fis/graph/README.md) - Network graph building and enrichment
- [Lock Schematization](fis/lock/README.md) - Detailed lock feature generation
- [Bridge Schematization](fis/bridge/README.md) - Bridge feature generation
- [Integrated Drop-ins](fis/dropins/README.md) - Global integration of all structures

## Notebooks

Analysis notebooks in `notebooks/`:
- Network analysis (`network.ipynb`)
- EURIS data processing (`euris/*.ipynb`)
- Data visualization and exploration
## Project Structure

```
fis/
├── bridge/         # Bridge schematization and graph features
├── dropins/        # Integrated drop-ins (locks, bridges, terminals, berths)
│   ├── core.py     # Coordination logic
│   ├── graph.py    # Shared graph feature generation
│   ├── embedded.py # Embedded structure matching
│   ├── splicing.py # Fairway segment splicing
│   ├── terminals.py # Terminal-specific features
│   ├── berths.py    # Berth-specific features
│   └── io.py       # Data loading and export
├── graph/          # Core network graph building (FIS, EURIS, Merge)
├── lock/           # Detailed lock schematization
├── spiders/        # Scrapy crawlers (FIS, EURIS, DISK)
├── splicer/        # General fairway splicing utilities
├── cli.py          # Main CLI entry point
├── ris_index.py    # RIS Index mapping utilities
├── utils.py        # Shared helper functions
└── settings.py     # Scrapy configuration
```


## Testing

Integration tests verify the topological correctness of the network generation logic.

### Running Tests
```bash
uv run pytest tests/
```

### Integration Test Data
Some tests (e.g., `tests/test_topological_scenarios.py`) require a schematized network graph. In CI, this is generated automatically from a minimal data subset stored in `tests/data/`.

To update or expand this test data subset from your local full export:
1. Ensure you have full data in `output/fis-export/` and `output/disk-export/`.
2. Run the subsetting script:
   ```bash
   uv run python scripts/subset_test_data.py
   ```
   This script filters the global dataset to a specific bounding box (Volkerak/Krammer area) to keep the repository lightweight while maintaining test coverage.
