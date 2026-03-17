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
| Schematize Dropins | `fis.cli dropins schematize --disk-dir output/disk-export` | `output/dropins-schematization/` |
| Merge | `fis.cli graph merge` | `output/merged-graph/` |

### Drop-in Schematization (Locks and Bridges)

The drop-in schematization step produces **drop-in replacement subgraphs** for the sections of the network that contain locks or bridges. It processes all obstacles simultaneously using a `FairwaySplicer` to guarantee that overlapping structures (e.g. a bridge physically sitting on top of a lock complex) are properly topologically connected.
The nodes and edges it generates replace the corresponding fairway stretch in the routing network, adding chamber-level routing and bridge passage constraints for discrete event simulation use cases.

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

## Notebooks

Analysis notebooks in `notebooks/`:
- Network analysis (`network.ipynb`)
- EURIS data processing (`euris/*.ipynb`)
- Data visualization and exploration

## Project Structure

```
fis/
├── cli.py          # Main CLI entry point
├── graph/          # Graph pipeline module
├── lock/           # Lock schematization module
├── spiders/        # Scrapy crawlers
├── pipelines.py    # Data export pipelines
└── settings.py     # Scrapy settings
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
