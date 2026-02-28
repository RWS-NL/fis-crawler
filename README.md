## Repository Overview

This repository crawls, processes, and exports inland waterway network data for transport modelling of Dutch and European inland waterways. It produces consistent, ready-to-use network graphs and lock schematizations for use in navigation and traffic simulation tools.

## Use Cases

### Route Assignment (Traffic Modelling)
Locks are modelled as **delay elements** in a network graph: ships traverse the lock as a weighted edge, with the delay representing average processing time. Used in macroscopic traffic assignment studies where individual vessel queues are not simulated. Models: **BIVAS**, **OpenTNSim**.

### Detailed Lock Analysis (Discrete Event Simulation)
Locks are modelled in full detail: individual chambers, approach segments, waiting berths, and door positions. Ships queue, request a chamber, and transit step by step. Used for capacity studies and bottleneck analysis. Models: **SIVAK**, **OpenTNSim**.

## Network Coverage

| Network | Crawler | Description |
|---------|---------|-------------|
| FIS | `scrapy crawl dataservice` | Dutch inland waterways (Rijkswaterstaat) |
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
| Crawl EURIS | `scrapy crawl euris` | `output/euris-export/` |
| Build Networks | `fis.cli graph {fis,euris}` | `output/{fis,euris}-graph/` |
| Enrich | `fis.cli graph enrich-{fis,euris}` | `output/{fis,euris}-enriched/` |
| Schematize Locks | `fis.cli lock schematize` | `output/lock-schematization/` |
| Merge | `fis.cli graph merge` | `output/merged-graph/` |

### Lock Schematization

The lock schematization step produces **drop-in replacement subgraphs** for the sections of the network that contain locks. The nodes and edges it generates replace the corresponding fairway stretch in the routing network, adding chamber-level detail for discrete event simulation use cases.

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

# Lock schematization
uv run python -m fis.cli lock --help
uv run python -m fis.cli lock schematize    # Process lock complexes
```

## Data Crawling

```bash
# FIS (Dutch fairways) → output/fis-export/
uv run scrapy crawl dataservice -L INFO

# EURIS (European fairways) → output/euris-export/
uv run scrapy crawl euris -L INFO
```

## Pipeline Architecture

```
CRAWL → NETWORKS → ENRICH → SCHEMATIZE → MERGE
```

| Stage | Command | Output |
|-------|---------|--------|
| Crawl FIS | `scrapy crawl dataservice` | `output/fis-export/` |
| Crawl EURIS | `scrapy crawl euris` | `output/euris-export/` |
| Build Networks | `fis.cli graph {fis,euris}` | `output/{fis,euris}-graph/` |
| Enrich | `fis.cli graph enrich-{fis,euris}` | `output/{fis,euris}-enriched/` |
| Schematize Locks | `fis.cli lock schematize` | `output/lock-schematization/` |
| Merge | `fis.cli graph merge` | `output/merged-graph/` |

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
