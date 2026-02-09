## Repository Overview

This repository is a data processing and scraping project focused on inland waterway information.

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
