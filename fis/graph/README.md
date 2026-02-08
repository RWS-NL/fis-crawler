# FIS Graph Pipeline

Build network graphs from FIS (Dutch) and EURIS (European) fairway data.

## Data Flow

```
┌─────────────┐     ┌─────────────┐
│ FIS Crawler │     │EURIS Crawler│
│ (fis-export)│     │(euris-export)│
└──────┬──────┘     └──────┬──────┘
       │                   │
       ▼                   ▼
┌─────────────┐     ┌─────────────┐
│  fis-graph  │     │ euris-graph │
│ (basic)     │     │ (basic)     │
└──────┬──────┘     └──────┬──────┘
       │                   │
       ▼                   ▼
┌─────────────┐     ┌──────────────┐
│ fis-enriched│     │euris-enriched│
│ (+attrs)    │     │ (+attrs)     │
└──────┬──────┘     └──────┬───────┘
       │                   │
       └───────┬───────────┘
               ▼
        ┌─────────────┐
        │merged-graph │
        │(FIS+EURIS)  │
        └─────────────┘
```

## Data Sources

Download data using the scrapy crawlers from the project root:

```bash
# FIS (Dutch fairways) → fis-export/
scrapy crawl dataservice -L INFO

# EURIS (European fairways) → euris-export/
scrapy crawl euris -L INFO
```

## Directory Structure

```
fis-export/              # Raw FIS data (scrapy: dataservice)
├── section.geoparquet
├── sectionjunction.geoparquet
├── commonbordernode.geoparquet
└── ...

euris-export/            # Raw EURIS data (scrapy: euris)
├── SailingSpeed_*.geojson
├── FairwaySection_*.geojson
└── v0.1.0/
    └── export-graph-v0.1.0.pickle

output/                  # Pipeline outputs
├── fis-graph/           # Basic FIS graph
├── euris-graph/         # Basic EURIS graph
├── fis-enriched/        # Enriched FIS
├── euris-enriched/      # Enriched EURIS (+SailingSpeed)
└── merged-graph/        # Combined FIS+EURIS
```

## CLI Usage

```bash
# Individual steps
uv run python -m fis.graph.cli fis          # Build FIS graph
uv run python -m fis.graph.cli euris        # Build EURIS graph
uv run python -m fis.graph.cli enrich-fis   # Enrich FIS (placeholder)
uv run python -m fis.graph.cli enrich-euris # Enrich EURIS with SailingSpeed
uv run python -m fis.graph.cli merge        # Merge via border nodes

# Full pipeline
uv run python -m fis.graph.cli all
```

## Edge Attributes

### Speed Data (EURIS)

EURIS edges have **two sources of speed data**:

**1. FairwaySection speed (original)**
From the original FairwaySection data, included in basic EURIS graph:
- `calspeed_up`, `calspeed_down` - Calculated speed (km/h)
- `maxspeed_up`, `maxspeed_down` - Maximum speed (km/h)
- `speed` - Human-readable description

**2. SailingSpeed (enriched)**
Additional data from `SailingSpeed_*.geojson`, added by `enrich-euris`:
- `speed_maxspeed` - Maximum speed (km/h)
- `speed_calspeed` - Calculated speed (km/h)
- `speed_direction` - Direction code
- `speed_shipcategory` - Ship category

> **Note**: SailingSpeed data is only available for Austria (AT) and Belgium (BE).
> Matched via `sectionref` field - enriches ~350 edges.

## Graph Statistics

| Stage | Nodes | Edges | Components |
|-------|-------|-------|------------|
| fis-graph | 4,157 | 4,721 | 1 |
| euris-graph | 6,414 | 6,883 | 14 |
| merged-graph | 10,571 | 11,612 | 15 |

Border connections: 13 (via ISRS location codes)
