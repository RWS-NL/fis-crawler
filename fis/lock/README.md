# Lock Schematization

This module processes FIS lock data into detailed graph features for navigation networks.

## Usage

```bash
uv run python -m fis.cli lock schematize
```

### Options
- `--export-dir`: Input directory (default: `output/fis-export`)
- `--output-dir`: Output directory (default: `output/lock-schematization`)

## Output

| File | Description |
|------|-------------|
| `lock_schematization.json` | Full hierarchical lock complex data |
| `lock_schematization.geojson` | Flattened GeoJSON features |
| `lock_schematization.geoparquet` | Flattened GeoParquet features |

## Feature Types

| Type | `feature_type` | Description |
|------|----------------|-------------|
| Lock | `lock` | Lock complex polygon |
| Chamber | `chamber` | Individual chamber polygon |
| Berth | `berth` | Waiting area |
| Segment | `fairway_segment` | Sailing path LineString |
| Node | `node` | Topological point |

### Segment Types (`segment_type`)
- `before` / `after`: Main fairway segments
- `chamber_approach`: Split → Chamber Start
- `chamber_route`: Chamber Start → Chamber End
- `chamber_exit`: Chamber End → Merge

### Node Types (`node_type`)
- `lock_split` / `lock_merge`: Divergence/convergence points
- `chamber_start` / `chamber_end`: Chamber entrance/exit

## Module Structure

```
fis/lock/
├── __init__.py
├── cli.py      # CLI commands
├── core.py     # Data loading and grouping
├── graph.py    # Feature generation
└── utils.py    # Geometry utilities
```
