# Lock Schematization

This module processes FIS lock data into detailed subgraph features for navigation network modelling.
Lock schematization outputs are **drop-in replacement subgraphs**: the generated nodes and edges replace
the corresponding fairway stretch in the routing network (FIS, EURIS, or merged) wherever a lock is present.

## Use Cases

### Route Assignment (Traffic Modelling)
Only the coarse structure is needed: a `before` edge, one edge per chamber, and an `after` edge,
together with their connecting nodes. The edge weight (delay) represents average lock processing time.
Suitable for models such as **BIVAS** and **OpenTNSim**.

### Discrete Event Simulation (Detailed Lock Analysis)
The full detail is needed: split/merge nodes, individual chamber approach/route/exit edges,
waiting berths, and chamber dimensions. Ships queue and transit step by step through the model.
Suitable for models such as **SIVAK** and **OpenTNSim**.

## Usage

```bash
uv run python -m fis.cli lock schematize
```

### Options
| Option | Default | Description |
|--------|---------|-------------|
| `--export-dir` | `output/fis-export` | Input directory with parquet/geoparquet files |
| `--fis-graph` | `output/fis-graph/graph.pickle` | FIS network graph for topology matching |
| `--output-dir` | `output/lock-schematization` | Output directory |

## Output Files

All spatial outputs are produced in two formats: `.geojson` (GIS / interoperability) and `.geoparquet`
(efficient Python loading). File names follow the same convention as `fis-enriched`:

| File | Description |
|------|-------------|
| `nodes.geojson` / `.geoparquet` | Routing nodes: junctions, split/merge points, chamber doors |
| `edges.geojson` / `.geoparquet` | Routing edges: fairway segments and chamber routes |
| `berths.geojson` / `.geoparquet` | Waiting berths associated with each lock |
| `summary.json` | Per-lock metadata: name, ISRS code, fairway, chamber count, berth count |

> **Note:** The current implementation outputs a single flat `lock_schematization.geoparquet` /
> `.geojson` / `.json`. The split-file structure above is the intended target (tracked in issue #17).

## Node Types (`node_type`)

| Type | Description |
|------|-------------|
| `junction` | Existing FIS network junction (start/end of the lock's fairway) |
| `lock_split` | Divergence point where chamber routes branch off |
| `lock_merge` | Convergence point where chamber routes rejoin |
| `chamber_start` | Chamber entrance (door position) |
| `chamber_end` | Chamber exit (door position) |

## Edge Types (`segment_type`)

| Type | Description |
|------|-------------|
| `before` | Fairway approaching the lock (junction → split) |
| `after` | Fairway leaving the lock (merge → junction) |
| `chamber_approach` | Approach lane from split node to chamber entrance |
| `chamber_route` | Transit through the chamber (start → end) |
| `chamber_exit` | Exit lane from chamber exit to merge node |

## Module Structure

```
fis/lock/
├── __init__.py
├── cli.py      # CLI commands
├── core.py     # Data loading, lock grouping, berth/section association
├── graph.py    # Node/edge feature generation
└── utils.py    # Geometry utilities (door finding, etc.)
```
