# Lock Schematization

This module processes FIS lock data into detailed subgraph features for navigation network modelling.
Lock schematization outputs are **drop-in replacement subgraphs**: the generated nodes and edges replace
the corresponding fairway stretch in the routing network (FIS, EURIS, or merged) wherever a lock is present.

> **Note:** Ensure you have installed prerequisites (`uv`) and run `uv sync` from the repository root before running these commands. See the [main README](../../README.md) for details.

## Use Cases

### Route Assignment (Traffic Modelling)
Only the simplified structure is needed: a `before` edge, one edge per chamber, and an `after` edge,
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
| `--disk-dir` | `output/disk-export` | Input directory with DISK lock and bridge parquet files |
| `--fis-graph` | `output/fis-graph/graph.pickle` | FIS network graph for topology matching |
| `--output-dir` | `output/lock-schematization` | Output directory |

## Output Files

All spatial outputs are produced in two formats: `.geojson` (GIS / interoperability) and `.geoparquet`
(efficient Python loading). File names follow the same convention as `fis-enriched`:

| File | Description |
|------|-------------|
| `lock.geojson` / `.geoparquet` | Lock complex polygon geometries with all FIS metadata |
| `chamber.geojson` / `.geoparquet` | Individual lock chamber polygons with metadata |
| `subchamber.geojson` / `.geoparquet` | Sub-chamber polygons (partitions within a chamber) |
| `nodes.geojson` / `.geoparquet` | Routing nodes: junctions, split/merge points, chamber doors |
| `edges.geojson` / `.geoparquet` | Routing edges: fairway segments and chamber routes |
| `berths.geojson` / `.geoparquet` | Waiting berths associated with each lock |
| `summary.json` | Detailed JSON export of the lock hierarchy (complex → lock → chamber → subchamber) |


## Node Types (`node_type`)

| Type | Description |
|------|-------------|
| `junction` | Existing FIS network junction (start/end of the lock's fairway) |
| `lock_split` | Divergence point where chamber routes branch off upstream of the lock complex |
| `lock_merge` | Convergence point where chamber routes rejoin downstream of the lock complex |
| `chamber_start` | Chamber entrance (door position on the upstream side) |
| `chamber_end` | Chamber exit (door position on the downstream side) |
| `chamber_internal_junction` | FIS network junction that lies *inside* the chamber polygon; inserted as an intermediate node on the `chamber_route` edge (e.g. NL_J2501 / 8864190 inside Weurt chamber 47538) |

### Lock-level vs. complex-level nodes

For single-lock complexes the `lock_{id}_split` / `lock_{id}_merge` nodes serve as the outermost boundary of the complex.  For multi-branch complexes (e.g. Oranjesluizen) where the fairway genuinely forks before the individual chambers, pre-existing FIS junction nodes (identified by `detect_complex_groups`) serve as the complex-level boundary nodes and are recorded as `junction` type.

## Edge Types (`segment_type`)

| Type | Description |
|------|-------------|
| `before` | Fairway approaching the lock (junction → split) |
| `after` | Fairway leaving the lock (merge → junction) |
| `chamber_approach` | Approach lane from split node to chamber entrance |
| `chamber_route` | Transit through the chamber (start → [internal junctions] → end) |
| `chamber_exit` | Exit lane from chamber exit to merge node |

## Asymmetric Buffering

Split and merge node positions are computed from the **actual spatial extents of the chamber polygons** rather than a symmetric offset from the lock centroid:

1. Each chamber polygon's bounding-box corners are projected onto the fairway line.
2. `buffer_before_m` = distance from lock centroid to the earliest (upstream) chamber projection + `DETAILED_LOCK_SPLICING_BUFFER_M`.
3. `buffer_after_m`  = distance from lock centroid to the latest (downstream) chamber projection + `DETAILED_LOCK_SPLICING_BUFFER_M`.

This corrects the Weurt (49032) split placement where chambers 40927 and 47538 are staggered along the fairway.

## Module Structure

```
fis/lock/
├── __init__.py
├── cli.py      # CLI commands
├── core.py     # Data loading, lock grouping, berth/section association
│                 detect_complex_groups: groups locks sharing boundary junctions
├── graph.py    # Node/edge feature generation
└── utils.py    # Geometry utilities (door finding, etc.)
```
