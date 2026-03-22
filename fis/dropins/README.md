# Integrated Drop-ins

This module handles the global integration of locks, bridges, terminals, and berths into the fairway network. It uses a `FairwaySplicer` to modify the base fairway segments and inject detailed (or simplified) subgraphs.

## Features

- **Global Splicing**: Handles overlapping structures and ensures topological consistency.
- **Embedded Structures**: Automatically identifies bridges that are part of a lock complex.
- **Variable Detail**: Supports `detailed` (chamber-level locks and bridge openings) and `simplified` (single-edge) schematization.
- **Optional Integration**: Berths and terminals can be optionally included.

## Usage

```bash
uv run python -m fis.cli dropins schematize
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--mode` | `detailed` | `detailed` or `simplified` |
| `--include-berths` | `False` | Whether to include berths in the graph |
| `--bbox` | `None` | Filter by bounding box `minx,miny,maxx,maxy` |

## Output Files

Located in `output/dropins-schematization/` (or user-defined):
- `nodes.geoparquet` / `.geojson`: All network nodes (junctions, split/merge, doors).
- `edges.geoparquet` / `.geojson`: All network edges (fairway segments, passages, access).
- `terminals.geoparquet` / `.geojson`: Integrated terminals.
- `berths.geoparquet` / `.geojson`: Integrated berths.

## Module Structure

```
fis/dropins/
├── core.py      # Main orchestrator
├── graph.py     # Shared graph feature generation logic
├── embedded.py  # Embedded structure detection
├── splicing.py  # Fairway splicing and geometry logic
├── terminals.py # Terminal-specific features
├── berths.py    # Berth-specific features
└── io.py        # Data loading and export
```
