# Bridge Schematization

This module processes FIS bridge data into detailed subgraph features for navigation network modelling. 
It supports identifying bridge openings, their physical dimensions (width, height), and their 
spatial association with the fairway network.

## Usage

```bash
uv run python -m fis.cli bridge schematize
```

## Features

- **Opening Matching**: Detects individual openings and their constraints.
- **Physical Geometry**: Replaces zero-length passage points with a 2-meter physical LineString oriented along the fairway.
- **Constraint Aggregation**: Aggregates constraints across all openings to compute the minimum navigable width and height for each bridge representation (used when integrated in `simplified` mode).

## Module Structure

```
fis/bridge/
├── cli.py      # CLI commands
├── core.py     # Data loading, bridge grouping, and spatial association
└── graph.py    # Node/edge feature generation
```
