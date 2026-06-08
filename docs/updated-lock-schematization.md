# Restructuring Lock & Bridge Schematization: Proposed Design & Plan

This document proposes a ground-up restructuring of the lock and bridge schematization pipeline in the `fis-crawler` repository to address the topological routing failures and integration issues reported in #143, #145, and #147. 

The main goals are:
- Decoupling crawling, base graph construction, structure collection, and graph integration.
- Separating pure administrative data collection from spatial snapping and network topology routing.
- Using graph-aware traversal to dynamically discover complex lock boundaries (such as for the Volkeraksluizen "jachtsluis").
- Explicitly showing and matching berths alongside locks and bridges.
- Managing mapping ambiguities (such as bridge openings and lock chambers) through manual overrides.

---

## 1. Redefined 5-Stage Workflow

The pipeline is split into five decoupled stages. The key separation is between **Stage 3 (Administrative Collection)** and **Stage 4 (Spatial & Topological Integration)**. 

### Workflow Stages

1. **Stage 1: Crawl (unchanged)**: Crawls dataservices and exports raw files to `output/*-export/`.
2. **Stage 2: Build Base Graph (unchanged)**: Builds basic network graphs and merges them into `output/merged-graph/`.
3. **Stage 3: Administrative Collection**: Independently loads database files for locks, chambers, berths, bridges, and openings, parsing their raw attributes into canonical hierarchy JSONs. **No graph input, no spatial matching, and no node/edge generation are performed here.**
4. **Stage 4: Lock Complex Integration (NEW)**: Loads the administrative JSONs (including chambers, berths, and bridges) from Stage 3 and the base graph from Stage 2. Traverses the graph to resolve boundaries, matches structures and berths spatially to fairway sections, resolves bridge-to-chamber mappings, and generates the detailed routing subgraphs (split, merge, door, and berth wait nodes, along with internal/berth access edges).
5. **Stage 5: Fairway Splicing + Export**: Splices the generated subgraphs from Stage 4 into the fairway sections of the base graph and exports the final geoparquet/geojson files.

### Workflow Diagrams

#### As-Is Workflow (Tightly Coupled)
![Current Workflow](current_workflow.svg)

#### To-Be Workflow (Decoupled with Graph-Aware Topology)
![Proposed Workflow](proposed_workflow.svg)

---

## 2. Topological Challenge Patterns & Rules

To solve the complex spatial layouts, we define explicit topological rules for the main challenge cases.

### Case A: Bridges *in* Locks (Embedded Bridges)
When a bridge opening physically resides inside a lock chamber (e.g., Sluis Weurt, Chamber 47538 with Bridge 5835).
- The chamber route is split into internal routes, and bridge opening nodes (`opening_start`, `opening_end`) are inserted in series.

---

### Case B: Bridges *after* Locks (Adjacent Bridges)
When a bridge opening is adjacent to the lock chamber exit or approach, but outside the chamber polygon itself.
- The bridge is placed on the specific chamber exit/approach segment and *not* inside the chamber route or on the shared merged fairway.

---

### Case C: Inverted Directions (Fairway Digitization)
When the digitized direction of the fairway line is opposite to the logical direction of the lock complex flow.
- Topological roles (`split`, `merge`, `start`, `end`) are defined relative to the canonical fairway flow (using `route_km` gradients), and connectivity is verified via undirected graphs in test suites.

---

### Case D: Nodes *in* Lock Chambers (Existing Junctions)
When a pre-existing junction node (degree > 2) falls within the lock's geometric footprint.
- Splicing logic intersects these nodes and preserves them along the approach, route, or exit segments instead of bypassing them.

---

## 3. Network Traversal & Bridge-Chamber-Berth Complexity

### Traversal for Complex Boundaries (e.g. Volkeraksluizen & Oranjesluizen)
For complexes with parallel chambers on distinct branches or separate edges (such as the Volkeraksluizen "jachtsluis" or Oranjesluizen's north/south arms), a simple spatial buffer is insufficient. 
- **Traversal Strategy**: Stage 4 will load the merged base graph. Starting from the chambers' centroid/door points, the algorithm will traverse the graph up and down (e.g., finding the nearest common ancestors and descendants with degree > 2) to dynamically discover the boundaries where the chambers merge back together.
- This ensures that all parallel lock chambers, including the "jachtsluis", are enclosed inside a single consistent lock complex domain boundary.

### Berths Matching & Injection
Berths represent waiting areas adjacent to lock chambers.
- **Matching in Stage 4**: Berths from `lock_complexes.json` are spatially snapped to the fairway sections within the discovered domain boundary. 
- **Topology Injection**: For each berth, a corresponding waiting node is created on the approach or exit edge of the lock, and linked to the main fairway route via access edges. This ensures the berths are correctly integrated into the routing network.

### Bridge-Chamber Mapping Complexity
During traversal, bridges may be found near chambers. Some bridge openings are linked to exactly one chamber, while others span across multiple chambers or approach paths.
- **Manual Administration File**: Because automated spatial/semantic matching can be ambiguous for complex configurations, we will support a manual mapping configuration (e.g., `config/lock_bridge_mappings.toml` or JSON). This configuration will allow overrides to:
  - Explicitly link a bridge opening ID to a specific chamber ID.
  - Define custom start/end junction node overrides for specific lock complexes if the dynamic traversal falls short.
  - Mark specific bridges as adjacent vs. embedded.

### Exceptional Navigational Structures (Weirs & Retaining Locks) Integration
Exceptional structures (like weirs and retaining locks) located within the lock complex boundaries must be integrated:
- **Retaining Locks (`KSS`)**: Spliced as navigable passage edges (similar to lock chambers) but tagged with `structure_type = 'keersluis'`.
- **Weirs (`STW`)**: Spliced as non-navigable edges by default (`navigable = false`) to represent physical barriers, unless overrides mark them as navigable (e.g., when fully opened).
- **Opening Coordinate Overrides**: For weirs with stacked openings (e.g., Stuw Grave), `lock_bridge_mappings.toml` will provide the manual coordinate offsets (e.g., matching neighboring bridge openings) to ensure correct separate layout nodes in the graph.

---

## 4. Structured Test Lock Complexes (#147)

We will define structured test scenarios in `tests/test_lock_domains.py` using the 14 validated lock complex domains to verify boundary discovery.

| Complex Name | Entry Node | Exit Node | Topology Pattern | Target Rules Tested |
| :--- | :--- | :--- | :--- | :--- |
| **Volkeraksluizen** | `8860743` | `8866727` | Parallel (2 paths) | Case D, "Jachtsluis" traversal, & Berths |
| **Krammersluizen** | `8864545` | `8866367` | Parallel (2 paths) | Case A (Bridges inside locks) |
| **Oranjesluizen** | `8864384` | `59275858` | Parallel (2 paths) | Case C (Branch-aware routing) |
| **IJmuiden Sluizen** | `8864991` | `8861863` | Parallel (4 paths) | Multi-path traversal |
| **Terneuzen Sluizen** | `8867489` | `8863105` | Parallel (3 paths) | Multi-path traversal |
| **Lorentzsluizen** | `8864239` | `8860933` | Serial (Single section) | Case B (Bridges after locks) |
| **Sluis Weurt** | `8864666` | `8865102` | Serial (Multi-section) | Case A, B, D (Staggered chambers) |
| **Sluis Eefde** | `8860918` | `30986757` | Serial (Multi-lock) | Case C (Direction-independent) |
| **Sluis Born** | `8868208` | `8867148` | Serial (Single section) | Simple serial baseline |
| **Sluis Maasbracht** | `8861292` | `8862583` | Serial (Single section) | Simple serial baseline |
| **Sluis Heel** | `8864929` | `8865890` | Serial (Single section) | Simple serial baseline |
| **Sluis Grave** | `8861448` | `8865198` | Serial (Single section) | Case D (Junctions inside lock) |
| **Kreekraksluizen** | `8868181` | `8867425` | Serial (Multi-section) | Case C (Inverted sections) |
| **Sluis Linne** | `8864929` | `8861324` | Serial (Single section) | Simple serial baseline |

---

## 5. Multi-Phase Exploration & Implementation Roadmap

To avoid the regressions and scope creep of prior attempts, we propose a multi-phase approach that prioritizes exploration and test generation before writing integration code.

### Phase 1: Explore & Generate Test Cases/Datasets
1. **Diagnostic Plots**: Create a script `scripts/generate_lock_diagnostics.py` to plot the current network graph topology for the 14 complexes, overlaying chamber/bridge/berth geometries.
2. **Define Gold-Standard Paths**: Formally document the expected node sequence and bridge/berth mappings for Sluis Weurt, Volkeraksluizen, and Oranjesluizen.
3. **Mock Datasets**: Extract and save sub-graphs for these 14 complexes as local test fixtures so we can test the traversal algorithms offline.
4. **Manual Mappings Draft**: Create the initial draft of `config/lock_bridge_mappings.toml`.
5. **Explore Exceptional Structures**: Create a diagnostic script to inspect the raw `exceptionalnavigationalstructure` dataset (attributes, geometries, and type distributions) to understand what exceptional structures (weirs, etc.) exist in the dataset.

### Phase 2: Traversal Algorithm Design & Validation
1. Create [domain.py](file:///Users/baart_f/src/fis/fis/dropins/domain.py) to implement the `extract_lock_complex_domain(G, lock_complex)` traversal logic.
2. Validate the function by running it on the base graphs and verifying that it successfully discovers the entry/exit node pairs from Section 4.
3. Assert that parallel paths (like the "jachtsluis") and associated berths are correctly included.

### Phase 3: Reorganize Workflow & Serialization (Stage 3 & 4)
1. Refactor `fis lock schematize` and `fis bridge schematize` to only perform administrative collection and write raw hierarchies to JSON, removing any spatial and graph code from them.
2. Create the Stage 4 orchestrator, taking the JSONs and base graph, and performing spatial matching (for chambers, bridges, and berths) and detailed topological graph generation.
3. **Exceptional Navigational Structures**: Load the `exceptionalnavigationalstructure` dataset (weirs like Stuw Grave) in Stage 3, map them to fairways in Stage 4, and represent them as special non-navigable nodes unless overrides permit.

### Phase 4: Splicing & Integration (Stage 5)
1. Update `fis dropins schematize` to perform the geometric fairway splicing strictly at the boundaries generated by Stage 4.
2. Ensure standalone structures outside lock complex domains are handled correctly.

### Phase 5: Full Verification
1. Run the entire integration test suite.
2. Generate final network validation reports to ensure zero disconnected components or routing anomalies.

---

## Appendix: D2 Source Documents

### Current Workflow (As-Is)
The D2 source code is located at [current_workflow.d2](file:///Users/baart_f/src/fis/docs/current_workflow.d2).

```text
title: |md
  # Current Workflow (as-is)
  Tightly coupled: lock complex integration happens during dropins,
  requiring both crawled data and graph to be present.
|

direction: down

# Data Sources
crawl: "1. Crawl" {
  style.fill: "#e8f4fd"
  fis: "scrapy crawl dataservice\n→ output/fis-export/"
  euris: "scrapy crawl euris\n→ output/euris-export/"
  disk: "scrapy crawl disk\n→ output/disk-export/"
}

# Graph Building
graph: "2. Build Graphs" {
  style.fill: "#e8f8e8"
  fis-graph: "fis graph fis\n→ output/fis-graph/"
  euris-graph: "fis graph euris\n→ output/euris-graph/"
  enrich-fis: "fis graph enrich-fis\n→ output/fis-enriched/"
  enrich-euris: "fis graph enrich-euris\n→ output/euris-enriched/"
  merge: "fis graph merge\n→ output/merged-graph/"

  fis-graph -> enrich-fis
  euris-graph -> enrich-euris
  enrich-fis -> merge
  enrich-euris -> merge
}

# Schematization (THE PROBLEM AREA)
schematize: "3. Schematize" {
  style.fill: "#fff3cd"
  lock: "fis lock schematize\n→ output/lock-schematization/" {
    style.fill: "#ffcccc"
  }
  bridge: "fis bridge schematize\n→ output/bridge-schematization/" {
    style.fill: "#ffcccc"
  }
  dropins: "4. Integrate Dropins" {
    style.fill: "#ffcccc"
    style.stroke: "#cc0000"
    detailed: "fis dropins schematize --mode detailed"
    simplified: "fis dropins schematize --mode simplified"
  }
  lock -> dropins: "lock_complexes"
  bridge -> dropins: "bridge_complexes"
}

# Connections
crawl.fis -> graph.fis-graph
crawl.euris -> graph.euris-graph
crawl.fis -> graph.enrich-fis
crawl.euris -> graph.enrich-euris

crawl.fis -> schematize.lock: "FIS parquet"
crawl.disk -> schematize.lock: "DISK parquet"
graph.fis-graph -> schematize.lock: "graph.pickle\n(for topology)"
crawl.fis -> schematize.bridge: "FIS parquet"
crawl.disk -> schematize.bridge: "DISK parquet"

crawl.fis -> schematize.dropins: "sections"
crawl.disk -> schematize.dropins: "DISK data"

# Problem annotation
problem: |md
  ## Problems
  1. **Tight coupling**: dropins needs lock + bridge + sections + DISK all at once
  2. **No caching**: changing lock logic requires re-running everything
  3. **Embedded bridge detection** happens during dropins, not during lock/bridge
  4. **Lock complex grouping** (`group_complexes`) does too much at once
  5. **Test data** is ad-hoc, not structured by challenge type
| {
  style.fill: "#ffe0e0"
  style.stroke: "#cc0000"
}
```

### Proposed Workflow (To-Be)
The D2 source code is located at [proposed_workflow.d2](file:///Users/baart_f/src/fis/docs/proposed_workflow.d2).

```text
title: |md
  # Proposed Workflow (to-be)
  Decoupled stages with serialized intermediate artifacts.
  Each stage can run independently from cached outputs.
|

direction: down

# ===== STAGE 1: Data Acquisition (unchanged) =====
stage1: "Stage 1: Crawl (unchanged)" {
  style.fill: "#e8f4fd"
  fis: "scrapy crawl dataservice\n→ output/fis-export/"
  euris: "scrapy crawl euris\n→ output/euris-export/"
  disk: "scrapy crawl disk\n→ output/disk-export/"
}

# ===== STAGE 2: Base Graph (unchanged) =====
stage2: "Stage 2: Build Base Graph (unchanged)" {
  style.fill: "#e8f8e8"
  fis-graph: "fis graph fis + enrich"
  euris-graph: "fis graph euris + enrich"
  merge: "fis graph merge\n→ output/merged-graph/"
  fis-graph -> merge
  euris-graph -> merge
}

# ===== STAGE 3: Administrative Collection (independent) =====
stage3: "Stage 3: Administrative Collection (independent, cacheable)" {
  style.fill: "#e8f0ff"
  style.stroke: "#0066cc"

  lock: "fis lock schematize\n→ output/lock-schematization/\n\nOutputs: lock_complexes.json\n(administrative locks, chambers, berths)" {
    style.fill: "#d0e8ff"
  }
  bridge: "fis bridge schematize\n→ output/bridge-schematization/\n\nOutputs: bridge_complexes.json\n(administrative bridges, openings)" {
    style.fill: "#d0e8ff"
  }
}

# ===== STAGE 4: Lock Complex Integration (NEW) =====
stage4: "Stage 4: Lock Complex Integration (NEW)" {
  style.fill: "#fff0e0"
  style.stroke: "#cc6600"
  label: |md
    **NEW STEP**: Resolves complex lock topology using base graph.
    Runs AFTER both lock and bridge schematization are complete.
    Uses base graph to traverse up/down and find split/merge domain boundaries.
  |

  embedded: "Match bridges & berths to lock\n(traversal + spatial + semantic)"
  inject: "Inject chambers, bridges & berths\ninto lock complex domain"
  validate: "Validate lock complex\ntopology"

  embedded -> inject -> validate
}

# ===== STAGE 5: Fairway Splicing (formerly dropins) =====
stage5: "Stage 5: Fairway Splicing + Export" {
  style.fill: "#e8ffe8"
  style.stroke: "#006600"

  splice: "Splice structures into\nfairway sections"
  detailed: "Detailed mode\n(chamber-level routing)"
  simplified: "Simplified mode\n(single passage edges)"

  splice -> detailed
  splice -> simplified
}

# ===== Connections =====
stage1.fis -> stage2.fis-graph
stage1.euris -> stage2.euris-graph

stage1.fis -> stage3.lock: "FIS parquet"
stage1.disk -> stage3.lock: "DISK parquet"
stage1.fis -> stage3.bridge: "FIS parquet"
stage1.disk -> stage3.bridge: "DISK parquet"

stage3.lock -> stage4.embedded: "lock_complexes.json"
stage3.bridge -> stage4.embedded: "bridge_complexes.json"
stage2.merge -> stage4.embedded: "merged-graph"

stage4.validate -> stage5.splice: "integrated_complexes.json"
stage2.merge -> stage5.splice: "merged-graph"

# ===== Key benefit =====
benefit: |md
  ## Key Benefits
  - **Stage 3 is independently runnable**: modify lock logic, re-run only lock schematize
  - **Stage 4 is isolated**: all bridge-in-lock logic in one place
  - **Serialized artifacts**: each stage reads/writes JSON/parquet, no re-crawling
  - **Test any stage in isolation**: mock inputs from cached files
  - **Stage 4 can have its own test suite** with visual lock complex examples
| {
  style.fill: "#e8ffe8"
}
```
