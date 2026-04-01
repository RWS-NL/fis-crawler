# FIS-EURIS Inland Waterway Network Dataset

## Overview
This dataset provides a harmonized, pan-European inland waterway network integrated with high-resolution Dutch fairway data. It combines the broad topological connectivity of the **European River Information Services (EuRIS)** with the detailed physical and administrative attributes of the **Dutch Fairway Information System (FIS)**.

The network is specifically designed for:
- **Macroscopic Traffic Assignment:** Route planning and strategic transport modeling (e.g., BIVAS).
- **Microscopic Simulation:** Detailed discrete event simulations of lock operations and bridge passages (e.g., OpenTNSim, SIVAK).
- **Network Analysis:** Pan-European connectivity studies and infrastructure bottleneck analysis.

## Data Sources & Integration
### 1. EuRIS (European River Information Services)
Provides the backbone for the pan-European network, ensuring seamless connectivity across 13+ countries. Data includes fairway sections, nodes, and basic dimensions (CEMT classes).
*Source: [eurisportal.eu](https://www.eurisportal.eu)*

### 2. FIS (Dutch Fairway Information System / Vaarweginformatie.nl)
Provides high-resolution data for the Dutch waterway network, including the Rhine corridor. This source contributes detailed information on:
- Hydraulic conditions (water levels and flow velocities).
- Bathymetric profiles and navigable depths.
- Comprehensive infrastructure details for locks, bridges, and berths.
*Source: [vaarweginformatie.nl](https://www.vaarweginformatie.nl)*

### Integration Strategy
The dataset uses a custom processing pipeline to merge these sources:
- **Topological Harmonization:** Geometric snapping and merging of source nodes at border points (e.g., Lobith) to ensure a single, contiguous graph.
- **Attribute Mapping:** FIS attributes are mapped to a canonical schema based on EURIS naming conventions.
- **Detailed Schematization:** Locks and bridges are expanded into detailed subgraphs, representing individual chambers, approach segments, and gate positions.

## Dataset Structure
The dataset is organized into a main integrated graph and several specialized artifact packages.

### Main Artifacts (Available in the root directory)
- **`graph.pickle`**: A serialized Python `networkx.MultiDiGraph` object. This is the most efficient way to load the full network with all attributes into a Python environment.
- **`edges.geojson` / `edges.geoparquet`**: The network edges (fairway segments) with spatial geometry and harmonized attributes (length, CEMT class, dimensions, speed limits).
- **`nodes.geojson` / `nodes.geoparquet`**: The network nodes (junctions, ports, border points) with spatial coordinates.
- **`merged_validation_report.md`**: A detailed report on graph integrity, connectivity, and schema compliance.

### Detailed Packages (Zipped)
- **`fis-export.zip`**: Raw and enriched data from the Dutch FIS system.
- **`euris-export.zip`**: Raw and enriched data from the European EuRIS portal.
- **`schematizations.zip`**: Detailed structural models for locks and bridges, including chamber-level routing and bridge opening geometries.

## Technical Specifications
### Coordinate Reference System
All spatial data is provided in **WGS 84 (EPSG:4326)**. Processing and metric calculations (lengths, areas) are performed using the Dutch projected system **RD New (EPSG:28992)** where applicable.

### Key Attributes
- **Edges:** `id`, `name`, `length_m`, `cemt_class`, `maxspeed`, `dim_width`, `dim_depth`, `dim_height`.
- **Nodes:** `id`, `name`, `vplnpoint` (Voyage Planning Point), `country_code`.

## Attribution & License
### Authors
- **Baart, Fedor** (Rijkswaterstaat) - ORCID: [0000-0001-8231-094X](https://orcid.org/0000-0001-8231-094X)
- **Turpijn, Bas** (Rijkswaterstaat) - ORCID: [0009-0002-6779-1065](https://orcid.org/0009-0002-6779-1065)

### License
This dataset is licensed under the **Creative Commons Attribution 4.0 International (CC-BY 4.0)**.

### Mandatory Attribution
*Data provided by EuRIS (www.eurisportal.eu) and Rijkswaterstaat.*
