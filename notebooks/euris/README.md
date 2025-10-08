# EURIS Graph Dataset

This dataset contains a graph representation of the European River Information Services (EURIS) network, designed for inland waterway transport modeling.

## Disclaimer

This dataset is an extract of the original data hosted on the EURIS platform and is intended for **research purposes only**. It should **not** be used for navigational purposes. For official and up-to-date information, please refer to the [EuRIS Portal](https://eurisportal.eu).

## Data Source

The data is generated based on the web services provided by the official [EuRIS Portal](https://eurisportal.eu).

## Generation Process

The creation of this dataset involves several steps:

1.  **Data Aggregation**: Information is collected from the various national waterway authorities by combining data from different countries into a unified structure.
2.  **Graph Construction**: The aggregated data is then used to build a network graph, where fairway sections become edges and junctions or points of interest become nodes. One key step is that multiple EURIS nodes at the same geographic location are merged into a single graph node.
3.  **Border Connectivity**: A specific step is taken to connect the waterway networks across national borders, creating a seamless pan-European graph. This is achieved by identifying and linking corresponding border points from adjacent countries.

## Purpose

The primary purpose of this dataset is to provide a network graph for inland waterway transport modeling. It can be used for various analyses, such as route planning, traffic simulation, and network analysis.

## Graph Structure and Data Schema

The graph is composed of nodes and edges. The attributes for these features correspond to the data models provided by the EURIS API. For detailed information on the data attributes and schema, please refer to the [Fairway Information API documentation](https://eurisportal.eu/fairway-information-api-documentation).

### Nodes

Nodes represent junctions, locks, bridges, and other key points in the waterway network. Each node contains several attributes, including:
*   `n`: The unique identifier for the node in the graph (e.g., `DE_J3240`).
*   `function`: The function of the node (e.g., `junction`, `lock`).
*   `ww_name`: The name of the waterway the node is on (e.g., `Rhein`).
*   `countrycode`: The country where the node is located (e.g., `DE`).
*   `euris_nodes`: A list containing the original EURIS node records that were merged into this single graph node. This is important as some locations in the source data are represented by multiple nodes.
*   `subgraph`: An identifier for the connected component of the graph that the node belongs to.

### Edges

Edges represent the fairway sections connecting the nodes. Key attributes include:
*   `source`: The starting node of the edge (e.g., `DE_J3240`).
*   `target`: The ending node of the edge (e.g., `DE_J1120`).
*   `name`: The name of the fairway section (e.g., `Rhein`).
*   `ww_name`: The name of the waterway the section belongs to.
*   `length_m`: The length of the section in meters.
*   `cemt`: The CEMT class of the waterway section (e.g., `VIc`).
*   `countrycode`: The country where the edge is located (e.g., `DE`).


## Files

This dataset includes the following files:

*   `export-nodes-v0.1.0.geojson`: A GeoJSON file containing the geographic points for all nodes in the network.
*   `export-edges-v0.1.0.geojson`: A GeoJSON file containing the geographic lines for all edges (fairway sections) in the network.
*   `export-graph-v0.1.0.pickle`: A Python pickle file containing a `networkx` graph object, which includes all nodes, edges, and their attributes, representing the complete waterway network.
*   `ris_index_v0.1.0.gpkg`: A GeoPackage file related to the River Information Services (RIS) index. For more information on this data, see the [RIS Index API documentation](https://eurisportal.eu/ris-index-api-documentation).
