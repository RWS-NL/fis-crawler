## Repository Overview

This repository is a data processing and scraping project focused on inland waterway information, with two main components:

### 1. Scrapy Project (`vaarweginformatie`)

- Contains the `DataserviceSpider` for crawling and extracting data from [vaarweginformatie.nl](https://www.vaarweginformatie.nl).
- Uses pipelines (see `pipelines.py`) to export data in multiple formats (JSONL, JSON, Parquet, GeoJSON, GeoParquet).
- Configured via `settings.py`, with middleware support in `middlewares.py`.
- Output data is stored in a directory (default: `fis-export` for vaarweginformatie.nl and `euris-export` for euris ).

#### Running the Scrapy Spider

To run the default Scrapy spider (`dataservice`), use the following command from the project root:

```bash
# For  vaarweginformatie.nl
scrapy crawl dataservice 
# For EURIS:
scrapy crawl euris
```

Scrapy will store processed output in the configured export directory (default: `fis-export` and `euris-export` for euris) .

To reduce excessive logging and only show informational messages and above, add the `-L INFO` option:

```bash
scrapy crawl dataservice -L INFO
```

If you can't run the scrapy executable, you can also run the module `python -m scrapy.cmdline`


### 2. Jupyter Notebooks (`notebooks`)

- A collection of notebooks for data analysis, visualization, and further processing of the scraped data.
- Notebooks cover topics such as:
  - Downloading and processing the latest datasets from the EURIS portal (`euris/latest-downloads.ipynb`)
  - Network analysis (`network.ipynb`)
  - Data schematization (`schematize-lock.ipynb`)
  - GeoJSON conversion and graph generation (`euris/euris-to-geojson.ipynb`, `euris/generate-graph.ipynb`)
- Uses libraries such as pandas, geopandas, networkx, and requests for data manipulation and visualization.

### Other Notable Files

- `pyproject.toml`: Python project configuration.
- `scrapy.cfg`: Scrapy configuration file.
- `qgis`: QGIS project and style files for geospatial visualization.

### 3. Lock Graph Schematization

The `fis/schematize.py` script generates a graph representation of lock complexes, exporting to `output/lock-output/lock_schematization.geojson`.

#### Feature Types (`feature_type`)
- **`lock`**: The lock complex (polygon).
- **`chamber`**: Individual lock chambers (polygon).
- **`berth`**: Waiting areas near the lock.
- **`fairway_segment`**: LineStrings representing the sailing path.
  - `segment_type="before"`: Upstream fairway segment.
  - `segment_type="after"`: Downstream fairway segment.
  - `segment_type="chamber_approach"`: Split node to Chamber Start.
  - `segment_type="chamber_route"`: Inside the chamber (Start to End).
  - `segment_type="chamber_exit"`: Chamber End to Merge node.
- **`node`**: Topological points.
  - `node_type="lock_split"`: Divergence point from main fairway.
  - `node_type="lock_merge"`: Convergence point back to main fairway.
  - `node_type="chamber_start"`: Entrance of the chamber.
  - `node_type="chamber_end"`: Exit of the chamber.

#### ID and Naming Scheme
- **Lock**: Uses raw FIS `Id`.
- **Chamber**: Uses raw FIS `Id`.
- **Nodes**:
  - Split: `lock_{lock_id}_split`
  - Merge: `lock_{lock_id}_merge`
  - Chamber Start: `chamber_{chamber_id}_start`
  - Chamber End: `chamber_{chamber_id}_end`

