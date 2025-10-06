## Repository Overview

This repository is a data processing and scraping project focused on inland waterway information, with two main components:

### 1. Scrapy Project (`vaarweginformatie`)

- Contains the `DataserviceSpider` for crawling and extracting data from [vaarweginformatie.nl](https://www.vaarweginformatie.nl).
- Uses pipelines (see `pipelines.py`) to export data in multiple formats (JSONL, JSON, Parquet, GeoJSON, GeoParquet).
- Configured via `settings.py`, with middleware support in `middlewares.py`.
- Output data is stored in a directory (default: `fis-export`).

#### Running the Scrapy Spider

To run the default Scrapy spider (`dataservice`), use the following command from the project root:

```bash
scrapy crawl dataservice
```

By default, Scrapy outputs scraped items to the screen (stdout).  
To save the output to a file (e.g., JSON Lines format), use the `-o` option:

```bash
scrapy crawl dataservice -o output.jsonl
```

You can specify different formats by changing the file extension (`.jsonl`, `.json`, `.csv`, etc.).

Scrapy will also store processed output in the configured export directory (default: `fis-export`) if pipelines are enabled.

To reduce excessive logging and only show informational messages and above, add the `-L INFO` option:

```bash
scrapy crawl dataservice -L INFO
```

You can combine this with the `-o` option as needed.

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
- `vaarweginformatie.jsonl`: Example or output data file.
- `qgis`: QGIS project and style files for geospatial visualization.

### Summary

This repository combines automated data extraction (via Scrapy) with interactive data analysis and visualization (via Jupyter notebooks), primarily targeting inland waterway network and infrastructure data.