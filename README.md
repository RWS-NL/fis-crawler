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
