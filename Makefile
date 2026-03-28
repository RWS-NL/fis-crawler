.PHONY: all crawl crawl-fis crawl-euris crawl-disk build-graphs merge-graphs schematize validate clean

# Default target
all: crawl build-graphs merge-graphs schematize validate

# --- Crawling Steps ---
crawl: crawl-fis crawl-euris crawl-disk

crawl-fis:
	uv run scrapy crawl dataservice -L INFO

crawl-euris:
	uv run scrapy crawl euris -L INFO

crawl-disk:
	uv run scrapy crawl disk -L INFO


# --- Graph Building ---
build-graphs: build-fis-graph build-euris-graph

build-fis-graph: crawl-fis
	uv run python -m fis.cli graph fis
	uv run python -m fis.cli graph enrich-fis

build-euris-graph: crawl-euris
	uv run python -m fis.cli graph euris
	uv run python -m fis.cli graph enrich-euris


# --- Graph Merging ---
merge-graphs: build-fis-graph build-euris-graph
	uv run python -m fis.cli graph merge


# --- Schematization ---
schematize: schematize-lock schematize-bridge schematize-dropins

schematize-lock: crawl-disk build-fis-graph
	uv run python -m fis.cli lock schematize --fis-graph output/fis-graph/graph.pickle

schematize-bridge: crawl-disk build-fis-graph
	uv run python -m fis.cli bridge schematize

schematize-dropins: crawl-disk build-fis-graph
	uv run python -m fis.cli dropins schematize --mode simplified --output-dir output/dropins-schematization-simplified
	uv run python -m fis.cli dropins schematize --mode detailed --output-dir output/dropins-schematization-detailed
	uv run python -m fis.cli dropins schematize --include-berths --output-dir output/integrated-schematization-with-berths


# --- Validation ---
validate: validate-fis validate-merged

validate-fis: build-fis-graph
	uv run python -m fis.cli graph validate --graph output/fis-enriched/graph.pickle --schema config/schema.toml --output-file output/fis_validation_report.md

validate-merged: merge-graphs
	uv run python -m fis.cli graph validate --graph output/merged-graph/graph.pickle --schema config/schema.toml --output-file output/merged_validation_report.md


# --- Utilities ---
clean:
	rm -rf output/
