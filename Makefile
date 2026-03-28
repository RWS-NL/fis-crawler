.PHONY: all crawl crawl-fis crawl-euris crawl-disk build-graphs merge-graphs schematize validate clean logs-dir

# Default target
all: crawl build-graphs merge-graphs schematize validate

logs-dir:
	mkdir -p output/logs

# --- Crawling Steps ---
crawl: crawl-fis crawl-euris crawl-disk

crawl-fis: logs-dir
	uv run scrapy crawl dataservice -L INFO 2>&1 | tee output/logs/crawl-fis.log

crawl-euris: logs-dir
	uv run scrapy crawl euris -L INFO 2>&1 | tee output/logs/crawl-euris.log

crawl-disk: logs-dir
	uv run scrapy crawl disk -L INFO 2>&1 | tee output/logs/crawl-disk.log

# --- Graph Building ---
build-graphs: build-fis-graph build-euris-graph

build-fis-graph: crawl-fis logs-dir
	uv run python -m fis.cli graph fis 2>&1 | tee output/logs/build-fis-graph.log
	uv run python -m fis.cli graph enrich-fis 2>&1 | tee output/logs/enrich-fis-graph.log

build-euris-graph: crawl-euris logs-dir
	uv run python -m fis.cli graph euris 2>&1 | tee output/logs/build-euris-graph.log
	uv run python -m fis.cli graph enrich-euris 2>&1 | tee output/logs/enrich-euris-graph.log

# --- Merging ---
merge-graphs: build-graphs logs-dir
	uv run python -m fis.cli graph merge 2>&1 | tee output/logs/merge-graphs.log

# --- Schematization ---
schematize: schematize-lock schematize-bridge schematize-dropins

schematize-lock: merge-graphs logs-dir
	uv run python -m fis.cli lock schematize --fis-graph output/fis-graph/graph.pickle 2>&1 | tee output/logs/schematize-lock.log

schematize-bridge: merge-graphs logs-dir
	uv run python -m fis.cli bridge schematize 2>&1 | tee output/logs/schematize-bridge.log

schematize-dropins: merge-graphs logs-dir
	uv run python -m fis.cli dropins schematize 2>&1 | tee output/logs/schematize-dropins.log

# --- Validation ---
validate: validate-fis validate-merged

validate-fis: build-fis-graph logs-dir
	uv run python -m fis.cli graph validate --graph output/fis-enriched/graph.pickle --schema config/schema.toml --output-file output/fis_validation_report.md 2>&1 | tee output/logs/validate-fis.log

validate-merged: merge-graphs logs-dir
	uv run python -m fis.cli graph validate --graph output/merged-graph/graph.pickle --schema config/schema.toml --output-file output/merged_validation_report.md 2>&1 | tee output/logs/validate-merged.log


# --- Utilities ---
clean:
	rm -rf output/
