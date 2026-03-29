.PHONY: all crawl crawl-fis crawl-euris crawl-disk build-graphs merge-graphs schematize validate clean logs-dir download-bivas validate-bivas

# Default target
all: crawl build-graphs merge-graphs schematize validate

logs-dir:
	mkdir -p output/logs

# --- Crawling Steps ---
crawl: crawl-fis crawl-euris crawl-disk

reference/Bivas.5.10.1.sqlite:
	mkdir -p reference
	@echo "Downloading BIVAS database from Google Drive..."
	curl -L -c reference/cookies.txt 'https://docs.google.com/uc?export=download&id=1s2QXcWnpUkALgF17zBKKv3j6ZVdKUXIP' | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1/p' > reference/confirm.txt
	curl -L -b reference/cookies.txt 'https://docs.google.com/uc?export=download&confirm='$$(cat reference/confirm.txt)'&id=1s2QXcWnpUkALgF17zBKKv3j6ZVdKUXIP' -o reference/BIVAS_v5.10.1.zip
	rm reference/cookies.txt reference/confirm.txt
	unzip -o reference/BIVAS_v5.10.1.zip -d reference/
	mv reference/Bivas.db reference/Bivas.5.10.1.sqlite
	@echo "BIVAS database ready at reference/Bivas.5.10.1.sqlite"

download-bivas: reference/Bivas.5.10.1.sqlite

output/fis-export/.fis_crawl_complete: logs-dir
	mkdir -p output/fis-export
	uv run scrapy crawl dataservice -L INFO 2>&1 | tee output/logs/crawl-fis.log
	@if [ ! -f output/fis-export/section.geoparquet ] || [ ! -f output/fis-export/sectionjunction.geoparquet ]; then \
		echo "FIS crawl incomplete: missing section or junction files"; exit 1; \
	fi
	touch $@

crawl-fis: output/fis-export/.fis_crawl_complete

output/euris-export/.euris_crawl_complete: logs-dir
	mkdir -p output/euris-export
	uv run scrapy crawl euris -L INFO 2>&1 | tee output/logs/crawl-euris.log
	@if ! ls output/euris-export/Node_*.geojson >/dev/null 2>&1 || ! ls output/euris-export/FairwaySection_*.geojson >/dev/null 2>&1; then \
		echo "EURIS crawl incomplete: missing Node or FairwaySection files"; exit 1; \
	fi
	touch $@

crawl-euris: output/euris-export/.euris_crawl_complete

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
validate: validate-fis validate-merged validate-bivas

validate-fis: build-fis-graph logs-dir
	uv run python -m fis.cli graph validate --graph output/fis-enriched/graph.pickle --schema config/schema.toml --output-file output/fis_validation_report.md 2>&1 | tee output/logs/validate-fis.log

validate-merged: merge-graphs logs-dir
	uv run python -m fis.cli graph validate --graph output/merged-graph/graph.pickle --schema config/schema.toml --output-file output/merged_validation_report.md 2>&1 | tee output/logs/validate-merged.log

FIS_ENRICHED = output/fis-enriched/edges.geoparquet

validate-bivas: reference/Bivas.5.10.1.sqlite logs-dir
	@if [ ! -f $(FIS_ENRICHED) ]; then \
		$(MAKE) build-fis-graph; \
	fi
	uv run python scripts/bivas/compare_networks.py \
		--bivas-db reference/Bivas.5.10.1.sqlite \
		--bivas-version 5.10.1 \
		--fis-edges $(FIS_ENRICHED) \
		--fis-version $$(date +%Y%m%d) \
		--output-dir output/bivas-validation 2>&1 | tee output/logs/validate-bivas.log


# --- Utilities ---
clean:
	rm -rf output/
