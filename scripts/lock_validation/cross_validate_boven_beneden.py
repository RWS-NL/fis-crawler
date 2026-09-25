"""Cross-validate the automatic boven/beneden resolution against the manually
curated MANUAL_WATERWAY_LEVELS table.

See docs/werkwijze_sluiscontrole.md §3.4 for the definition and method, and the
"Acceptatie-eis" in the plan for issue #58 / PR #188: clean canal-pand locks must
MATCH on both side and value before the automatic method is trusted to override
the manual table.

Usage: uv run python scripts/lock_validation/cross_validate_boven_beneden.py
"""

import logging
import geopandas as gpd

from fis.lock.orientation import cross_validate_manual_levels

logger = logging.getLogger(__name__)

NODES_PATH = "output/lock-schematization/nodes.geoparquet"
LOCK_PATH = "output/lock-schematization/lock.geoparquet"
OUTPUT_CSV = "output/lock-schematization/boven_beneden_cross_validation.csv"


def main():
    nodes = gpd.read_parquet(NODES_PATH)
    locks = gpd.read_parquet(LOCK_PATH)

    result = cross_validate_manual_levels(nodes, locks)
    result.to_csv(OUTPUT_CSV, index=False)

    logger.info("Wrote %d rows to %s", len(result), OUTPUT_CSV)
    logger.info("\n%s", result["category"].value_counts().to_string())
    for category in [
        "MATCH",
        "SIDE_MISMATCH",
        "VALUE_MISMATCH",
        "PARTIAL",
        "UNRESOLVED",
        "NO_LOCK_MATCH",
    ]:
        subset = result[result["category"] == category]
        if subset.empty:
            continue
        table_str = subset[
            [
                "sluis_key",
                "lock_name",
                "manual_peil_hoog",
                "manual_peil_laag",
                "auto_boven_nap",
                "auto_beneden_nap",
                "source",
            ]
        ].to_string(index=False)
        logger.info("--- %s ---\n%s", category, table_str)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    main()
