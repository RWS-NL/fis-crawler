"""Graph enrichment module.

Re-exports enrichment functions from source-specific modules.
"""

import warnings

# FIS enrichment
from .enrich_fis import (
    load_fis_node_enrichments,
    match_by_geometry,
    match_by_route_km,
    build_fis_edge_enrichments,
    enrich_fis_graph,
)

# EURIS enrichment
from .enrich_euris import (
    load_euris_sailing_speed,
    enrich_euris_with_speed,
)


# Backward-compatible aliases
def load_fis_enrichment_data(*args, **kwargs):
    warnings.warn(
        "load_fis_enrichment_data is deprecated, use load_fis_node_enrichments instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return load_fis_node_enrichments(*args, **kwargs)


def build_fis_section_enrichment(*args, **kwargs):
    warnings.warn(
        "build_fis_section_enrichment is deprecated, use build_fis_edge_enrichments instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return build_fis_edge_enrichments(*args, **kwargs)


__all__ = [
    # FIS
    "load_fis_node_enrichments",
    "load_fis_enrichment_data",  # Deprecated
    "match_by_geometry",
    "match_by_route_km",
    "build_fis_edge_enrichments",
    "build_fis_section_enrichment",  # Deprecated
    "enrich_fis_graph",
    # EURIS
    "load_euris_sailing_speed",
    "enrich_euris_with_speed",
]
