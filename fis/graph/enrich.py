"""Graph enrichment module.

Re-exports enrichment functions from source-specific modules.
"""

# FIS enrichment
from .enrich_fis import (
    load_fis_enrichment_data,
    match_by_geometry,
    match_by_route_km,
    build_fis_section_enrichment,
    enrich_fis_graph,
)

# EURIS enrichment
from .enrich_euris import (
    load_euris_sailing_speed,
    enrich_euris_with_speed,
)

__all__ = [
    # FIS
    'load_fis_enrichment_data',
    'match_by_geometry',
    'match_by_route_km',
    'build_fis_section_enrichment',
    'enrich_fis_graph',
    # EURIS
    'load_euris_sailing_speed',
    'enrich_euris_with_speed',
]
