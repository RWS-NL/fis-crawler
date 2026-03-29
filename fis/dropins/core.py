import logging
import pathlib
from typing import List, Dict, Any

import pandas as pd

from fis.dropins.io import export_graph
from fis.dropins.embedded import identify_embedded_structures, inject_embedded_bridges
from fis.dropins.splicing import splice_fairways
from fis.dropins.graph import generate_simplified_passages
from fis.dropins.terminals import generate_terminal_graph_features
from fis.dropins.berths import generate_berth_graph_features
from fis.lock.graph import build_graph_features as lock_graph_features
from fis.bridge.graph import build_graph_features as bridge_graph_features
from fis import utils

logger = logging.getLogger(__name__)


@utils.timer
def build_integrated_dropins_graph(
    lock_complexes: List[Dict],
    bridge_complexes: List[Dict],
    terminals: List[Dict],
    berths: List[Dict],
    sections: pd.DataFrame,
    _openings: pd.DataFrame,
    output_dir: pathlib.Path,
    mode="detailed",
    include_berths=False,
):
    """
    Main orchestrator to build the completely integrated Drop-ins graph.
    Expects all drop-in data and fairway sections to be provided in canonical format.
    """

    embedded_bridges = identify_embedded_structures(lock_complexes, bridge_complexes)
    dropins_by_section = _map_dropins_to_sections(
        lock_complexes,
        bridge_complexes,
        terminals,
        berths if include_berths else [],
    )
    all_features = splice_fairways(
        sections, dropins_by_section, embedded_bridges, mode=mode
    )

    # Determine which complexes to detail and which to simplify based on mode
    detailed_locks = lock_complexes if mode == "detailed" else []
    simplified_locks = [] if mode == "detailed" else lock_complexes

    detailed_bridges = []
    simplified_bridges = []
    for b in bridge_complexes:
        # A bridge is detailed if mode is 'detailed' (regardless of embedding)
        if mode == "detailed":
            detailed_bridges.append(b)
        else:
            simplified_bridges.append(b)

    logger.info("Generating internal domain graph features for detailed locks...")
    all_features.extend(lock_graph_features(detailed_locks))

    logger.info("Generating internal domain graph features for detailed bridges...")
    all_features.extend(bridge_graph_features(detailed_bridges))

    logger.info("Generating terminal nodes and access edges...")
    all_features.extend(generate_terminal_graph_features(terminals))

    if include_berths:
        logger.info("Generating berth nodes and access edges...")
        all_features.extend(generate_berth_graph_features(berths))

    logger.info(
        "Generating simplified passage edges for standalone/simplified structures..."
    )
    all_features.extend(generate_simplified_passages(simplified_locks, "lock"))
    all_features.extend(generate_simplified_passages(simplified_bridges, "bridge"))

    if mode == "detailed":
        all_features = inject_embedded_bridges(
            all_features, lock_complexes, bridge_complexes, embedded_bridges
        )

    export_graph(
        all_features,
        lock_complexes,
        bridge_complexes,
        terminals,
        berths if include_berths else [],
        output_dir,
    )
    logger.info("Done! Exported integrated dropins graph to %s", output_dir)


def _map_dropins_to_sections(
    lock_complexes: List[Dict],
    bridge_complexes: List[Dict],
    terminals: List[Dict],
    berths: List[Dict] = None,
) -> Dict[Any, List[Dict]]:
    """
    Creates a reverse mapping of fairway section ID to all drop-ins (locks/bridges/terminals/berths)
    that are spatially associated with that section.
    """
    dropins_by_section = {}
    for lock in lock_complexes:
        for sec in lock.get("sections", []):
            sid = utils.stringify_id(sec["id"])
            dropins_by_section.setdefault(sid, []).append({"type": "lock", "obj": lock})
    for bridge in bridge_complexes:
        for sec in bridge.get("sections", []):
            sid = utils.stringify_id(sec["id"])
            dropins_by_section.setdefault(sid, []).append(
                {"type": "bridge", "obj": bridge}
            )
    for term in terminals:
        sid = utils.stringify_id(term.get("FairwaySectionId"))
        if not sid:
            raise ValueError(
                f"Terminal {term.get('id', term.get('Id'))} has no FairwaySectionId and cannot be spliced."
            )
        dropins_by_section.setdefault(sid, []).append({"type": "terminal", "obj": term})
    if berths:
        for berth in berths:
            sid = utils.stringify_id(berth.get("FairwaySectionId"))
            if not sid:
                raise ValueError(
                    f"Berth {berth.get('id', berth.get('Id'))} has no FairwaySectionId and cannot be spliced."
                )
            dropins_by_section.setdefault(sid, []).append(
                {"type": "berth", "obj": berth}
            )

    return dropins_by_section
