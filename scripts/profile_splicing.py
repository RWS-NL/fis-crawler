import cProfile
import pstats
import pathlib
import logging
from fis.dropins.io import load_dropins_with_spatial_matching
from fis.dropins.splicing import splice_fairways
from fis.dropins.embedded import identify_embedded_structures

logging.basicConfig(level=logging.INFO)


def run_splicing():
    source_dir = pathlib.Path("output/fis-export")
    disk_dir = pathlib.Path("output/disk-export")

    # Load data
    res = load_dropins_with_spatial_matching(source_dir, disk_dir)
    lock_complexes, bridge_complexes, terminals, berths, sections, openings = res

    from fis.dropins.core import _map_dropins_to_sections

    dropins_by_section = _map_dropins_to_sections(
        lock_complexes, bridge_complexes, terminals, berths
    )
    embedded_bridges = identify_embedded_structures(lock_complexes, bridge_complexes)

    # Profile only the splicing part
    # Limit to first 500 sections for a quick look if needed, or run full
    # sections_subset = sections.iloc[:500]
    print("Starting profile of splice_fairways...")
    profiler = cProfile.Profile()
    profiler.enable()
    _ = splice_fairways(sections, dropins_by_section, embedded_bridges, mode="detailed")
    profiler.disable()

    stats = pstats.Stats(profiler).sort_stats("cumulative")
    stats.print_stats(30)
    stats.sort_stats("time").print_stats(30)


if __name__ == "__main__":
    run_splicing()
