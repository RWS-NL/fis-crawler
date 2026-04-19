"""
Shared pytest fixtures for the fis-crawler test suite.
"""

import pytest
from shapely.geometry import Point, LineString, Polygon


@pytest.fixture
def sluis_weurt_complex():
    """
    Minimal synthetic representation of Sluis Weurt (lock complex 49032).

    Geometry (WGS84, coordinates within the real-world extent
    5.81598793,51.85016437,5.82974769,51.85750686):

      • Fairway runs E–W at latitude 51.8538.
      • Split point : (5.818, 51.8538) – west of the complex, on the fairway.
      • Merge point : (5.826, 51.8538) – east of the complex, on the fairway.
      • Chamber 40927 (north branch) : lon 5.819–5.825, lat 51.854–51.856
        – no internal junctions.
      • Chamber 47538 (south branch) : lon 5.819–5.825, lat 51.852–51.854
        – contains internal FIS junction 8864190 (NL_J2501) at (5.822, 51.853).

    Both split and merge are deliberately placed *outside* every chamber polygon,
    which is the correct (valid) schematization.  The invalid case
    (merge landing inside chamber 47538) is exercised in test_sluis_weurt.py by
    mutating ``geometry_after_wkt`` within the individual test.
    """
    # Fairway section running E–W, centred through the lock complex
    section_line = LineString([(5.808, 51.8538), (5.838, 51.8538)])

    # North chamber 40927 – large rectangle north of the fairway, no internal junctions
    ch_40927_poly = Polygon([
        (5.819, 51.854), (5.825, 51.854),
        (5.825, 51.856), (5.819, 51.856),
        (5.819, 51.854),
    ])

    # South chamber 47538 – large rectangle south of the fairway
    # Contains internal FIS junction 8864190 at (5.822, 51.853).
    ch_47538_poly = Polygon([
        (5.819, 51.852), (5.825, 51.852),
        (5.825, 51.854), (5.819, 51.854),
        (5.819, 51.852),
    ])

    # Enclosing lock complex polygon (spans the full extent of both chambers)
    complex_poly = Polygon([
        (5.817, 51.851), (5.827, 51.851),
        (5.827, 51.857), (5.817, 51.857),
        (5.817, 51.851),
    ])

    # Split (upstream / west) and merge (downstream / east) points
    split_lon, split_lat = 5.818, 51.8538
    merge_lon, merge_lat = 5.826, 51.8538

    geometry_before_wkt = LineString(
        [(5.808, 51.8538), (split_lon, split_lat)]
    ).wkt
    geometry_after_wkt = LineString(
        [(merge_lon, merge_lat), (5.838, 51.8538)]
    ).wkt

    return {
        "id": "49032",
        "name": "Sluis Weurt",
        "geometry": complex_poly.wkt,
        "fairway_id": "fw_weurt",
        "fairway_name": "Maas-Waalkanaal",
        "geometry_before_wkt": geometry_before_wkt,
        "geometry_after_wkt": geometry_after_wkt,
        "sections": [
            {
                "id": "sec_weurt",
                "fairway_id": "fw_weurt",
                "geometry": section_line.wkt,
                "relation": "overlap",
            }
        ],
        "locks": [
            {
                "id": "49032",
                "chambers": [
                    {
                        "id": "40927",
                        "name": "Sluis Weurt Noord",
                        "geometry": ch_40927_poly.wkt,
                        "dim_usable_length": 250,
                        "dim_gate_width": 16,
                        "internal_junctions": [],
                    },
                    {
                        "id": "47538",
                        "name": "Sluis Weurt Zuid",
                        "geometry": ch_47538_poly.wkt,
                        "dim_usable_length": 250,
                        "dim_gate_width": 16,
                        "internal_junctions": [
                            {
                                "id": "8864190",
                                "geometry": Point(5.822, 51.853),
                            }
                        ],
                    },
                ],
            }
        ],
    }
