from shapely.geometry import Polygon, Point
from fis.lock.utils import find_chamber_doors


def test_find_chamber_doors_precision_fallback():
    # A chamber geometry that resembles Zudkolk Krammerjachtensluis (chamber 7069818)
    # The important part is that the MRR centerline exactly matches the polygon boundary or falls slightly short
    # due to floating point precision when intersected.

    # These coordinates are taken directly from the failing chamber in EPSG:4326
    c_geom = Polygon(
        [
            (4.16028026811818, 51.6648651575732),
            (4.16041062519308, 51.6648689778381),
            (4.16097222239064, 51.664885972684),
            (4.16139259692607, 51.6648984387447),
            (4.1613861306317, 51.6649813611466),
            (4.16100819902583, 51.6649704093808),
            (4.16055973951278, 51.6649562453186),
            (4.16021836337054, 51.6649448649865),
            (4.16021782468506, 51.664952473361),
            (4.16021388042965, 51.6649523960627),
            (4.16022527364893, 51.6648120381455),
            (4.16022870308311, 51.6648124426553),
            (4.16022487701844, 51.6648629409528),
            (4.16028026811818, 51.6648651575732),
        ]
    )

    # These point inputs triggered the identical points fallback in CI
    split_pt = Point(4.162249937399088, 51.66503049559424)
    merge_pt = Point(4.159651548094601, 51.66495446907182)

    start_door, end_door = find_chamber_doors(c_geom, split_pt, merge_pt)

    # The bug was that start_door and end_door were returned as the exact same identical Point
    # The fix ensures the MRR centerline is extended, guaranteeing a proper intersection
    # instead of a fallback to identical nearest_points.
    assert start_door is not None
    assert end_door is not None

    # They should be distinct points across the chamber from one another
    assert start_door.distance(end_door) > 0.0001, (
        "Start and end doors should not be identical"
    )

    # Optional sanity check: The doors should be on the boundary of the chamber
    # buffer slightly for floating point errors in intersection comparison
    assert c_geom.boundary.distance(start_door) < 1e-6
    assert c_geom.boundary.distance(end_door) < 1e-6
