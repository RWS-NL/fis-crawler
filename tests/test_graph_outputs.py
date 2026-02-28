"""Tests for the typed GeoDataFrame builder functions in fis.lock.graph."""
import pytest
from shapely.geometry import Point, LineString, Polygon
from fis.lock.graph import (
    build_nodes_gdf, 
    build_edges_gdf, 
    build_berths_gdf,
    build_locks_gdf,
    build_chambers_gdf,
    build_subchambers_gdf
)


# ---------------------------------------------------------------------------
# Minimal synthetic complex fixture
# ---------------------------------------------------------------------------

def _make_complex():
    """Return a minimal lock complex dict with one chamber and one berth."""
    split_pt = Point(4.5, 52.5)
    merge_pt = Point(4.5, 52.4)
    door_start = Point(4.5, 52.48)
    door_end = Point(4.5, 52.42)

    return {
        "id": 1,
        "name": "Test Lock",
        "geometry": Point(4.5, 52.45).wkt,
        "fairway_id": 100,
        "fairway_name": "Test Fairway",
        "geometry_before_wkt": LineString([(4.5, 52.6), (4.5, 52.5)]).wkt,
        "geometry_after_wkt": LineString([(4.5, 52.4), (4.5, 52.3)]).wkt,
        "start_junction_id": 10,
        "end_junction_id": 20,
        "sections": [{"id": 300}],
        "berths": [
            {
                "id": 99,
                "name": "Test Berth",
                "geometry": Point(4.5, 52.6).wkt,
                "dist_m": 123.4,
                "relation": "before",
            }
        ],
        "locks": [
            {
                "id": 1,
                "name": "Test Lock",
                "chambers": [
                    {
                        "id": 55,
                        "name": "Chamber A",
                        "geometry": Polygon(
                            [(4.49, 52.43), (4.51, 52.43), (4.51, 52.47), (4.49, 52.47)]
                        ).wkt,
                        "length": 200.0,
                        "width": 24.0,
                        "route_geometry": None,
                        "subchambers": [
                            {
                                "id": 77,
                                "name": "Subchamber 1",
                                "geometry": Polygon(
                                    [(4.495, 52.44), (4.505, 52.44), (4.505, 52.46), (4.495, 52.46)]
                                ).wkt,
                                "length": 100.0,
                            }
                        ]
                    }
                ],
            }
        ],
    }


COMPLEXES = [_make_complex()]


# ---------------------------------------------------------------------------
# build_nodes_gdf
# ---------------------------------------------------------------------------

def test_nodes_gdf_geometry_type():
    gdf = build_nodes_gdf(COMPLEXES)
    assert not gdf.empty
    assert all(g.geom_type == "Point" for g in gdf.geometry)


def test_nodes_gdf_columns():
    gdf = build_nodes_gdf(COMPLEXES)
    for col in ["id", "node_type", "lock_id", "chamber_id"]:
        assert col in gdf.columns, f"Missing column: {col}"


def test_nodes_gdf_has_split_and_merge():
    gdf = build_nodes_gdf(COMPLEXES)
    node_types = set(gdf["node_type"].dropna())
    assert "lock_split" in node_types
    assert "lock_merge" in node_types


def test_nodes_gdf_crs():
    gdf = build_nodes_gdf(COMPLEXES)
    assert gdf.crs.to_epsg() == 4326


# ---------------------------------------------------------------------------
# build_edges_gdf
# ---------------------------------------------------------------------------

def test_edges_gdf_geometry_type():
    gdf = build_edges_gdf(COMPLEXES)
    assert not gdf.empty
    assert all(g.geom_type == "LineString" for g in gdf.geometry)


def test_edges_gdf_columns():
    gdf = build_edges_gdf(COMPLEXES)
    for col in ["id", "segment_type", "lock_id", "source_node", "target_node", "length_m"]:
        assert col in gdf.columns, f"Missing column: {col}"


def test_edges_gdf_has_before_and_after():
    gdf = build_edges_gdf(COMPLEXES)
    seg_types = set(gdf["segment_type"].dropna())
    assert "before" in seg_types
    assert "after" in seg_types


def test_edges_gdf_length_nonnegative():
    gdf = build_edges_gdf(COMPLEXES)
    assert (gdf["length_m"] >= 0).all()


# ---------------------------------------------------------------------------
# build_berths_gdf
# ---------------------------------------------------------------------------

def test_berths_gdf_geometry_type():
    gdf = build_berths_gdf(COMPLEXES)
    assert not gdf.empty
    assert all(g.geom_type == "Point" for g in gdf.geometry)


def test_berths_gdf_columns():
    gdf = build_berths_gdf(COMPLEXES)
    for col in ["id", "name", "lock_id", "dist_m", "relation"]:
        assert col in gdf.columns, f"Missing column: {col}"


def test_berths_gdf_values():
    gdf = build_berths_gdf(COMPLEXES)
    assert gdf.iloc[0]["id"] == 99
    assert gdf.iloc[0]["lock_id"] == 1
    assert gdf.iloc[0]["relation"] == "before"


# ---------------------------------------------------------------------------
# build_locks_gdf
# ---------------------------------------------------------------------------

def test_locks_gdf_geometry_type():
    gdf = build_locks_gdf(COMPLEXES)
    assert not gdf.empty
    assert all(g.geom_type == "Point" for g in gdf.geometry)  # Wait, lock geometry in fixture is Point

def test_locks_gdf_columns():
    gdf = build_locks_gdf(COMPLEXES)
    assert "isrs_code" not in gdf.columns # Not in fixture
    assert "fairway_name" in gdf.columns
    assert "geometry" in gdf.columns

# ---------------------------------------------------------------------------
# build_chambers_gdf
# ---------------------------------------------------------------------------

def test_chambers_gdf_geometry_type():
    gdf = build_chambers_gdf(COMPLEXES)
    assert not gdf.empty
    assert all(g.geom_type == "Polygon" for g in gdf.geometry)

def test_chambers_gdf_columns():
    gdf = build_chambers_gdf(COMPLEXES)
    for col in ["id", "name", "lock_id", "length", "width", "geometry"]:
        assert col in gdf.columns, f"Missing column: {col}"

# ---------------------------------------------------------------------------
# build_subchambers_gdf
# ---------------------------------------------------------------------------

def test_subchambers_gdf_geometry_type():
    gdf = build_subchambers_gdf(COMPLEXES)
    assert not gdf.empty
    assert all(g.geom_type == "Polygon" for g in gdf.geometry)

def test_subchambers_gdf_columns():
    gdf = build_subchambers_gdf(COMPLEXES)
    for col in ["id", "name", "lock_id", "chamber_id", "length", "geometry"]:
        assert col in gdf.columns, f"Missing column: {col}"

def test_subchambers_gdf_values():
    gdf = build_subchambers_gdf(COMPLEXES)
    assert gdf.iloc[0]["id"] == 77
    assert gdf.iloc[0]["chamber_id"] == 55
    assert gdf.iloc[0]["lock_id"] == 1


# ---------------------------------------------------------------------------
# Empty input
# ---------------------------------------------------------------------------

def test_empty_complexes():
    assert build_nodes_gdf([]).empty
    assert build_edges_gdf([]).empty
    assert build_berths_gdf([]).empty
    assert build_locks_gdf([]).empty
    assert build_chambers_gdf([]).empty
    assert build_subchambers_gdf([]).empty
