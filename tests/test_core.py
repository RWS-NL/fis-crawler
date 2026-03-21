import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
from fis.utils import find_nearby_berths
from fis.lock.core import match_disk_objects, sanitize_attrs
from fis import settings


def test_find_nearby_berths_distance():
    # Construct mock data for a lock and two berths
    # We will position them purely by RouteKmBegin to test the distance inclusion

    lock_row = pd.Series(
        {
            "id": 42863,
            "name": "Volkeraksluizen",
            "route_km": 0.5,
            "fairway_id": 28354,
            "geometry": Point(4.4, 51.7),
        }
    )

    # Berth 1 is ~550m away (e.g. ID 48871)
    # Berth 2 is ~6.0km away (should be excluded)
    berths_data = [
        {
            "id": 48871,
            "name": "Wachtplaats 2 Volkeraksluizen - Zuid",
            "route_km": 3.1,
            "fairway_id": 28354,
            "geometry": Point(4.4, 51.695),
        },
        {
            "id": 99999,
            "name": "Far away berth",
            "route_km": 6.5,
            "fairway_id": 28354,
            "geometry": Point(4.4, 51.65),
        },
    ]
    berths_gdf = gpd.GeoDataFrame(berths_data, geometry="geometry", crs="EPSG:4326")

    # We pass None for fairway geoms as we are checking the KM distance logic primarily
    nearby = find_nearby_berths(lock_row, berths_gdf, None, None, max_dist_m=2000)

    # Should include 48871 (dist ~550m < 2000m) but exclude 99999 (dist > 2000m)
    assert len(nearby) == 1
    assert nearby[0]["id"] == 48871
    assert nearby[0]["dist_m"] == pytest.approx(556.3, rel=0.01)


def test_find_nearby_berths_wrong_fairway():
    lock_row = pd.Series({"id": 42863, "route_km": 0.5, "fairway_id": 28354})
    berths_gdf = gpd.GeoDataFrame(
        [{"id": 123, "route_km": 0.6, "fairway_id": 99999, "geometry": Point(0, 0)}],
        crs="EPSG:4326",
    )

    nearby = find_nearby_berths(
        lock_row, berths_gdf, None, None, allowed_fairways=[28354]
    )
    assert len(nearby) == 0


def test_find_nearby_berths_category_filter():
    lock_row = pd.Series(
        {
            "id": 15185,
            "name": "Sluis Hengelo",
            "route_km": 45.1,
            "fairway_id": 51569,
            "geometry": Point(6.804, 52.246),
        }
    )

    # Berth 100 is WAITING_AREA (Should be included)
    # Berth 200 is LOADING_AND_UNLOADING (Should be excluded)
    berths_data = [
        {
            "id": 100,
            "name": "Good Berth",
            "route_km": 44.5,
            "fairway_id": 51569,
            "category": "WAITING_AREA",
            "geometry": Point(6.805, 52.247),
        },
        {
            "id": 200,
            "name": "Ligplaats CTT Hengelo",
            "route_km": 43.7,
            "fairway_id": 51569,
            "category": "LOADING_AND_UNLOADING",
            "geometry": Point(6.786, 52.248),
        },
        {
            "id": 300,
            "name": "Unknown Berth",
            "route_km": 44.0,
            "fairway_id": 51569,
            "category": None,
            "geometry": Point(6.801, 52.246),
        },
    ]
    berths_gdf = gpd.GeoDataFrame(berths_data, geometry="geometry", crs="EPSG:4326")

    nearby = find_nearby_berths(
        lock_row, berths_gdf, None, None, allowed_categories=["WAITING_AREA"]
    )

    # Should include 100 and 300 (None category) but exclude 200
    ids = [n["id"] for n in nearby]
    assert 100 in ids
    assert 300 in ids
    assert 200 not in ids


def test_find_nearby_berths_geometric_position():
    # Setup lock with fairway geoms
    lock_geom = Point(5.0, 52.0)
    lock_row = pd.Series(
        {"id": 1, "route_km": 10.0, "fairway_id": 100, "geometry": lock_geom}
    )

    # Fairway segment before: (4.9, 52.0) -> (5.0, 52.0)
    fw_before = LineString([(4.9, 52.0), (5.0, 52.0)])
    # Fairway segment after: (5.0, 52.0) -> (5.1, 52.0)
    fw_after = LineString([(5.0, 52.0), (5.1, 52.0)])

    berths_data = [
        {
            "id": 10,
            "route_km": 9.9,
            "fairway_id": 100,
            "geometry": Point(4.95, 52.001),
        },  # BEFORE
        {
            "id": 20,
            "route_km": 10.1,
            "fairway_id": 100,
            "geometry": Point(5.05, 52.001),
        },  # AFTER
    ]
    berths_gdf = gpd.GeoDataFrame(berths_data, geometry="geometry", crs="EPSG:4326")

    # Increase max_dist_m for testing with small degree changes
    nearby = find_nearby_berths(
        lock_row, berths_gdf, fw_before, fw_after, max_dist_m=5000
    )

    assert len(nearby) == 2
    mapping = {n["id"]: n["relation"] for n in nearby}
    assert mapping[10] == "before"
    assert mapping[20] == "after"


def test_sanitize_attrs():
    row = pd.Series({"id": 1, "name": "Test", "geometry": Point(0, 0), "extra": 42})
    sanitized = sanitize_attrs(row)

    assert sanitized["id"] == 1
    assert sanitized["name"] == "Test"
    assert sanitized["extra"] == 42
    assert "geometry" in sanitized
    assert isinstance(sanitized["geometry"], str)


def test_match_disk_objects_spatial(monkeypatch):
    # Set a large buffer for testing
    monkeypatch.setattr(settings, "DISK_MATCH_BUFFER_LOCK_M", 1000.0)

    # Setup mock FIS lock and chambers
    lock_geom = Point(4.8, 52.3)
    lock = pd.Series({"id": 1, "geometry": lock_geom})

    # Chamber geometry
    chamber_geom = Point(4.801, 52.301)
    chambers = gpd.GeoDataFrame(
        [{"id": 101, "geometry": chamber_geom}], geometry="geometry", crs="EPSG:4326"
    )

    # Setup mock DISK locks and bridges
    # Project them to RD New as expected by match_disk_objects
    disk_locks = gpd.GeoDataFrame(
        [{"id": "disk_l1", "geometry": Point(4.8015, 52.3015)}],
        geometry="geometry",
        crs="EPSG:4326",
    ).to_crs("EPSG:28992")

    disk_bridges = gpd.GeoDataFrame(
        [{"id": "disk_b1", "geometry": Point(4.805, 52.305)}],
        geometry="geometry",
        crs="EPSG:4326",
    ).to_crs("EPSG:28992")

    matched_locks, matched_bridges = match_disk_objects(
        lock, chambers, disk_locks, disk_bridges
    )

    assert len(matched_locks) == 1
    assert matched_locks[0]["id"] == "disk_l1"
    assert len(matched_bridges) == 1
    assert matched_bridges[0]["id"] == "disk_b1"
