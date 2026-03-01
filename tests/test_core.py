import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from fis.lock.core import find_nearby_berths


def test_find_nearby_berths_distance():
    # Construct mock data for a lock and two berths
    # We will position them purely by RouteKmBegin to test the distance inclusion

    lock_row = pd.Series(
        {
            "Id": 42863,
            "Name": "Volkeraksluizen",
            "RouteKmBegin": 0.5,
            "FairwayId": 28354,
            "geometry": Point(4.4, 51.7),
        }
    )

    # Berth 1 is ~550m away (e.g. ID 48871)
    # Berth 2 is ~6.0km away (should be excluded)
    berths_data = [
        {
            "Id": 48871,
            "Name": "Wachtplaats 2 Volkeraksluizen - Zuid",
            "RouteKmBegin": 3.1,
            "FairwayId": 28354,
            "geometry": Point(4.4, 51.695),
        },
        {
            "Id": 99999,
            "Name": "Far away berth",
            "RouteKmBegin": 6.5,
            "FairwayId": 28354,
            "geometry": Point(4.4, 51.65),
        },
    ]
    berths_gdf = gpd.GeoDataFrame(berths_data, geometry="geometry")

    # We pass None for fairway geoms as we are checking the KM distance logic primarily
    nearby = find_nearby_berths(lock_row, berths_gdf, None, None, max_dist_m=2000)

    # Should include 48871 (dist ~550m < 2000m) but exclude 99999 (dist > 2000m)
    assert len(nearby) == 1
    assert nearby[0]["id"] == 48871
    assert nearby[0]["dist_m"] == pytest.approx(556.3, rel=0.01)


def test_find_nearby_berths_wrong_fairway():
    lock_row = pd.Series({"Id": 42863, "RouteKmBegin": 0.5, "FairwayId": 28354})
    berths_gdf = gpd.GeoDataFrame(
        [{"Id": 123, "RouteKmBegin": 0.6, "FairwayId": 99999, "geometry": Point(0, 0)}]
    )

    nearby = find_nearby_berths(lock_row, berths_gdf, None, None)
    assert len(nearby) == 0


def test_find_nearby_berths_category_filter():
    lock_row = pd.Series(
        {
            "Id": 15185,
            "Name": "Sluis Hengelo",
            "RouteKmBegin": 45.1,
            "FairwayId": 51569,
            "geometry": Point(6.804, 52.246),
        }
    )

    # Berth 100 is WAITING_AREA (Should be included)
    # Berth 200 is LOADING_AND_UNLOADING (Should be excluded)
    berths_data = [
        {
            "Id": 100,
            "Name": "Good Berth",
            "RouteKmBegin": 44.5,
            "FairwayId": 51569,
            "Category": "WAITING_AREA",
            "geometry": Point(6.805, 52.247),
        },
        {
            "Id": 200,
            "Name": "Ligplaats CTT Hengelo",
            "RouteKmBegin": 43.7,
            "FairwayId": 51569,
            "Category": "LOADING_AND_UNLOADING",
            "geometry": Point(6.786, 52.248),
        },
        {
            "Id": 300,
            "Name": "Unknown Berth",
            "RouteKmBegin": 44.0,
            "FairwayId": 51569,
            "Category": None,
            "geometry": Point(6.801, 52.246),
        },
    ]
    berths_gdf = gpd.GeoDataFrame(berths_data, geometry="geometry")

    # Default call should implicitly use allowed_categories=["WAITING_AREA"] and include NaNs
    nearby = find_nearby_berths(lock_row, berths_gdf, None, None, max_dist_m=5000)

    # We expect 2 nearby berths (ID 100, ID 300)
    assert len(nearby) == 2
    ids = [b["id"] for b in nearby]
    assert 100 in ids
    assert 300 in ids
    assert 200 not in ids


def test_find_nearby_berths_cross_fairway():
    lock_row = pd.Series(
        {
            "Id": 51064,
            "Name": "Zuidersluis IJmuiden",
            "RouteKmBegin": 10.0,
            "FairwayId": 41686,
            "geometry": Point(0, 0),
        }
    )

    # Berth 100 sits exactly on the lock but on a different Fairway ID
    berths_data = [
        {
            "Id": 100,
            "Name": "Wachtplaats Zuidersluis",
            "RouteKmBegin": 10.0,
            "FairwayId": 17140,
            "Category": "WAITING_AREA",
            "geometry": Point(0.001, 0),
        },
    ]
    berths_gdf = gpd.GeoDataFrame(berths_data, geometry="geometry")

    nearby = find_nearby_berths(lock_row, berths_gdf, None, None, max_dist_m=5000)

    # We expect 1 nearby berth despite FairwayId not matching
    assert len(nearby) == 1
    assert nearby[0]["id"] == 100


def test_find_nearby_berths_relation():
    # Construct lock at origin
    lock_row = pd.Series(
        {
            "Id": 1,
            "Name": "Lock X",
            "RouteKmBegin": 5.0,
            "FairwayId": 100,
            "geometry": Point(0, 0),
        }
    )

    # Berth A is to the West (-X), Berth B is to the East (+X)
    berths_data = [
        {
            "Id": 10,
            "Name": "West Berth",
            "RouteKmBegin": 4.5,
            "FairwayId": 100,
            "geometry": Point(-0.01, 0),
        },
        {
            "Id": 20,
            "Name": "East Berth",
            "RouteKmBegin": 5.5,
            "FairwayId": 100,
            "geometry": Point(0.01, 0),
        },
    ]
    berths_gdf = gpd.GeoDataFrame(berths_data, geometry="geometry")

    # Mock the fairway geometries (before is West, after is East)
    # Give them WKT representations to mimic database strings
    from shapely.geometry import LineString

    geom_before_wkt = LineString([(-0.05, 0), (0, 0)]).wkt
    geom_after_wkt = LineString([(0, 0), (0.05, 0)]).wkt

    # Calculate nearby berths with relation checked
    nearby = find_nearby_berths(
        lock_row, berths_gdf, geom_before_wkt, geom_after_wkt, max_dist_m=5000
    )

    # We expect 2 nearby berths
    assert len(nearby) == 2

    # Map output by ID to easily assert relation
    results = {b["id"]: b for b in nearby}

    # West Berth (ID 10) should be closer to geom_before ("before")
    assert results[10]["relation"] == "before"

    # East Berth (ID 20) should be closer to geom_after ("after")
    assert results[20]["relation"] == "after"
