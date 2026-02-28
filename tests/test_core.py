import pytest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from fis.lock.core import find_nearby_berths

def test_find_nearby_berths_distance():
    # Construct mock data for a lock and two berths
    # We will position them purely by RouteKmBegin to test the distance inclusion
    
    lock_row = pd.Series({
        "Id": 42863,
        "Name": "Volkeraksluizen",
        "RouteKmBegin": 0.5,
        "FairwayId": 28354,
        "geometry": Point(4.4, 51.7)
    })
    
    # Berth 1 is ~2.6km away (e.g. ID 48871)
    # Berth 2 is ~6.0km away (should be excluded)
    berths_data = [
        {"Id": 48871, "Name": "Wachtplaats 2 Volkeraksluizen - Zuid", "RouteKmBegin": 3.1, "FairwayId": 28354, "geometry": Point(4.4, 51.68)},
        {"Id": 99999, "Name": "Far away berth", "RouteKmBegin": 6.5, "FairwayId": 28354, "geometry": Point(4.4, 51.65)},
    ]
    berths_gdf = gpd.GeoDataFrame(berths_data, geometry="geometry")
    
    # We pass None for fairway geoms as we are checking the KM distance logic primarily
    nearby = find_nearby_berths(lock_row, berths_gdf, None, None, max_dist_m=5000)
    
    # Should include 48871 (dist ~2225m < 5000m) but exclude 99999 (dist ~5500m > 5000m)
    assert len(nearby) == 1
    assert nearby[0]["id"] == 48871
    assert nearby[0]["dist_m"] == pytest.approx(2225.1, rel=0.01)
    
def test_find_nearby_berths_wrong_fairway():
    lock_row = pd.Series({"Id": 42863, "RouteKmBegin": 0.5, "FairwayId": 28354})
    berths_gdf = gpd.GeoDataFrame([
        {"Id": 123, "RouteKmBegin": 0.6, "FairwayId": 99999, "geometry": Point(0,0)}
    ])
    
    nearby = find_nearby_berths(lock_row, berths_gdf, None, None)
    assert len(nearby) == 0
