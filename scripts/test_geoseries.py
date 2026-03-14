import geopandas as gpd
from shapely.geometry import Point
import pandas as pd

try:
    s = gpd.GeoSeries([Point(0, 0)])
    print("Testing GeoSeries of objects...")
    res = gpd.GeoSeries.from_wkt(s)
    print("Success:", res)
except Exception as e:
    print("Failed with objects:", e)

try:
    s = pd.Series(["POINT (0 0)"])
    print("\nTesting Series of WKT strings...")
    res = gpd.GeoSeries.from_wkt(s)
    print("Success:", res)
except Exception as e:
    print("Failed with WKT:", e)
