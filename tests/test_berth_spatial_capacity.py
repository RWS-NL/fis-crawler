import unittest
import geopandas as gpd
from shapely.geometry import Point
from scripts.berth_analysis.analyze_spatial_capacity import (
    classify_and_calculate_capacity,
    build_network_classification_map,
)


class TestBerthSpatialCapacity(unittest.TestCase):
    def test_classify_and_calculate_capacity(self):
        berths = gpd.GeoDataFrame(
            [
                {
                    "Id": 1,
                    "Name": "Overnachtingslocatie A",
                    "Facility": ["OVERNIGHT_ACCOMODATION", "GANGWAY"],
                    "Category": "OTHER",
                    "MaximumStayDurationH": 72.0,
                    "Signalling": "NO_BLUE_LIGHT",
                    "Length": 240.0,
                    "NumberOfRows": 1,
                    "NumberOfMooringFacilities": None,
                    "geometry": Point(4.9, 52.1),
                },
                {
                    "Id": 2,
                    "Name": "Kegelligplaats B",
                    "Facility": ["GANGWAY"],
                    "Category": "OTHER",
                    "MaximumStayDurationH": 24.0,
                    "Signalling": "TWO_BLUE_LIGHTS",
                    "Length": 120.0,
                    "NumberOfRows": 1,
                    "NumberOfMooringFacilities": 2.0,
                    "geometry": Point(4.91, 52.11),
                },
                {
                    "Id": 3,
                    "Name": "Wachtplaats C",
                    "Facility": ["OVERNIGHT_ACCOMODATION"],
                    "Category": "WAITING_AREA",
                    "MaximumStayDurationH": 2.0,
                    "Signalling": None,
                    "Length": 100.0,
                    "NumberOfRows": 1,
                    "NumberOfMooringFacilities": None,
                    "geometry": Point(4.92, 52.12),
                },
            ],
            crs="EPSG:4326",
        )

        cardropoffs = gpd.GeoDataFrame(
            [
                {
                    "Id": 101,
                    "BerthId": 1,
                    "Name": "Autoafzetplaats A",
                    "geometry": Point(4.9, 52.1),
                }
            ],
            crs="EPSG:4326",
        )

        df = classify_and_calculate_capacity(berths, cardropoffs)

        # Berth 1: overnight, has cardropoff, capacity = 240 / 120 = 2 ships
        self.assertTrue(df.loc[df["Id"] == 1, "is_overnight"].iloc[0])
        self.assertTrue(df.loc[df["Id"] == 1, "has_cardropoff"].iloc[0])
        self.assertEqual(df.loc[df["Id"] == 1, "capacity_ships"].iloc[0], 2)
        self.assertFalse(df.loc[df["Id"] == 1, "is_kegel"].iloc[0])

        # Berth 2: kegel (2 blue lights), explicit capacity = 2 ships
        self.assertTrue(df.loc[df["Id"] == 2, "is_kegel"].iloc[0])
        self.assertEqual(df.loc[df["Id"] == 2, "kegel_count"].iloc[0], 2)
        self.assertEqual(df.loc[df["Id"] == 2, "capacity_ships"].iloc[0], 2)

        # Berth 3: WAITING_AREA should not be classified as overnight
        self.assertFalse(df.loc[df["Id"] == 3, "is_overnight"].iloc[0])

    def test_build_network_classification_map(self):
        fc = gpd.GeoDataFrame(
            [
                {"RouteId": 10, "TypeDescription": "Hoofdtransportas"},
                {"RouteId": 20, "TypeDescription": "Hoofdvaarweg"},
                {"RouteId": 30, "TypeDescription": "Overige vaarweg"},
            ]
        )
        tmap = build_network_classification_map(fc)
        self.assertEqual(tmap[10], "Hoofdtransportas")
        self.assertEqual(tmap[20], "Hoofdvaarweg")
        self.assertEqual(tmap[30], "Overige vaarweg")
        self.assertNotIn(40, tmap)


if __name__ == "__main__":
    unittest.main()
