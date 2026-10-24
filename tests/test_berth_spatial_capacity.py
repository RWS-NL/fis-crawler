import unittest
import geopandas as gpd
import networkx as nx
from shapely.geometry import LineString, Point
from scripts.berth_analysis.analyze_spatial_capacity import (
    calc_segment_compliant_length,
    calc_segment_furthest_distance,
    classify_and_calculate_capacity,
    build_network_classification_map,
    compute_route_capacity_and_intensity,
    run_spatial_distance_analysis,
    DAILY_SAILING_DISTANCE_KM,
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

        # Berth 1: overnight, has cardropoff, capacity = (240 + 10) // 120 = 2 ships
        self.assertTrue(df.loc[df["Id"] == 1, "is_overnight"].iloc[0])
        self.assertTrue(df.loc[df["Id"] == 1, "has_cardropoff"].iloc[0])
        self.assertEqual(df.loc[df["Id"] == 1, "capacity_ships"].iloc[0], 2)
        self.assertEqual(df.loc[df["Id"] == 1, "effective_length_m"].iloc[0], 240.0)
        self.assertFalse(df.loc[df["Id"] == 1, "is_kegel"].iloc[0])

        # Berth 2: kegel (2 blue lights), capacity = 1 ship (120m)
        self.assertTrue(df.loc[df["Id"] == 2, "is_kegel"].iloc[0])
        self.assertEqual(df.loc[df["Id"] == 2, "kegel_count"].iloc[0], 2)
        self.assertEqual(df.loc[df["Id"] == 2, "capacity_ships"].iloc[0], 1)
        self.assertEqual(df.loc[df["Id"] == 2, "effective_length_m"].iloc[0], 120.0)

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

    def test_calc_segment_distance_and_continuous_coverage(self):
        # A–J–K example from SiggyF:
        # Berth at A (d_A = 0). Segment A-J: length = 20 km, d_J = 20 km.
        # Furthest point on A-J: (0 + 20 + 20) / 2 = 20 km (not 10 km!)
        max_dist_aj = calc_segment_furthest_distance(0.0, 20.0, 20.0)
        self.assertEqual(max_dist_aj, 20.0)

        # Compliant length on A-J with R = 15 km:
        # reach_A = max(0, 15 - 0) = 15 km, reach_J = max(0, 15 - 20) = 0 km => 15 km
        comp_len_aj = calc_segment_compliant_length(0.0, 20.0, 20.0, 15.0)
        self.assertEqual(comp_len_aj, 15.0)

        # Segment J-K: length = 20 km, d_J = 20 km, d_K = 40 km.
        # Furthest point on J-K: (20 + 40 + 20) / 2 = 40 km.
        max_dist_jk = calc_segment_furthest_distance(20.0, 40.0, 20.0)
        self.assertEqual(max_dist_jk, 40.0)

        # Compliant length on J-K with R = 15 km: reach_J = 0, reach_K = 0 => 0 km
        comp_len_jk = calc_segment_compliant_length(20.0, 40.0, 20.0, 15.0)
        self.assertEqual(comp_len_jk, 0.0)

    def test_spatial_distance_analysis_ajk_chain(self):
        # Full graph Dijkstra test for A -20km- J -20km- K
        G = nx.Graph()
        G.add_node("node_A", is_overnight=True, is_kegel=True)
        G.add_node("node_J", is_overnight=False, is_kegel=False)
        G.add_node("node_K", is_overnight=False, is_kegel=False)
        G.add_edge("node_A", "node_J", weight=20.0)
        G.add_edge("node_J", "node_K", weight=20.0)

        route_segments = [
            {
                "source": "node_A",
                "target": "node_J",
                "route_id": 100,
                "route_name": "Test Route",
                "route_code": "TR1",
                "netwerk_type": "Hoofdtransportas",
                "km_from": 0.0,
                "km_to": 20.0,
                "length_km": 20.0,
                "geometry": LineString([(0, 0), (0, 0.2)]),
            },
            {
                "source": "node_J",
                "target": "node_K",
                "route_id": 100,
                "route_name": "Test Route",
                "route_code": "TR1",
                "netwerk_type": "Hoofdtransportas",
                "km_from": 20.0,
                "km_to": 40.0,
                "length_km": 20.0,
                "geometry": LineString([(0, 0.2), (0, 0.4)]),
            },
        ]

        gdf = run_spatial_distance_analysis(G, route_segments)

        # Segment A-J
        seg_aj = gdf.iloc[0]
        self.assertTrue(seg_aj["is_hvwn"])
        self.assertEqual(seg_aj["dist_to_overnight_km"], 20.0)
        self.assertEqual(seg_aj["compliant_30km_overnight_km"], 15.0)
        self.assertFalse(seg_aj["is_fully_compliant_overnight"])

        # Segment J-K
        seg_jk = gdf.iloc[1]
        self.assertEqual(seg_jk["dist_to_overnight_km"], 40.0)
        self.assertEqual(seg_jk["compliant_30km_overnight_km"], 0.0)
        self.assertFalse(seg_jk["is_fully_compliant_overnight"])

    def test_compute_route_capacity_and_intensity(self):
        routes = gpd.GeoDataFrame(
            [
                {
                    "Id": 100,
                    "Name": "Kanaal van Test",
                    "Code": "KVT",
                    "geometry": LineString([(0, 0), (0, 0.2)]),
                }
            ],
            crs="EPSG:4326",
        )

        berths = gpd.GeoDataFrame(
            [
                {
                    "Id": 1,
                    "RouteId": 100,
                    "Name": "Berth Regulier",
                    "is_overnight": True,
                    "is_kegel": False,
                    "effective_length_m": 240.0,
                    "capacity_ships": 2,
                    "has_cardropoff": True,
                },
                {
                    "Id": 2,
                    "RouteId": 100,
                    "Name": "Berth Kegel",
                    "is_overnight": False,
                    "is_kegel": True,
                    "effective_length_m": 120.0,
                    "capacity_ships": 1,
                    "has_cardropoff": False,
                },
            ]
        )

        gdf_segments = gpd.GeoDataFrame(
            [
                {
                    "route_id": 100,
                    "length_km": 20.0,
                    "compliant_30km_overnight_km": 15.0,
                    "dist_to_overnight_km": 20.0,
                    "compliant_60km_kegel_km": 20.0,
                    "dist_to_kegel_km": 20.0,
                    "netwerk_type": "Hoofdvaarweg",
                    "route_name": "Kanaal van Test",
                    "route_code": "KVT",
                    "geometry": LineString([(0, 0), (0, 0.2)]),
                }
            ],
            crs="EPSG:4326",
        )

        gdf_routes = compute_route_capacity_and_intensity(routes, berths, gdf_segments)

        self.assertEqual(len(gdf_routes), 1)
        r = gdf_routes.iloc[0]
        self.assertTrue(r["is_hvwn"])

        # Stop probability = 20 / 168
        expected_p_stop = round(20.0 / DAILY_SAILING_DISTANCE_KM, 4)
        self.assertEqual(r["p_overnight_stop"], expected_p_stop)

        # Overnight length & capacity
        self.assertEqual(r["overnight_berth_length_m"], 240.0)
        self.assertEqual(r["overnight_meter_nights_yr"], 240.0 * 365.0)

        # Max carrying capacity overnight: (240 / 1000) * (168 / 20) * 365 = 735.8 km/yr
        expected_max_cap = round((240.0 / 1000.0) * (168.0 / 20.0) * 365.0, 1)
        self.assertEqual(r["max_capacity_ship_length_km_yr"], expected_max_cap)

        # Kegel length & capacity
        self.assertEqual(r["kegel_berth_length_m"], 120.0)
        self.assertEqual(r["kegel_meter_nights_yr"], 120.0 * 365.0)
        expected_max_kegel = round((120.0 / 1000.0) * (168.0 / 20.0) * 365.0, 1)
        self.assertEqual(r["max_kegel_ship_length_km_yr"], expected_max_kegel)

        # Coverage percentages: 15 / 20 * 100 = 75%
        self.assertEqual(r["overnight_coverage_pct"], 75.0)
        self.assertEqual(r["kegel_coverage_pct"], 100.0)

    def test_empty_sources_dijkstra_fails_fast(self):
        G = nx.Graph()
        G.add_node("node_A", is_overnight=False, is_kegel=False)
        route_segments = [{"source": "node_A", "target": "node_A", "length_km": 1.0}]
        with self.assertRaises(ValueError):
            run_spatial_distance_analysis(G, route_segments)


if __name__ == "__main__":
    unittest.main()
