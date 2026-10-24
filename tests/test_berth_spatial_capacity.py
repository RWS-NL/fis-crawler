import unittest
import geopandas as gpd
import networkx as nx
from shapely.geometry import LineString, Point
from scripts.berth_analysis.route_network_graph import (
    extract_segment_geometry,
    add_or_update_node,
    build_route_network_graph,
)
from scripts.berth_analysis.analyze_spatial_capacity import (
    calc_segment_compliant_length,
    calc_segment_furthest_distance,
    classify_and_calculate_capacity,
    classify_berths,
    calculate_berth_capacities,
    determine_kegels,
    determine_capacity_and_length,
    build_network_classification_map,
    compute_route_carrying_capacity,
    compute_route_capacity_and_intensity,
    run_spatial_distance_analysis,
    DAILY_SAILING_DISTANCE_KM,
    format_km,
    format_pct,
    format_int,
    format_draagkracht,
    format_dist,
)


class TestBerthSpatialCapacity(unittest.TestCase):
    def test_determine_kegels(self):
        self.assertEqual(determine_kegels("TWO_BLUE_LIGHTS", "Ligplaats"), 2)
        self.assertEqual(determine_kegels(None, "Ligplaats 2-kegel"), 2)
        self.assertEqual(determine_kegels(None, "twee kegels ligplaats"), 2)
        self.assertEqual(determine_kegels("ONE_BLUE_LIGHT", "Ligplaats"), 1)
        self.assertEqual(determine_kegels(None, "1-kegel kade"), 1)
        self.assertEqual(determine_kegels(None, "kegelligplaats algemeen"), 1)
        self.assertEqual(determine_kegels(None, "Reguliere kade", is_kegel=True), 1)
        self.assertEqual(determine_kegels("NO_BLUE_LIGHT", "Reguliere kade"), 0)

    def test_determine_capacity_and_length(self):
        cap, eff_len = determine_capacity_and_length(150.5, 2, None)
        self.assertEqual(eff_len, 301.0)
        self.assertEqual(cap, 2.0)

        cap, eff_len = determine_capacity_and_length(200.0, 1, 5)
        self.assertEqual(eff_len, 200.0)
        self.assertEqual(cap, 5.0)

        cap, eff_len = determine_capacity_and_length(None, 1, None)
        self.assertEqual(eff_len, 0.0)
        self.assertEqual(cap, 1.0)

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
                    "Name": "2-kegel ligplaats B",
                    "Facility": ["GANGWAY"],
                    "Category": "OTHER",
                    "MaximumStayDurationH": 24.0,
                    "Signalling": None,
                    "Length": 120.0,
                    "NumberOfRows": 2,
                    "NumberOfMooringFacilities": None,
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
                {
                    "Id": 4,
                    "Name": "Ligplaats Duwvaartsluis Krammer",
                    "Facility": [],
                    "Category": "ALL_VESSELS",
                    "MaximumStayDurationH": None,
                    "Signalling": None,
                    "Length": 450.0,
                    "NumberOfRows": 1,
                    "NumberOfMooringFacilities": None,
                    "geometry": Point(4.93, 52.13),
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

        classified = classify_berths(berths, cardropoffs)
        self.assertTrue(classified.loc[classified["Id"] == 1, "is_overnight"].iloc[0])
        self.assertTrue(classified.loc[classified["Id"] == 1, "has_cardropoff"].iloc[0])
        self.assertTrue(classified.loc[classified["Id"] == 2, "is_kegel"].iloc[0])
        self.assertEqual(
            classified.loc[classified["Id"] == 2, "kegel_count"].iloc[0], 2
        )

        capacities = calculate_berth_capacities(classified)
        self.assertEqual(
            capacities.loc[capacities["Id"] == 1, "capacity_ships"].iloc[0], 1.0
        )
        self.assertEqual(
            capacities.loc[capacities["Id"] == 1, "effective_length_m"].iloc[0], 240.0
        )

        df = classify_and_calculate_capacity(berths, cardropoffs)

        self.assertTrue(df.loc[df["Id"] == 1, "is_overnight"].iloc[0])
        self.assertTrue(df.loc[df["Id"] == 1, "has_cardropoff"].iloc[0])
        self.assertEqual(df.loc[df["Id"] == 1, "capacity_ships"].iloc[0], 1.0)
        self.assertEqual(df.loc[df["Id"] == 1, "effective_length_m"].iloc[0], 240.0)
        self.assertFalse(df.loc[df["Id"] == 1, "is_kegel"].iloc[0])

        self.assertTrue(df.loc[df["Id"] == 2, "is_kegel"].iloc[0])
        self.assertEqual(df.loc[df["Id"] == 2, "kegel_count"].iloc[0], 2)
        self.assertEqual(df.loc[df["Id"] == 2, "capacity_ships"].iloc[0], 2.0)
        self.assertEqual(df.loc[df["Id"] == 2, "effective_length_m"].iloc[0], 240.0)

        self.assertFalse(df.loc[df["Id"] == 3, "is_overnight"].iloc[0])

        self.assertTrue(df.loc[df["Id"] == 4, "is_overnight"].iloc[0])
        self.assertEqual(df.loc[df["Id"] == 4, "effective_length_m"].iloc[0], 450.0)

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
        self.assertEqual(tmap.get(99, "Overig"), "Overig")

    def test_furthest_distance_and_continuous_coverage(self):
        peak_dist = calc_segment_furthest_distance(5.0, 10.0, 10.0)
        self.assertEqual(peak_dist, 12.5)

        dominated_dist = calc_segment_furthest_distance(2.0, 15.0, 10.0)
        self.assertEqual(dominated_dist, 15.0)

        covered_len = calc_segment_compliant_length(5.0, 12.0, 20.0, 15.0)
        self.assertEqual(covered_len, 13.0)

        full_covered = calc_segment_compliant_length(5.0, 5.0, 10.0, 15.0)
        self.assertEqual(full_covered, 10.0)

    def test_spatial_distance_analysis_ajk_chain(self):
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

        seg_aj = gdf.iloc[0]
        self.assertTrue(seg_aj["is_hvwn"])
        self.assertEqual(seg_aj["dist_to_overnight_km"], 20.0)
        self.assertEqual(seg_aj["compliant_30km_overnight_km"], 15.0)
        self.assertFalse(seg_aj["is_fully_compliant_overnight"])

        seg_jk = gdf.iloc[1]
        self.assertEqual(seg_jk["dist_to_overnight_km"], 40.0)
        self.assertEqual(seg_jk["compliant_30km_overnight_km"], 0.0)
        self.assertFalse(seg_jk["is_fully_compliant_overnight"])

    def test_compute_route_carrying_capacity(self):
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

        # Verify alias
        self.assertIs(
            compute_route_capacity_and_intensity, compute_route_carrying_capacity
        )
        gdf_routes = compute_route_carrying_capacity(routes, berths, gdf_segments)

        self.assertEqual(len(gdf_routes), 1)
        r = gdf_routes.iloc[0]
        self.assertTrue(r["is_hvwn"])

        # Expected overnight stops = 20 / 168 (unrounded during computation)
        expected_p_stop = 20.0 / DAILY_SAILING_DISTANCE_KM
        self.assertAlmostEqual(r["expected_overnight_stops"], expected_p_stop, places=6)
        self.assertAlmostEqual(r["p_overnight_stop"], expected_p_stop, places=6)

        # Overnight length & capacity
        self.assertEqual(r["overnight_berth_length_m"], 240.0)
        self.assertEqual(r["overnight_meter_nights_yr"], 240.0 * 365.0)

        # Max carrying capacity overnight: (240 / 1000) * (168 / 20) * 365 = 735.84 km/yr (unrounded during computation)
        expected_max_cap = (240.0 / 1000.0) * (168.0 / 20.0) * 365.0
        self.assertAlmostEqual(
            r["max_capacity_ship_length_km_yr"], expected_max_cap, places=4
        )

        # Kegel length & capacity
        self.assertEqual(r["kegel_berth_length_m"], 120.0)
        self.assertEqual(r["kegel_meter_nights_yr"], 120.0 * 365.0)
        expected_max_kegel = (120.0 / 1000.0) * (168.0 / 20.0) * 365.0
        self.assertAlmostEqual(
            r["max_kegel_ship_length_km_yr"], expected_max_kegel, places=4
        )

        # Coverage percentages: 15 / 20 * 100 = 75%
        self.assertEqual(r["overnight_coverage_pct"], 75.0)
        self.assertEqual(r["kegel_coverage_pct"], 100.0)

    def test_formatting_helpers(self):
        self.assertEqual(format_km(12.345), "12.3 km")
        self.assertEqual(format_km(None), "-")
        self.assertEqual(format_pct(75.56), "75.6%")
        self.assertEqual(format_int(1234.6), "1,235")
        self.assertEqual(format_draagkracht(735.84), "735.8 km")
        self.assertEqual(format_draagkracht(0.0), "-")
        self.assertEqual(format_dist(14.82), "14.8 km")
        self.assertEqual(format_dist(float("inf")), "Geen ligplaats bereikbaar")

    def test_extract_segment_geometry(self):
        line = LineString([(0, 0), (0, 20)])

        # Increasing kilometrage: 5km to 15km on 0-20km line
        sub = extract_segment_geometry(line, 5.0, 15.0, 0.0, 20.0)
        self.assertIsNotNone(sub)
        self.assertAlmostEqual(sub.length, 10.0)

        # Decreasing kilometrage: 15km to 5km on 20-0km line
        sub_rev = extract_segment_geometry(line, 5.0, 15.0, 20.0, 0.0)
        self.assertIsNotNone(sub_rev)
        self.assertAlmostEqual(sub_rev.length, 10.0)

        # Degenerate span returns None
        self.assertIsNone(extract_segment_geometry(line, 5.0, 5.0, 0.0, 20.0))
        self.assertIsNone(extract_segment_geometry(None, 0.0, 10.0, 0.0, 20.0))

    def test_add_or_update_node(self):
        G = nx.Graph()
        add_or_update_node(G, "node_1", "junction", 10.0, 100, {"is_overnight": False})
        self.assertIn("node_1", G)
        self.assertFalse(G.nodes["node_1"]["is_overnight"])

        # Update node with overnight berth flag
        add_or_update_node(G, "node_1", "berth", 10.0, 100, {"is_overnight": True})
        self.assertTrue(G.nodes["node_1"]["is_overnight"])

    def test_build_route_network_graph_synthetic(self):
        routes = gpd.GeoDataFrame(
            [
                {
                    "Id": 100,
                    "Name": "Test Fairway",
                    "Code": "TF1",
                    "RouteKmBegin": 0.0,
                    "RouteKmEnd": 30.0,
                    "geometry": LineString([(0, 0), (0, 30)]),
                }
            ],
            crs="EPSG:4326",
        )
        rjs = gpd.GeoDataFrame(
            [
                {
                    "Id": 1,
                    "RouteId": 100,
                    "SectionJunctionId": 501,
                    "RouteKmBegin": 10.0,
                    "geometry": Point(0, 10),
                }
            ],
            crs="EPSG:4326",
        )
        berths = gpd.GeoDataFrame(
            [
                {
                    "Id": 10,
                    "RouteId": 100,
                    "Name": "Berth 10",
                    "RouteKmBegin": 20.0,
                    "is_overnight": True,
                    "is_kegel": False,
                    "capacity_ships": 2.0,
                    "effective_length_m": 240.0,
                    "has_cardropoff": True,
                    "geometry": Point(0, 20),
                }
            ],
            crs="EPSG:4326",
        )
        type_map = {100: "Hoofdtransportas"}

        G, segments = build_route_network_graph(routes, rjs, berths, type_map)
        self.assertEqual(G.number_of_nodes(), 4)  # start, rj_501, berth_10, end
        self.assertEqual(len(segments), 3)
        self.assertTrue(G.nodes["berth_10"]["is_overnight"])
        self.assertEqual(segments[0]["source"], "start_100")
        self.assertEqual(segments[0]["target"], "rj_501")
        self.assertEqual(segments[0]["length_km"], 10.0)

    def test_empty_sources_dijkstra_fails_fast(self):
        G = nx.Graph()
        G.add_node("node_A", is_overnight=False, is_kegel=False)
        route_segments = [{"source": "node_A", "target": "node_A", "length_km": 1.0}]
        with self.assertRaises(ValueError):
            run_spatial_distance_analysis(G, route_segments)


if __name__ == "__main__":
    unittest.main()
