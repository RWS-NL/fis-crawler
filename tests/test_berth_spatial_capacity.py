import unittest
import geopandas as gpd
import networkx as nx
from shapely.geometry import LineString, MultiLineString, Point
from scripts.berth_analysis.route_network_graph import (
    extract_segment_geometry,
    build_route_network_graph,
)
from scripts.berth_analysis.analyze_spatial_capacity import (
    calc_berth_ship_slots,
    calc_segment_compliant_length,
    calc_segment_furthest_distance,
    classify_and_calculate_capacity,
    classify_berths,
    calculate_berth_capacities,
    determine_kegels,
    build_network_classification_map,
    compute_route_carrying_capacity,
    run_spatial_distance_analysis,
    DAILY_SAILING_DISTANCE_KM,
    fmt_num,
    fmt_pct,
    fmt_dist,
)


class TestBerthSpatialCapacity(unittest.TestCase):
    def test_determine_kegels(self):
        self.assertEqual(determine_kegels("TWO_BLUE_LIGHTS", "Ligplaats"), 2)
        self.assertEqual(determine_kegels(None, "Ligplaats 2-kegel"), 2)
        self.assertEqual(determine_kegels(None, "twee kegels ligplaats"), 2)
        self.assertEqual(determine_kegels("ONE_BLUE_LIGHT", "Ligplaats"), 1)
        self.assertEqual(determine_kegels(None, "1-kegel kade"), 1)
        self.assertEqual(determine_kegels(None, "kegelligplaats algemeen"), 1)
        self.assertEqual(determine_kegels(None, "Geen kegelligplaats"), 0)
        self.assertEqual(determine_kegels(None, "0-kegel kade"), 0)
        self.assertEqual(determine_kegels("NO_BLUE_LIGHT", "Reguliere kade"), 0)

    def test_calc_berth_ship_slots(self):
        import pandas as pd

        lengths = pd.Series([240.0, 120.0, 300.0, 0.0])
        rows = pd.Series([1.0, 2.0, 1.0, 1.0])
        max_len = pd.Series([float("nan"), float("nan"), 110.0, float("nan")])

        slots_110 = calc_berth_ship_slots(lengths, rows, max_len, 110.0)
        slots_135 = calc_berth_ship_slots(lengths, rows, max_len, 135.0)

        self.assertEqual(list(slots_110), [2.0, 2.0, 2.0, 0.0])
        self.assertEqual(list(slots_135), [1.0, 0.0, 0.0, 0.0])

    def test_classify_and_calculate_capacity(self):
        berths = gpd.GeoDataFrame(
            [
                {
                    "id": "1",
                    "name": "Overnachtingslocatie A",
                    "facility": ["OVERNIGHT_ACCOMODATION", "GANGWAY"],
                    "category": "OTHER",
                    "maximum_stay_duration_h": 72.0,
                    "signalling": "NO_BLUE_LIGHT",
                    "length": 240.0,
                    "number_of_rows": 1,
                    "maximal_ship_length": None,
                    "geometry": Point(4.9, 52.1),
                },
                {
                    "id": "2",
                    "name": "2-kegel ligplaats B",
                    "facility": ["GANGWAY"],
                    "category": "OTHER",
                    "maximum_stay_duration_h": 24.0,
                    "signalling": None,
                    "length": 120.0,
                    "number_of_rows": 2,
                    "maximal_ship_length": None,
                    "geometry": Point(4.91, 52.11),
                },
                {
                    "id": "3",
                    "name": "Wachtplaats C",
                    "facility": ["OVERNIGHT_ACCOMODATION"],
                    "category": "WAITING_AREA",
                    "maximum_stay_duration_h": 2.0,
                    "signalling": None,
                    "length": 100.0,
                    "number_of_rows": 1,
                    "maximal_ship_length": None,
                    "geometry": Point(4.92, 52.12),
                },
                {
                    "id": "4",
                    "name": "Ligplaats Duwvaartsluis Krammer",
                    "facility": [],
                    "category": "ALL_VESSELS",
                    "maximum_stay_duration_h": None,
                    "signalling": None,
                    "length": 450.0,
                    "number_of_rows": 1,
                    "maximal_ship_length": 110.0,
                    "geometry": Point(4.93, 52.13),
                },
            ],
            crs="EPSG:4326",
        )

        cardropoffs = gpd.GeoDataFrame(
            [
                {
                    "id": "101",
                    "berth_id": "1",
                    "name": "Autoafzetplaats A",
                    "geometry": Point(4.9, 52.1),
                }
            ],
            crs="EPSG:4326",
        )

        classified = classify_berths(berths, cardropoffs)
        self.assertTrue(classified.loc[classified["id"] == "1", "is_overnight"].iloc[0])
        self.assertTrue(
            classified.loc[classified["id"] == "1", "has_cardropoff"].iloc[0]
        )
        self.assertTrue(classified.loc[classified["id"] == "2", "is_kegel"].iloc[0])
        self.assertEqual(
            classified.loc[classified["id"] == "2", "kegel_count"].iloc[0], 2
        )

        capacities = calculate_berth_capacities(classified)
        self.assertEqual(
            capacities.loc[capacities["id"] == "1", "capacity_ships_110m"].iloc[0], 2
        )
        self.assertEqual(
            capacities.loc[capacities["id"] == "1", "capacity_ships_135m"].iloc[0], 1
        )
        self.assertEqual(
            capacities.loc[capacities["id"] == "1", "effective_length_m"].iloc[0], 240.0
        )

        df = classify_and_calculate_capacity(berths, cardropoffs)

        self.assertTrue(df.loc[df["id"] == "1", "is_overnight"].iloc[0])
        self.assertTrue(df.loc[df["id"] == "1", "has_cardropoff"].iloc[0])
        self.assertEqual(df.loc[df["id"] == "1", "capacity_ships_110m"].iloc[0], 2)
        self.assertEqual(df.loc[df["id"] == "1", "capacity_ships_135m"].iloc[0], 1)
        self.assertEqual(df.loc[df["id"] == "1", "effective_length_m"].iloc[0], 240.0)
        self.assertFalse(df.loc[df["id"] == "1", "is_kegel"].iloc[0])

        self.assertTrue(df.loc[df["id"] == "2", "is_kegel"].iloc[0])
        self.assertEqual(df.loc[df["id"] == "2", "kegel_count"].iloc[0], 2)
        self.assertEqual(df.loc[df["id"] == "2", "capacity_ships_110m"].iloc[0], 2)
        self.assertEqual(df.loc[df["id"] == "2", "capacity_ships_135m"].iloc[0], 0)
        self.assertEqual(df.loc[df["id"] == "2", "effective_length_m"].iloc[0], 240.0)

        self.assertFalse(df.loc[df["id"] == "3", "is_overnight"].iloc[0])

        self.assertTrue(df.loc[df["id"] == "4", "is_overnight"].iloc[0])
        self.assertEqual(df.loc[df["id"] == "4", "effective_length_m"].iloc[0], 450.0)
        # 450m restricted to max 110m ships -> 3 ships at 110m, 0 ships at 135m
        self.assertEqual(df.loc[df["id"] == "4", "capacity_ships_110m"].iloc[0], 3)
        self.assertEqual(df.loc[df["id"] == "4", "capacity_ships_135m"].iloc[0], 0)

    def test_build_network_classification_map(self):
        fc = gpd.GeoDataFrame(
            [
                {"route_id": "10", "type_description": "Hoofdtransportas"},
                {"route_id": "20", "type_description": "Hoofdvaarweg"},
                {"route_id": "30", "type_description": "Overige vaarweg"},
                {"route_id": "40", "type_description": "Onbekend"},
            ]
        )
        tmap = build_network_classification_map(fc)
        self.assertEqual(tmap["10"], "Hoofdtransportas")
        self.assertEqual(tmap["20"], "Hoofdvaarweg")
        self.assertEqual(tmap["30"], "Overige vaarweg")
        self.assertEqual(tmap["40"], "Overig")
        self.assertEqual(tmap.get("99", "Overig"), "Overig")

    def test_furthest_distance_and_continuous_coverage(self):
        peak_dist = calc_segment_furthest_distance(5.0, 10.0, 10.0)
        self.assertEqual(peak_dist, 12.5)

        dominated_dist = calc_segment_furthest_distance(2.0, 15.0, 10.0)
        self.assertEqual(dominated_dist, 12.0)

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
                "route_id": "100",
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
                "route_id": "100",
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
                    "id": "100",
                    "name": "Kanaal van Test",
                    "code": "KVT",
                    "geometry": LineString([(0, 0), (0, 0.2)]),
                }
            ],
            crs="EPSG:4326",
        )

        berths = gpd.GeoDataFrame(
            [
                {
                    "id": "1",
                    "route_id": "100",
                    "name": "Berth Regulier",
                    "is_overnight": True,
                    "is_kegel": False,
                    "effective_length_m": 240.0,
                    "capacity_ships_110m": 2,
                    "capacity_ships_135m": 1,
                    "has_cardropoff": True,
                },
                {
                    "id": "2",
                    "route_id": "100",
                    "name": "Berth Kegel",
                    "is_overnight": False,
                    "is_kegel": True,
                    "effective_length_m": 120.0,
                    "capacity_ships_110m": 1,
                    "capacity_ships_135m": 0,
                    "has_cardropoff": False,
                },
            ]
        )

        gdf_segments = gpd.GeoDataFrame(
            [
                {
                    "route_id": "100",
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

        gdf_routes = compute_route_carrying_capacity(routes, berths, gdf_segments)

        self.assertEqual(len(gdf_routes), 1)
        r = gdf_routes.iloc[0]
        self.assertTrue(r["is_hvwn"])

        # Expected overnight stops = 20 / 168 (unrounded during computation)
        expected_p_stop = 20.0 / DAILY_SAILING_DISTANCE_KM
        self.assertAlmostEqual(r["expected_overnight_stops"], expected_p_stop, places=6)

        # Overnight length & capacity
        self.assertEqual(r["overnight_berth_length_m"], 240.0)
        self.assertEqual(r["overnight_meter_nights_yr"], 240.0 * 365.0)
        self.assertEqual(r["overnight_capacity_ships_110m"], 2)
        self.assertEqual(r["overnight_capacity_ships_135m"], 1)

        # Max carrying capacity overnight: (240 / 1000) * (168 / 20) * 365 = 735.84 km/yr
        expected_max_cap = (240.0 / 1000.0) * (168.0 / 20.0) * 365.0
        self.assertAlmostEqual(
            r["max_capacity_ship_length_km_yr"], expected_max_cap, places=4
        )
        self.assertAlmostEqual(
            r["max_overnight_passages_110m_yr"],
            2 * (168.0 / 20.0) * 365.0,
            places=4,
        )
        self.assertAlmostEqual(
            r["max_overnight_passages_135m_yr"],
            1 * (168.0 / 20.0) * 365.0,
            places=4,
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
        self.assertEqual(fmt_num(12.345, 1, " km"), "12.3 km")
        self.assertEqual(fmt_num(None, 1, " km"), "-")
        self.assertEqual(fmt_num(0.0, 1, " km", zero_dash=True), "-")
        self.assertEqual(fmt_num(1234.6, 0), "1,235")
        self.assertEqual(fmt_pct(75.56), "75.6%")
        self.assertEqual(fmt_pct(None), "-")
        self.assertEqual(fmt_dist(14.82), "14.8 km")
        self.assertEqual(fmt_dist(float("inf")), "Geen ligplaats bereikbaar")
        self.assertEqual(fmt_dist(None), "Geen ligplaats bereikbaar")

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

        # MultiLineString is merged properly
        multi = MultiLineString([[(0, 0), (0, 10)], [(0, 10), (0, 20)]])
        sub_multi = extract_segment_geometry(multi, 5.0, 15.0, 0.0, 20.0)
        self.assertIsNotNone(sub_multi)
        self.assertAlmostEqual(sub_multi.length, 10.0)

        # Degenerate span returns None
        self.assertIsNone(extract_segment_geometry(line, 5.0, 5.0, 0.0, 20.0))
        self.assertIsNone(extract_segment_geometry(None, 0.0, 10.0, 0.0, 20.0))

    def test_build_route_network_graph_synthetic(self):
        routes = gpd.GeoDataFrame(
            [
                {
                    "id": "100",
                    "name": "Test Fairway",
                    "code": "TF1",
                    "route_km_begin": 0.0,
                    "route_km_end": 30.0,
                    "geometry": LineString([(0, 0), (0, 30)]),
                }
            ],
            crs="EPSG:4326",
        )
        rjs = gpd.GeoDataFrame(
            [
                {
                    "id": "1",
                    "route_id": "100",
                    "section_junction_id": "501",
                    "route_km_begin": 10.0,
                    "geometry": Point(0, 10),
                }
            ],
            crs="EPSG:4326",
        )
        berths = gpd.GeoDataFrame(
            [
                {
                    "id": "10",
                    "route_id": "100",
                    "name": "Berth 10",
                    "route_km": 20.0,
                    "is_overnight": True,
                    "is_kegel": False,
                    "capacity_ships_110m": 2,
                    "capacity_ships_135m": 1,
                    "effective_length_m": 240.0,
                    "has_cardropoff": True,
                    "geometry": Point(0, 20),
                }
            ],
            crs="EPSG:4326",
        )
        type_map = {"100": "Hoofdtransportas"}

        G, segments = build_route_network_graph(
            routes, rjs, berths, type_map, expected_parallel_edges=0
        )
        self.assertEqual(G.number_of_nodes(), 4)  # start, rj_501, berth_10, end
        self.assertEqual(len(segments), 3)
        self.assertTrue(G.nodes["berth_10"]["is_overnight"])
        self.assertEqual(segments[0]["source"], "start_100")
        self.assertEqual(segments[0]["target"], "rj_501")
        self.assertEqual(segments[0]["length_km"], 10.0)

        # Add a second route sharing two junctions (rj_501 -> rj_502) to verify parallel edge removal and assertion
        routes_dup = gpd.GeoDataFrame(
            [
                routes.iloc[0].to_dict(),
                {
                    "id": "200",
                    "name": "Overlap Route",
                    "code": "OR2",
                    "route_km_begin": 0.0,
                    "route_km_end": 5.0,
                    "geometry": LineString([(0, 10), (0, 15)]),
                },
            ],
            crs="EPSG:4326",
        )
        rjs_dup = gpd.GeoDataFrame(
            [
                rjs.iloc[0].to_dict(),
                {
                    "id": "2",
                    "route_id": "100",
                    "section_junction_id": "502",
                    "route_km_begin": 15.0,
                    "geometry": Point(0, 15),
                },
                {
                    "id": "3",
                    "route_id": "200",
                    "section_junction_id": "501",
                    "route_km_begin": 0.0,
                    "geometry": Point(0, 10),
                },
                {
                    "id": "4",
                    "route_id": "200",
                    "section_junction_id": "502",
                    "route_km_begin": 3.0,
                    "geometry": Point(0, 15),
                },
            ],
            crs="EPSG:4326",
        )
        G_dup, segments_dup = build_route_network_graph(
            routes_dup, rjs_dup, berths, type_map, expected_parallel_edges=1
        )
        self.assertEqual(G_dup.number_of_edges(), len(segments_dup))
        self.assertEqual(G_dup.edges["rj_501", "rj_502"]["weight"], 3.0)
        with self.assertRaises(AssertionError):
            build_route_network_graph(
                routes_dup, rjs_dup, berths, type_map, expected_parallel_edges=0
            )

    def test_empty_sources_dijkstra_fails_fast(self):
        G = nx.Graph()
        G.add_node("node_A", is_overnight=False, is_kegel=False)
        route_segments = [{"source": "node_A", "target": "node_A", "length_km": 1.0}]
        with self.assertRaises(ValueError):
            run_spatial_distance_analysis(G, route_segments)


if __name__ == "__main__":
    unittest.main()
