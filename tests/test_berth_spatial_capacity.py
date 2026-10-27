import geopandas as gpd
import networkx as nx
import pandas as pd
import pytest
from shapely.geometry import LineString, Point

from scripts.berth_analysis.analyze_spatial_capacity import (
    DAILY_SAILING_DISTANCE_KM,
    build_network_classification_map,
    calc_berth_ship_slots,
    calc_segment_compliant_length,
    calc_segment_furthest_distance,
    calculate_berth_capacities,
    classify_and_calculate_capacity,
    classify_berths,
    cluster_berths_and_check_facilities,
    compute_route_berth_capacity,
    determine_kegels,
    fmt_dist,
    fmt_num,
    fmt_pct,
    run_spatial_distance_analysis,
)
from scripts.berth_analysis.route_network_graph import (
    build_route_network_graph,
    extract_segment_geometry,
)


@pytest.fixture
def sample_berths_and_cardropoffs():
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
    return berths, cardropoffs


@pytest.fixture
def ajk_chain_graph_and_segments():
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
            "network_type": "Hoofdtransportas",
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
            "network_type": "Hoofdtransportas",
            "km_from": 20.0,
            "km_to": 40.0,
            "length_km": 20.0,
            "geometry": LineString([(0, 0.2), (0, 0.4)]),
        },
    ]
    return G, route_segments


@pytest.fixture
def berth_capacity_inputs():
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
                "capacity_ships_85m": 2,
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
                "capacity_ships_85m": 1,
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
                "network_type": "Hoofdvaarweg",
                "route_name": "Kanaal van Test",
                "route_code": "KVT",
                "geometry": LineString([(0, 0), (0, 0.2)]),
            }
        ],
        crs="EPSG:4326",
    )
    return routes, berths, gdf_segments


@pytest.fixture
def synthetic_graph_inputs():
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
                "capacity_ships_85m": 2,
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
    return routes, rjs, berths, type_map


def test_determine_kegels():
    assert determine_kegels("TWO_BLUE_LIGHTS", "Ligplaats") == 2
    assert determine_kegels(None, "Ligplaats 2-kegel") == 2
    assert determine_kegels(None, "twee kegels ligplaats") == 2
    assert determine_kegels("ONE_BLUE_LIGHT", "Ligplaats") == 1
    assert determine_kegels(None, "1-kegel kade") == 1
    assert determine_kegels(None, "kegelligplaats algemeen") == 1
    assert determine_kegels(None, "Geen kegelligplaats") == 0
    assert determine_kegels(None, "0-kegel kade") == 0
    assert determine_kegels("NO_BLUE_LIGHT", "Reguliere kade") == 0


def test_calc_berth_ship_slots():
    lengths = pd.Series([240.0, 90.0, 300.0, 0.0])
    rows = pd.Series([1.0, 2.0, 1.0, 1.0])
    max_len = pd.Series([float("nan"), 85.0, 110.0, float("nan")])

    slots_85 = calc_berth_ship_slots(lengths, rows, max_len, 85.0)
    slots_110 = calc_berth_ship_slots(lengths, rows, max_len, 110.0)
    slots_135 = calc_berth_ship_slots(lengths, rows, max_len, 135.0)

    assert list(slots_85) == [2.0, 2.0, 3.0, 0.0]
    assert list(slots_110) == [2.0, 0.0, 2.0, 0.0]
    assert list(slots_135) == [1.0, 0.0, 0.0, 0.0]


def test_classify_and_calculate_capacity(sample_berths_and_cardropoffs):
    berths, cardropoffs = sample_berths_and_cardropoffs

    classified = classify_berths(berths, cardropoffs)
    assert classified.loc[classified["id"] == "1", "is_overnight"].iloc[0]
    assert classified.loc[classified["id"] == "1", "has_cardropoff"].iloc[0]
    assert classified.loc[classified["id"] == "2", "is_kegel"].iloc[0]
    assert classified.loc[classified["id"] == "2", "kegel_count"].iloc[0] == 2

    capacities = calculate_berth_capacities(classified)
    assert capacities.loc[capacities["id"] == "1", "capacity_ships_85m"].iloc[0] == 2
    assert capacities.loc[capacities["id"] == "1", "capacity_ships_110m"].iloc[0] == 2
    assert capacities.loc[capacities["id"] == "1", "capacity_ships_135m"].iloc[0] == 1
    assert (
        capacities.loc[capacities["id"] == "1", "effective_length_m"].iloc[0] == 240.0
    )

    df = classify_and_calculate_capacity(berths, cardropoffs)

    assert df.loc[df["id"] == "1", "is_overnight"].iloc[0]
    assert df.loc[df["id"] == "1", "has_cardropoff"].iloc[0]
    assert df.loc[df["id"] == "1", "capacity_ships_110m"].iloc[0] == 2
    assert df.loc[df["id"] == "1", "capacity_ships_135m"].iloc[0] == 1
    assert df.loc[df["id"] == "1", "effective_length_m"].iloc[0] == 240.0
    assert not df.loc[df["id"] == "1", "is_kegel"].iloc[0]

    assert df.loc[df["id"] == "2", "is_kegel"].iloc[0]
    assert df.loc[df["id"] == "2", "kegel_count"].iloc[0] == 2
    assert df.loc[df["id"] == "2", "capacity_ships_110m"].iloc[0] == 2
    assert df.loc[df["id"] == "2", "capacity_ships_135m"].iloc[0] == 0
    assert df.loc[df["id"] == "2", "effective_length_m"].iloc[0] == 240.0

    assert not df.loc[df["id"] == "3", "is_overnight"].iloc[0]

    assert df.loc[df["id"] == "4", "is_overnight"].iloc[0]
    assert df.loc[df["id"] == "4", "effective_length_m"].iloc[0] == 450.0
    # 450m restricted to max 110m ships -> 4 ships at 85m, 3 ships at 110m, 0 ships at 135m
    assert df.loc[df["id"] == "4", "capacity_ships_85m"].iloc[0] == 4
    assert df.loc[df["id"] == "4", "capacity_ships_110m"].iloc[0] == 3
    assert df.loc[df["id"] == "4", "capacity_ships_135m"].iloc[0] == 0


def test_width_fallback_rows_and_spatial_cardropoff_clustering():
    berths = gpd.GeoDataFrame(
        [
            {
                "id": "b1",
                "route_id": "33255",
                "route_km": 0.8,
                "name": "Overnachtingssteiger IJzendoorn",
                "facility": ["OVERNIGHT_ACCOMODATION"],
                "category": "ALL_VESSELS",
                "maximum_stay_duration_h": 72.0,
                "signalling": None,
                "length": 700.0,
                "width": 36.0,
                "number_of_rows": None,
                "maximal_ship_length": 135.0,
                "geometry": Point(5.5500, 51.8900),
            },
            {
                "id": "b2",
                "route_id": "33255",
                "route_km": 0.9,
                "name": "Autosteiger IJzendoorn (kortverblijf)",
                "facility": ["CAR_DROPOFF"],
                "category": "OTHER",
                "maximum_stay_duration_h": 1.0,
                "signalling": None,
                "length": 40.0,
                "width": 12.0,
                "number_of_rows": 1,
                "maximal_ship_length": 110.0,
                "geometry": Point(5.5505, 51.8902),
            },
        ],
        crs="EPSG:4326",
    )
    cardropoffs = gpd.GeoDataFrame(
        columns=["id", "berth_id", "name", "geometry"], crs="EPSG:4326"
    )
    df = classify_and_calculate_capacity(berths, cardropoffs)
    # b1 has width=36.0 and number_of_rows=None -> floor(36.0 / 11.4) = 3 rows
    assert df.loc[df["id"] == "b1", "effective_rows"].iloc[0] == 3.0
    assert df.loc[df["id"] == "b1", "effective_length_m"].iloc[0] == 2100.0
    # 700m @ 110m -> floor(710/120) = 5 per row * 3 rows = 15 ships (>= 15 threshold)
    assert df.loc[df["id"] == "b1", "capacity_ships_110m"].iloc[0] == 15.0
    # b2 is short stay (1h) so not overnight, but has_cardropoff is True
    assert not df.loc[df["id"] == "b2", "is_overnight"].iloc[0]
    assert df.loc[df["id"] == "b2", "has_cardropoff"].iloc[0]

    clusters = cluster_berths_and_check_facilities(df, cardropoffs)
    assert len(clusters) == 1
    assert clusters.iloc[0]["total_capacity_ships_110m"] == 15.0
    assert clusters.iloc[0]["req_cardropoff_110m"]
    assert clusters.iloc[0]["has_cardropoff"]
    assert clusters.iloc[0]["cardropoff_compliant_110m"]


def test_anchorage_swing_diameter_and_max_quay_rows():
    berths = gpd.GeoDataFrame(
        [
            {
                "id": "anc1",
                "route_id": "20940",
                "route_km": 10.0,
                "name": "Ankerplaats 1 Westerschelde Terneuzen",
                "facility": [],
                "category": "OTHER",
                "maximum_stay_duration_h": None,
                "signalling": None,
                "length": 1600.0,
                "width": 330.0,
                "number_of_rows": None,
                "maximal_ship_length": None,
                "geometry": Point(3.8, 51.3),
            },
            {
                "id": "anc2",
                "route_id": "19631",
                "route_km": 5.0,
                "name": "Ankerplaats 1 Hollandsch Diep Willemstad",
                "facility": ["OVERNIGHT_ACCOMODATION"],
                "category": "ALL_VESSELS",
                "maximum_stay_duration_h": None,
                "signalling": None,
                "length": 400.0,
                "width": 300.0,
                "number_of_rows": None,
                "maximal_ship_length": None,
                "geometry": Point(4.4, 51.7),
            },
            {
                "id": "quay_wide",
                "route_id": "100",
                "route_km": 2.0,
                "name": "Brede kade havenbekken",
                "facility": ["OVERNIGHT_ACCOMODATION"],
                "category": "ALL_VESSELS",
                "maximum_stay_duration_h": 72.0,
                "signalling": None,
                "length": 300.0,
                "width": 80.0,
                "number_of_rows": None,
                "maximal_ship_length": None,
                "geometry": Point(4.5, 51.8),
            },
        ],
        crs="EPSG:4326",
    )
    cardropoffs = gpd.GeoDataFrame(
        columns=["id", "berth_id", "name", "geometry"], crs="EPSG:4326"
    )
    df = classify_and_calculate_capacity(berths, cardropoffs)

    # anc1: 1600m x 330m anchorage -> swinging diameter ~300m -> 1 row (not 28 rows!)
    assert df.loc[df["id"] == "anc1", "effective_rows"].iloc[0] == 1.0
    assert df.loc[df["id"] == "anc1", "effective_length_m"].iloc[0] == 1600.0
    # floor((1600 + 10) / 120) = 13 ships
    assert df.loc[df["id"] == "anc1", "capacity_ships_110m"].iloc[0] == 13.0

    # anc2: 400m x 300m anchorage -> swinging diameter ~300m -> 1 row (not 26 rows!)
    assert df.loc[df["id"] == "anc2", "effective_rows"].iloc[0] == 1.0
    assert df.loc[df["id"] == "anc2", "effective_length_m"].iloc[0] == 400.0
    # floor((400 + 10) / 120) = 3 ships (matches empirical 4-month AIS timex!)
    assert df.loc[df["id"] == "anc2", "capacity_ships_110m"].iloc[0] == 3.0

    # quay_wide: 80m wide quay -> width / 11.4 = 7 rows, capped at MAX_QUAY_ROWS = 3.0
    assert df.loc[df["id"] == "quay_wide", "effective_rows"].iloc[0] == 3.0
    assert df.loc[df["id"] == "quay_wide", "effective_length_m"].iloc[0] == 900.0


def test_recreation_and_yacht_lock_exclusion():
    routes = gpd.GeoDataFrame(
        [
            {
                "id": "30984852",
                "name": "Westelijke voorhaven Krammer Jachtsluizen - Krammer",
                "geometry": LineString([(4.12, 51.66), (4.13, 51.67)]),
            },
            {
                "id": "54844",
                "name": "Zuid Vlije, Jachtensluis Krammersluizen en Krammer-Sport",
                "geometry": LineString([(4.13, 51.67), (4.14, 51.68)]),
            },
            {
                "id": "100",
                "name": "Waal",
                "geometry": LineString([(5.0, 51.8), (5.1, 51.8)]),
            },
        ],
        crs="EPSG:4326",
    )
    berths = gpd.GeoDataFrame(
        [
            {
                "id": "rec_krammer",
                "route_id": "30984852",
                "name": "Ankerplaats 1 Krammer",
                "facility": [],
                "category": "OTHER",
                "maximum_stay_duration_h": None,
                "length": 822.0,
                "width": 355.0,
                "geometry": Point(4.125, 51.666),
            },
            {
                "id": "rec_worp",
                "route_id": "100",
                "name": "Ligplaats recreatievaart IJssel Worpzijde",
                "facility": [],
                "category": "OTHER",
                "maximum_stay_duration_h": None,
                "length": 105.0,
                "width": 7.0,
                "geometry": Point(5.05, 51.81),
            },
            {
                "id": "rec_gennep",
                "route_id": "100",
                "name": "Ligplaats loswal Gennep",
                "note": "Bestemd voor de pleziervaart.",
                "facility": ["GANGWAY"],
                "category": "ALL_VESSELS",
                "maximum_stay_duration_h": None,
                "length": 90.0,
                "width": 15.0,
                "geometry": Point(5.06, 51.82),
            },
            {
                "id": "comm_duwvaart",
                "route_id": "54844",
                "name": "Ligplaats 1 Duwvaartsluis Krammer oostzijde Bruinisse",
                "note": "Voor schepen, zowel duwvaart als andere schepen dan duwvaart.",
                "facility": [],
                "category": "OTHER",
                "maximum_stay_duration_h": None,
                "length": 180.0,
                "width": 40.0,
                "geometry": Point(4.135, 51.675),
            },
        ],
        crs="EPSG:4326",
    )
    cardropoffs = gpd.GeoDataFrame(
        columns=["id", "berth_id", "name", "geometry"], crs="EPSG:4326"
    )

    df = classify_and_calculate_capacity(berths, cardropoffs, routes=routes)

    # Recreation berths and yacht lock approach anchorages are excluded:
    assert not df.loc[df["id"] == "rec_krammer", "is_overnight"].iloc[0]
    assert not df.loc[df["id"] == "rec_worp", "is_overnight"].iloc[0]
    assert not df.loc[df["id"] == "rec_gennep", "is_overnight"].iloc[0]

    # Commercial duwvaart berth at Krammer is preserved:
    assert df.loc[df["id"] == "comm_duwvaart", "is_overnight"].iloc[0]


def test_build_network_classification_map():
    fc = gpd.GeoDataFrame(
        [
            {"route_id": "10", "type_description": "Hoofdtransportas"},
            {"route_id": "20", "type_description": "Hoofdvaarweg"},
            {"route_id": "30", "type_description": "Overige vaarweg"},
            {"route_id": "40", "type_description": "Onbekend"},
        ]
    )
    tmap = build_network_classification_map(fc)
    assert tmap["10"] == "Hoofdtransportas"
    assert tmap["20"] == "Hoofdvaarweg"
    assert tmap["30"] == "Overige vaarweg"
    assert tmap["40"] == "Overig"


def test_furthest_distance_and_continuous_coverage():
    assert calc_segment_furthest_distance(5.0, 10.0, 10.0) == 12.5
    assert calc_segment_furthest_distance(2.0, 15.0, 10.0) == 12.0
    assert calc_segment_compliant_length(5.0, 12.0, 20.0, 15.0) == 13.0
    assert calc_segment_compliant_length(5.0, 5.0, 10.0, 15.0) == 10.0


def test_spatial_distance_analysis_ajk_chain(ajk_chain_graph_and_segments):
    G, route_segments = ajk_chain_graph_and_segments
    gdf = run_spatial_distance_analysis(G, route_segments)

    seg_aj = gdf.iloc[0]
    assert seg_aj["is_hvwn"]
    assert seg_aj["dist_to_overnight_km"] == 20.0
    assert seg_aj["compliant_30km_overnight_km"] == 15.0
    assert not seg_aj["is_fully_compliant_overnight"]

    seg_jk = gdf.iloc[1]
    assert seg_jk["dist_to_overnight_km"] == 40.0
    assert seg_jk["compliant_30km_overnight_km"] == 0.0
    assert not seg_jk["is_fully_compliant_overnight"]


def test_compute_route_berth_capacity(berth_capacity_inputs):
    routes, berths, gdf_segments = berth_capacity_inputs
    gdf_routes = compute_route_berth_capacity(routes, berths, gdf_segments)

    assert len(gdf_routes) == 1
    r = gdf_routes.iloc[0]
    assert r["is_hvwn"]

    # Expected overnight stops = 20 / 168 (unrounded during computation)
    expected_p_stop = 20.0 / DAILY_SAILING_DISTANCE_KM
    assert r["expected_overnight_stops"] == pytest.approx(expected_p_stop, abs=1e-6)

    # Overnight length & capacity
    assert r["overnight_berth_length_m"] == 240.0
    assert r["overnight_meter_nights_yr"] == 240.0 * 365.0
    assert r["overnight_capacity_ships_110m"] == 2
    assert r["overnight_capacity_ships_135m"] == 1

    # Max carrying capacity overnight: (240 / 1000) * (168 / 20) * 365 = 735.84 km/yr
    expected_max_cap = (240.0 / 1000.0) * (168.0 / 20.0) * 365.0
    assert r["max_overnight_traffic_volume_km_yr"] == pytest.approx(
        expected_max_cap, abs=1e-4
    )
    assert r["max_overnight_passages_110m_yr"] == pytest.approx(
        2 * (168.0 / 20.0) * 365.0, abs=1e-4
    )
    assert r["max_overnight_passages_135m_yr"] == pytest.approx(
        1 * (168.0 / 20.0) * 365.0, abs=1e-4
    )

    # Kegel length & capacity
    assert r["kegel_berth_length_m"] == 120.0
    assert r["kegel_meter_nights_yr"] == 120.0 * 365.0
    expected_max_kegel = (120.0 / 1000.0) * (168.0 / 20.0) * 365.0
    assert r["max_kegel_traffic_volume_km_yr"] == pytest.approx(
        expected_max_kegel, abs=1e-4
    )

    # Coverage percentages: 15 / 20 * 100 = 75%
    assert r["overnight_coverage_pct"] == 75.0
    assert r["kegel_coverage_pct"] == 100.0

    # Without graph G, corridor capacity falls back to local route capacity
    assert r["corridor_overnight_berths"] == 1
    assert r["corridor_overnight_berth_length_m"] == 240.0
    assert r["corridor_overnight_capacity_ships_85m"] == 2
    assert r["corridor_overnight_capacity_ships_110m"] == 2
    assert r["corridor_overnight_capacity_ships_135m"] == 1


def test_formatting_helpers():
    assert fmt_num(12.345, 1, " km") == "12.3 km"
    assert fmt_num(None, 1, " km") == "-"
    assert fmt_num(0.0, 1, " km", zero_dash=True) == "-"
    assert fmt_num(1234.6, 0) == "1,235"
    assert fmt_pct(75.56) == "75.6%"
    assert fmt_pct(None) == "-"
    assert fmt_dist(14.82) == "14.8 km"
    assert fmt_dist(float("inf")) == "Geen ligplaats bereikbaar"
    assert fmt_dist(None) == "Geen ligplaats bereikbaar"


def test_extract_segment_geometry():
    line = LineString([(0, 0), (0, 20)])

    # Increasing kilometrage: 5km to 15km on 0-20km line
    sub = extract_segment_geometry(line, 5.0, 15.0, 0.0, 20.0)
    assert sub is not None
    assert sub.length == pytest.approx(10.0)

    # Decreasing kilometrage: 15km to 5km on 20-0km line
    sub_rev = extract_segment_geometry(line, 5.0, 15.0, 20.0, 0.0)
    assert sub_rev is not None
    assert sub_rev.length == pytest.approx(10.0)

    # Degenerate span returns None
    assert extract_segment_geometry(line, 5.0, 5.0, 0.0, 20.0) is None
    assert extract_segment_geometry(None, 0.0, 10.0, 0.0, 20.0) is None


def test_build_route_network_graph_synthetic(synthetic_graph_inputs):
    routes, rjs, berths, type_map = synthetic_graph_inputs

    G, segments = build_route_network_graph(
        routes, rjs, berths, type_map, expected_parallel_edges=0
    )
    assert G.number_of_nodes() == 4  # start, rj_501, berth_10, end
    assert len(segments) == 3
    assert G.nodes["berth_10"]["is_overnight"]
    assert segments[0]["source"] == "start_100"
    assert segments[0]["target"] == "rj_501"
    assert segments[0]["length_km"] == 10.0

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
    assert G_dup.number_of_edges() == len(segments_dup)
    assert G_dup.edges["rj_501", "rj_502"]["weight"] == 3.0
    with pytest.raises(AssertionError):
        build_route_network_graph(
            routes_dup, rjs_dup, berths, type_map, expected_parallel_edges=0
        )


def test_empty_sources_dijkstra_fails_fast():
    G = nx.Graph()
    G.add_node("node_A", is_overnight=False, is_kegel=False)
    route_segments = [{"source": "node_A", "target": "node_A", "length_km": 1.0}]
    with pytest.raises(ValueError):
        run_spatial_distance_analysis(G, route_segments)
