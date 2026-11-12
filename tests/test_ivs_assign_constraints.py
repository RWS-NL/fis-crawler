import pathlib
import pickle
import networkx as nx
import pytest
from scipy.spatial import KDTree
from shapely.geometry import Point
from fis.graph.validation import GraphValidator
from fis.ivs.assign import (
    AnnotatedPenalty,
    _find_min_length,
    _find_min_width,
    _get_ivs_config,
    _get_unlocode_node_overrides,
    _resolve_fis_section_id,
    calc_lock_delay,
    calc_travel_time,
    build_edge_structures_lookup,
    check_edge_constraints_soft,
    geocode_unlocode,
    get_edge_constraints,
    get_edge_key,
    get_edge_weight_soft,
)


@pytest.fixture
def vlissingen_chambers():
    """Lock Vlissingen (id 16483) with parallel small and large chambers."""
    return [
        {
            "id": "8136",
            "name": "Kleine kolk sluis Vlissingen",
            "lock_id": "16483",
            "lock_name": "Sluis Vlissingen",
            "fairway_section_id": "45550",
            "dim_gate_width": 8.0,
            "dim_usable_length": 64.8,
            "passage_duration_m": 30.0,
        },
        {
            "id": "39760",
            "name": "Grote kolk sluis Vlissingen",
            "lock_id": "16483",
            "lock_name": "Sluis Vlissingen",
            "fairway_section_id": "45550",
            "dim_gate_width": 22.5,
            "dim_usable_length": 140.7,
            "passage_duration_m": 30.0,
        },
    ]


@pytest.fixture
def volkerak_chambers():
    """Volkerak commercial lock complex (id 42863) with 3 parallel large chambers."""
    return [
        {
            "id": "6428",
            "name": "Westkolk Volkeraksluizen",
            "lock_id": "42863",
            "lock_name": "Volkeraksluizen",
            "fairway_section_id": "12821",
            "dim_gate_width": 24.0,
            "dim_usable_length": 327.0,
            "passage_duration_m": 30.0,
        },
        {
            "id": "7083",
            "name": "Oostkolk Volkeraksluizen",
            "lock_id": "42863",
            "lock_name": "Volkeraksluizen",
            "fairway_section_id": "12821",
            "dim_gate_width": 24.1,
            "dim_usable_length": 331.5,
            "passage_duration_m": 30.0,
        },
        {
            "id": "24817",
            "name": "Middenkolk Volkeraksluizen",
            "lock_id": "42863",
            "lock_name": "Volkeraksluizen",
            "fairway_section_id": "12821",
            "dim_gate_width": 24.1,
            "dim_usable_length": 308.9,
            "passage_duration_m": 30.0,
        },
    ]


@pytest.fixture
def jachtensluis_chamber():
    """Volkerak Jachtensluis (id 52078) dedicated to pleasure craft."""
    return [
        {
            "id": "18373",
            "name": "Sluiskolk Jachtensluis Volkeraksluizen",
            "lock_id": "52078",
            "lock_name": "Jachtensluis Volkeraksluizen",
            "fairway_section_id": "27027",
            "dim_gate_width": 16.0,
            "dim_usable_length": 135.0,
            "passage_duration_m": 30.0,
        }
    ]


@pytest.fixture
def parallel_bridge_openings():
    """Bridge with small side opening and large navigation opening."""
    return [
        {
            "id": "1001",
            "name": "Zijopening vaste brug",
            "bridge_id": "bridge_101",
            "dim_structural_width": 8.8,
        },
        {
            "id": "1002",
            "name": "Hoofddoorvaart vaste brug",
            "bridge_id": "bridge_101",
            "dim_structural_width": 140.0,
        },
    ]


def test_parallel_lock_chambers_allow_larger_vessel(vlissingen_chambers):
    """An M12 (17 x 135 m) vessel must pass through Vlissingen via the large chamber without penalty."""
    d_edge = {"length_m": 1000.0, "fis_id": "45550"}
    struct_data = {"chambers": vlissingen_chambers, "openings": []}

    # M12 ship (17m beam, 135m length) fits the large chamber (22.5 x 140.7m)
    ship_m12 = {"beam": 17.0, "length": 135.0, "cemt": "VB"}
    pen, viol = check_edge_constraints_soft(d_edge, struct_data, ship_m12)
    assert pen["lock"] == 0.0
    assert len(viol) == 0

    # Vessel wider than all chambers (e.g. 24m) gets beam penalty on the widest chamber
    ship_wide = {"beam": 24.0, "length": 110.0, "cemt": "VB"}
    pen, viol = check_edge_constraints_soft(d_edge, struct_data, ship_wide)
    assert pen["lock"] == 1000.0
    assert len(viol) == 1
    assert viol[0]["type"] == "beam"
    assert viol[0]["limit_value"] == 22.5

    # Vessel longer than all chambers (e.g. 150m) gets length penalty on the longest chamber
    ship_long = {"beam": 11.4, "length": 150.0, "cemt": "VB"}
    pen, viol = check_edge_constraints_soft(d_edge, struct_data, ship_long)
    assert pen["lock"] == 1000.0
    assert len(viol) == 1
    assert viol[0]["type"] == "length"
    assert viol[0]["limit_value"] == 140.7


def test_parallel_bridge_openings_allow_larger_vessel(parallel_bridge_openings):
    """A vessel of 11.4m beam must pass through the bridge via the 140m opening without penalty."""
    d_edge = {"length_m": 500.0, "fis_id": "section_bridge"}
    struct_data = {"chambers": [], "openings": parallel_bridge_openings}

    ship_m8 = {"beam": 11.4, "length": 110.0, "cemt": "VA"}
    pen, viol = check_edge_constraints_soft(d_edge, struct_data, ship_m8)
    assert pen["bridge"] == 0.0
    assert len(viol) == 0

    # Vessel wider than all openings (e.g. 150m) gets bridge beam penalty
    ship_huge = {"beam": 150.0, "length": 110.0, "cemt": "VB"}
    pen, viol = check_edge_constraints_soft(d_edge, struct_data, ship_huge)
    assert pen["bridge"] == 1000.0
    assert len(viol) == 1
    assert viol[0]["type"] == "beam"
    assert viol[0]["limit_value"] == 140.0


def test_lock_delay_counted_once_per_complex(volkerak_chambers):
    """Lock delay must be 0.5h for the Volkerak complex, not 3 x 0.5h = 1.5h (issue #211)."""
    d_edge = {"length_m": 10000.0, "fis_id": "12821", "maxspeed": 10.0}
    struct_data = {"chambers": volkerak_chambers, "openings": []}
    ship_m8 = {"beam": 11.4, "length": 110.0, "cemt": "VA"}

    # Travel time: 10 km / 10 km/h = 1.0 h.
    # Single lock complex delay = 30 min / 60 = 0.5 h.
    # Total cost should be exactly 1.0 + 0.5 = 1.5 h.
    weight = get_edge_weight_soft(d_edge, struct_data, ship_m8)
    assert pytest.approx(weight, rel=1e-5) == 1.5


def test_recreation_lock_penalized_for_commercial_vessel(jachtensluis_chamber):
    """Jachtensluizen must be penalized for commercial vessels to prevent unwanted shortcuts (issue #211)."""
    d_edge = {"length_m": 1000.0, "fis_id": "27027", "maxspeed": 10.0}
    struct_data = {"chambers": jachtensluis_chamber, "openings": []}
    ship_m8 = {"beam": 11.4, "length": 110.0, "cemt": "VA"}

    pen, viol = check_edge_constraints_soft(d_edge, struct_data, ship_m8)
    assert pen["lock"] >= 1000.0
    recreation_viols = [v for v in viol if v["type"] == "recreation"]
    assert len(recreation_viols) == 1
    assert "Jachtensluis" in recreation_viols[0]["struct_name"]


def test_structures_in_series_on_same_section():
    """Multiple structures in series on one fairway section must each be passable."""
    structures = {
        "chambers": [
            {
                "id": "42751",
                "name": "Sluiskolk Voorsluis Eefde",
                "lock_id": "lock_voor",
                "dim_gate_width": 24.0,
                "dim_usable_length": 200.0,
            },
            {
                "id": "50318",
                "name": "Noordkolk sluis Eefde",
                "lock_id": "lock_hoofd",
                "dim_gate_width": 12.5,
                "dim_usable_length": 125.0,
            },
        ],
        "openings": [],
    }
    d_edge = {"length_m": 2000.0, "fis_id": "30985952"}

    # Ship of 11.4m x 110m fits both Voorsluis (24m) and Hoofdsluis (12.5m)
    ship_fit = {"beam": 11.4, "length": 110.0, "cemt": "VA"}
    pen, viol = check_edge_constraints_soft(d_edge, structures, ship_fit)
    assert pen["lock"] == 0.0
    assert len(viol) == 0

    # Ship of 17m x 135m fits Voorsluis (24m) but fails Hoofdsluis (12.5m)
    ship_block = {"beam": 17.0, "length": 135.0, "cemt": "VB"}
    pen, viol = check_edge_constraints_soft(d_edge, structures, ship_block)
    assert pen["lock"] >= 1000.0
    assert any(v["limit_value"] == 12.5 for v in viol)


def test_find_min_width_and_length_helper_functions(
    vlissingen_chambers, parallel_bridge_openings
):
    """_find_min_width and _find_min_length must report the widest/longest passable options per structure."""
    d_edge = {"dim_structural_width": 50.0, "fis_id": "edge_1"}

    min_w, lim_w = _find_min_width(
        vlissingen_chambers, parallel_bridge_openings, d_edge
    )
    # Lock widest is 22.5m, bridge widest is 140m, edge is 50m -> bottleneck is lock 22.5m
    assert min_w == 22.5
    assert lim_w[0] == "39760"

    min_l, lim_l = _find_min_length(vlissingen_chambers)
    # Lock longest is 140.7m
    assert min_l == 140.7
    assert lim_l[0] == "39760"


def test_consistent_network_penalty_on_foreign_structure():
    """Foreign structures (e.g. French/German EURIS locks) receive the same physical penalty (PENALTY_PHYSICAL = 1000.0)."""
    d_edge = {"length_m": 1000.0, "code": "FRVN18800003"}
    structures = {
        "chambers": [
            {
                "id": "FR_CHAMBER_1",
                "name": "Ecluse de Gerstheim",
                "lock_id": "FR_LOCK_1",
                "dim_gate_width": 12.0,
                "dim_usable_length": 150.0,
            }
        ],
        "openings": [],
    }
    # Ship of beam 14.0m exceeds gate width 12.0m -> PENALTY_PHYSICAL = 1000.0
    ship = {"beam": 14.0, "length": 110.0, "cemt": "VA"}
    pen, viol = check_edge_constraints_soft(d_edge, structures, ship)
    assert pen["lock"] == 1000.0
    assert len(viol) == 1
    assert viol[0]["type"] == "beam"
    assert viol[0]["limit_value"] == 12.0


def test_parallel_lock_crossed_dimensions_blocked():
    """
    Test crossed dimensions (Copilot review comment):
    Chamber 1 is wide but short (20 x 80 m).
    Chamber 2 is narrow but long (10 x 150 m).
    A vessel of 15 x 100 m fits neither chamber and must receive a physical penalty.
    """
    crossed_chambers = [
        {
            "id": "c1",
            "name": "Wide Short Chamber",
            "lock_id": "lock_crossed",
            "dim_gate_width": 20.0,
            "dim_usable_length": 80.0,
        },
        {
            "id": "c2",
            "name": "Narrow Long Chamber",
            "lock_id": "lock_crossed",
            "dim_gate_width": 10.0,
            "dim_usable_length": 150.0,
        },
    ]
    d_edge = {"length_m": 1000.0, "fis_id": "edge_crossed"}
    struct_data = {"chambers": crossed_chambers, "openings": []}

    ship = {"beam": 15.0, "length": 100.0, "cemt": "VB"}
    pen, viols = check_edge_constraints_soft(d_edge, struct_data, ship)
    assert pen["lock"] >= 1000.0
    assert len(viols) > 0


def test_mixed_commercial_and_recreation_complex():
    """
    A complex with both a commercial chamber and a recreation chamber must allow commercial
    traffic through the commercial chamber without recreation penalty (Copilot review comment).
    """
    mixed_chambers = [
        {
            "id": "c_rec",
            "name": "Jachtensluis kolk",
            "lock_id": "lock_mixed",
            "dim_gate_width": 12.0,
            "dim_usable_length": 60.0,
        },
        {
            "id": "c_comm",
            "name": "Beroepskolk",
            "lock_id": "lock_mixed",
            "dim_gate_width": 24.0,
            "dim_usable_length": 200.0,
        },
    ]
    d_edge = {"length_m": 1000.0, "fis_id": "edge_mixed"}
    struct_data = {"chambers": mixed_chambers, "openings": []}

    ship = {"beam": 15.0, "length": 110.0, "cemt": "VB"}
    pen, viols = check_edge_constraints_soft(d_edge, struct_data, ship)
    assert pen["lock"] == 0.0
    assert len(viols) == 0


def test_get_edge_constraints_annotated_penalties():
    """get_edge_constraints returns total penalty cost, AnnotatedPenalty list with sources, and by_source dict."""
    d_edge = {
        "length_m": 1000.0,
        "fis_id": "edge_annotated",
        "water_name": "Noordzee kustlijn",
        "dim_structural_width": 12.0,
    }
    struct_data = {
        "chambers": [
            {
                "id": "ch_small",
                "name": "Sluisje",
                "lock_id": "lock_sm",
                "dim_gate_width": 8.0,
                "dim_usable_length": 50.0,
            }
        ],
        "openings": [
            {
                "id": "op_narrow",
                "name": "Smalle brug",
                "bridge_id": "br_sm",
                "dim_structural_width": 9.0,
            }
        ],
    }
    ship = {"beam": 14.0, "length": 80.0, "cemt": "VA"}

    total_cost, penalties, by_source = get_edge_constraints(d_edge, struct_data, ship)

    # lock (1000 beam + 1000 length) + bridge (1000 beam) + fairway (1000 beam) + sea (100000)
    assert by_source["lock"] == 2000.0
    assert by_source["bridge"] == 1000.0
    assert by_source["fairway"] == 1000.0
    assert by_source["sea"] == 100000.0
    assert total_cost == 104000.0
    assert by_source["total"] == total_cost

    # Check that each penalty is an AnnotatedPenalty with source, cost, and description
    assert all(isinstance(p, AnnotatedPenalty) for p in penalties)
    sources = [p.source for p in penalties]
    assert "lock" in sources
    assert "bridge" in sources
    assert "fairway" in sources
    assert "sea" in sources


def test_calc_travel_time_and_lock_delay(volkerak_chambers):
    """calc_travel_time and calc_lock_delay compute respective edge components in hours."""
    d_edge = {"length_m": 15000.0, "maxspeed": 10.0}
    t_time = calc_travel_time(d_edge)
    assert pytest.approx(t_time, rel=1e-5) == 1.5

    ship = {"beam": 11.4, "length": 110.0, "cemt": "VA"}
    delay = calc_lock_delay(volkerak_chambers, ship)
    assert pytest.approx(delay, rel=1e-5) == 0.5


def test_geocode_unlocode_pinned_ports():
    """Verify that [ivs.unlocode_nodes] in schema.toml resolves directly to designated fairway nodes."""
    pinned_nodes = _get_unlocode_node_overrides()
    G = nx.Graph()
    for node_id in pinned_nodes.values():
        G.add_node(node_id, geometry=Point(4.0, 52.0))

    tree = KDTree([(0.0, 0.0)])
    node_ids = ["fallback_node"]

    for code, expected_node in pinned_nodes.items():
        # Exact code
        assert geocode_unlocode(code, {}, {}, tree, node_ids, G) == expected_node
        # Lowercase and whitespace
        assert (
            geocode_unlocode(f"  {code.lower()}  ", {}, {}, tree, node_ids, G)
            == expected_node
        )


def test_geocode_unlocode_fallback_when_pinned_missing():
    """Verify that if a pinned fairway node is missing from graph, it falls back to standard lookup."""
    G = nx.Graph()
    G.add_node("harbour_fallback", geometry=Point(4.5, 52.0), node_type="harbour")

    # Node locode map pointing to fallback
    node_locode_map = {"NLAMS": [("harbour_fallback", "harbour")]}
    tree = KDTree([(4.5, 52.0)])
    node_ids = ["harbour_fallback"]

    # Graph does not have FIS_8864384, so it should fall back to harbour_fallback
    res = geocode_unlocode("NLAMS", {}, node_locode_map, tree, node_ids, G)
    assert res == "harbour_fallback"


def test_key_ports_routing_real_network():
    """Integration check on real merged graph: all pinned ports resolve and route with zero penalty."""
    graph_path = pathlib.Path("output/merged-graph/graph.pickle")
    required_paths = [
        graph_path,
        pathlib.Path("output/dropins-fis-detailed/chambers.geoparquet"),
        pathlib.Path("output/dropins-fis-detailed/openings.geoparquet"),
        pathlib.Path("output/dropins-euris-detailed/edges.geoparquet"),
        pathlib.Path("output/dropins-euris-detailed/chambers.geoparquet"),
        pathlib.Path("output/dropins-euris-detailed/openings.geoparquet"),
    ]
    if not all(p.exists() for p in required_paths):
        pytest.skip(
            "Full merged network graph or detailed structure files not found; skipping integration routing test."
        )

    with open(graph_path, "rb") as f:
        G_merged = pickle.load(f)

    lookup = build_edge_structures_lookup()

    pinned_nodes = _get_unlocode_node_overrides()

    # 1. Verify every pinned port node exists in G_merged
    for code, node_id in pinned_nodes.items():
        assert node_id in G_merged, (
            f"Pinned node {node_id} for {code} not found in G_merged"
        )

    # 2. Test routing between major port pairs with a standard Class Va vessel
    ship = {"beam": 11.4, "length": 110.0, "draft": 2.8, "height": 6.0, "cemt": "VA"}
    test_pairs = [
        ("NLRTM", "NLAMS"),
        ("NLAMS", "NLLEY"),
        ("NLRTM", "BEANR"),
        ("NLRTM", "DEDUI"),
        ("BEGNE", "NLTNZ"),
        ("NLDOR", "NLRTM"),
    ]

    for orig, dest in test_pairs:
        orig_node = pinned_nodes[orig]
        dest_node = pinned_nodes[dest]

        def weight_func(u, v, d):
            edge_id = get_edge_key(d)
            struct_data = lookup.get(edge_id, {"chambers": [], "openings": []})
            return get_edge_weight_soft(d, struct_data, ship)

        path = nx.shortest_path(
            G_merged,
            orig_node,
            dest_node,
            weight=weight_func,
        )
        assert len(path) > 1

        # Check total penalty along the shortest path
        total_penalties = 0.0
        violations = []
        for u, v in zip(path[:-1], path[1:]):
            edge_data = G_merged[u][v]
            if isinstance(edge_data, dict) and 0 in edge_data:
                edge_data = edge_data[0]
            edge_id = get_edge_key(edge_data)
            struct_data = lookup.get(edge_id, {"chambers": [], "openings": []})
            pen_dict, v_list = check_edge_constraints_soft(edge_data, struct_data, ship)
            total_penalties += sum(pen_dict.values())
            violations.extend(v_list)

        assert total_penalties == 0.0, (
            f"Route between {orig} ({orig_node}) and {dest} ({dest_node}) incurred {total_penalties} penalties: {violations}"
        )
        assert len(violations) == 0, (
            f"Route between {orig} and {dest} had unexpected violations: {violations}"
        )


def test_get_edge_key_border_fallback():
    """Verify get_edge_key handles fis_id, code, synthetic border fallback, and ignores harbours."""
    assert get_edge_key({"fis_id": 12345}) == "12345"
    assert get_edge_key({"code": "DE_123"}) == "DE_123"

    # Non-harbour edge without fis_id/code gets a deterministic sorted virtual key
    border_edge = {"data_source": "EURIS", "is_border": True}
    k1 = get_edge_key(border_edge, "EURIS_DE_J1230", "EURIS_FR_J1937")
    k2 = get_edge_key(border_edge, "EURIS_FR_J1937", "EURIS_DE_J1230")
    assert k1 == "virtual_EURIS_DE_J1230_EURIS_FR_J1937"
    assert k1 == k2

    # Harbour access edges should not get a synthetic key
    harbour_edge = {"data_source": "vinharbour", "segment_type": "harbour_access"}
    assert get_edge_key(harbour_edge, "FIS_1", "FIS_harbour_1") is None


def test_resolve_fis_section_id_redirects():
    """Verify all overwritten FIS section IDs resolve to their canonical graph fis_id."""
    ivs_cfg = _get_ivs_config()
    for old_sid, canonical_sid in ivs_cfg.get("section_id_redirects", {}).items():
        assert _resolve_fis_section_id(old_sid, "Some structure") == canonical_sid
        assert (
            _resolve_fis_section_id(float(old_sid), "Some structure") == canonical_sid
        )

    for ignored_sid in ivs_cfg.get("ignored_section_ids", []):
        assert _resolve_fis_section_id(ignored_sid, "Ignored side channel") is None
        assert (
            _resolve_fis_section_id(float(ignored_sid), "Ignored side channel") is None
        )

    assert _resolve_fis_section_id("12840", "Sluiskolk Irenesluis") == "47304"
    assert _resolve_fis_section_id("99999", "Regular lock") == "99999"
    assert _resolve_fis_section_id(None, "Missing sid") is None


def test_graph_validator_edge_keys():
    """Verify GraphValidator.check_edge_keys flags unkeyed non-harbour edges and ignores harbours."""
    G = nx.Graph()
    G.add_edge("FIS_1", "FIS_2", fis_id="100", data_source="FIS")
    G.add_edge(
        "FIS_2",
        "FIS_harbour_1",
        data_source="vinharbour",
        segment_type="harbour_access",
    )
    v_pass = GraphValidator(G)
    res_pass = v_pass.check_edge_keys()
    assert res_pass["status"] == "PASS"
    assert res_pass["missing_key_count"] == 0

    # Add an unkeyed EURIS border connector and NaN-keyed edges
    G.add_edge("EURIS_DE_1", "EURIS_FR_1", data_source="EURIS", is_border=True)
    G.add_edge("EURIS_DE_2", "EURIS_FR_2", fis_id=float("nan"), data_source="EURIS")
    G.add_edge("EURIS_DE_3", "EURIS_FR_3", code="nan", data_source="EURIS")
    v_fail = GraphValidator(G)
    res_fail = v_fail.check_edge_keys()
    assert res_fail["status"] == "FAIL"
    assert res_fail["missing_key_count"] == 3


def test_all_fis_structures_resolve_to_merged_graph():
    """Integration check: every FIS chamber/opening section ID resolves to an active edge in G_merged."""
    graph_path = pathlib.Path("output/merged-graph/graph.pickle")
    required_paths = [
        graph_path,
        pathlib.Path("output/dropins-fis-detailed/chambers.geoparquet"),
        pathlib.Path("output/dropins-fis-detailed/openings.geoparquet"),
        pathlib.Path("output/dropins-euris-detailed/edges.geoparquet"),
        pathlib.Path("output/dropins-euris-detailed/chambers.geoparquet"),
        pathlib.Path("output/dropins-euris-detailed/openings.geoparquet"),
    ]
    if not all(p.exists() for p in required_paths):
        pytest.skip(
            "Full merged network graph or detailed structure files not found; skipping."
        )

    with open(graph_path, "rb") as f:
        G_merged = pickle.load(f)

    validator = GraphValidator(G_merged)
    res = validator.check_structure_lookup()
    assert res["status"] == "PASS", (
        f"Unresolved FIS structures: {res.get('fis_missing_examples')}"
    )
    assert res["fis_missing_count"] == 0

    # Verify specific redirected structures from #214 are present in lookup
    lookup = build_edge_structures_lookup()
    assert any("Hartelsluis" in c.get("name", "") for c in lookup["43326"]["chambers"])
    assert any("Marijkesluis" in c.get("name", "") for c in lookup["49924"]["chambers"])
    assert any("Algerasluis" in c.get("name", "") for c in lookup["39670"]["chambers"])
    assert any(
        "Brienenoordbrug" in o.get("name", "") for o in lookup["39904"]["openings"]
    )
