import geopandas as gpd
from shapely.geometry import Point, LineString
from fis.dropins.euris_io import load_dropins_with_explicit_linking


def test_load_dropins_with_explicit_linking_mapping(tmp_path):
    # Setup mock EURIS data structure
    export_dir = tmp_path / "euris-export"
    export_dir.mkdir()

    # Create mock LockComplex (Point)
    lock_complex = gpd.GeoDataFrame(
        [
            {
                "locode": "TEST_LC_1",
                "objectname": "Test Lock Complex",
                "sectionref": "SEC_1",
                "geometry": Point(5.0, 52.0),
            }
        ],
        crs="EPSG:4326",
    )
    lock_complex.to_file(
        export_dir / "LockComplex_XX_20260101.geojson", driver="GeoJSON"
    )

    # Create mock LockChamber (Point) - units in cm
    lock_chamber = gpd.GeoDataFrame(
        [
            {
                "locode": "TEST_CH_1",
                "slslocode": "TEST_LC_1",
                "objectname": "Test Chamber 1",
                "mlengthcm": 10000,  # 100m
                "mwidthcm": 1200,  # 12m
                "mheightcm": 500,  # 5m
                "geometry": Point(5.001, 52.001),
            }
        ],
        crs="EPSG:4326",
    )
    lock_chamber.to_file(
        export_dir / "LockChamber_XX_20260101.geojson", driver="GeoJSON"
    )

    # Create mock FairwaySection
    fairway_section = gpd.GeoDataFrame(
        [
            {
                "code": "SEC_1",
                "name": "Test Fairway",
                "geometry": LineString([(4.9, 52.0), (5.1, 52.0)]),
            }
        ],
        crs="EPSG:4326",
    )
    fairway_section.to_file(
        export_dir / "FairwaySection_XX_20260101.geojson", driver="GeoJSON"
    )

    # Run loader
    locks, bridges, terminals, berths, sections, _ = load_dropins_with_explicit_linking(
        export_dir
    )

    # Assertions
    assert len(locks) == 1
    l_complex = locks[0]
    assert l_complex["id"] == "TEST_LC_1"
    assert l_complex["sections"][0]["id"] == "SEC_1"
    assert l_complex["topological_anchor"] == Point(5.0, 52.0).wkt

    assert len(l_complex["locks"][0]["chambers"]) == 1
    chamber = l_complex["locks"][0]["chambers"][0]
    assert chamber["id"] == "TEST_CH_1"
    assert chamber["dim_length"] == 100.0
    assert chamber["dim_width"] == 12.0
    assert chamber["dim_height"] == 5.0
    assert chamber["topological_anchor"] == Point(5.001, 52.001).wkt

    assert len(sections) == 1
    assert sections.iloc[0]["id"] == "SEC_1"


def test_load_dropins_with_explicit_linking_bridge_grouping(tmp_path):
    export_dir = tmp_path / "euris-export"
    export_dir.mkdir()

    # Create mock BridgeArea
    bridge_area = gpd.GeoDataFrame(
        [
            {
                "locode": "TEST_BR_1",
                "objectname": "Test Bridge",
                "sectionref": "SEC_1",
                "geometry": Point(5.05, 52.0),
            }
        ],
        crs="EPSG:4326",
    )
    bridge_area.to_file(export_dir / "BridgeArea_XX_20260101.geojson", driver="GeoJSON")

    # Create mock BridgeOpening
    bridge_opening = gpd.GeoDataFrame(
        [
            {
                "locode": "TEST_OP_1",
                "brgalocode": "TEST_BR_1",
                "mlengthcm": 5000,
                "geometry": Point(5.051, 52.001),
            }
        ],
        crs="EPSG:4326",
    )
    bridge_opening.to_file(
        export_dir / "BridgeOpening_XX_20260101.geojson", driver="GeoJSON"
    )

    # Run loader
    _, bridges, _, _, _, _ = load_dropins_with_explicit_linking(export_dir)

    assert len(bridges) == 1
    br = bridges[0]
    assert br["id"] == "TEST_BR_1"
    assert len(br["openings"]) == 1
    assert br["openings"][0]["id"] == "TEST_OP_1"
    assert br["openings"][0]["dim_length"] == 50.0
    assert br["openings"][0]["topological_anchor"] == Point(5.051, 52.001).wkt
