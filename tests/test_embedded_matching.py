import geopandas as gpd
from shapely.geometry import Point
from unittest.mock import patch
from fis.dropins.core import _identify_embedded_structures


@patch("fis.lock.graph.build_chambers_gdf")
@patch("fis.bridge.graph.build_openings_gdf")
def test_identify_embedded_structures_one_to_one(
    mock_build_openings, mock_build_chambers
):
    """
    Ensure that one chamber is matched to at most one bridge opening,
    even if multiple openings are within range.
    """
    # Create two openings close to a single chamber
    # Chamber at (0, 0)
    chambers_gdf = gpd.GeoDataFrame(
        [{"id": "ch1", "name": "Main Lock", "geometry": Point(0, 0)}], crs="EPSG:28992"
    )  # Using projected CRS for distance

    # Opening 1 at (10, 0) - Score high
    # Opening 2 at (20, 0) - Score also high
    openings_gdf = gpd.GeoDataFrame(
        [
            {"id": "op1", "name": "Main Bridge", "geometry": Point(10, 0)},
            {"id": "op2", "name": "Main Bridge", "geometry": Point(20, 0)},
        ],
        crs="EPSG:28992",
    )

    mock_build_chambers.return_value = chambers_gdf
    mock_build_openings.return_value = openings_gdf

    # We need to mock the CRS transformation or just use the same CRS
    # _identify_embedded_structures uses settings.PROJECTED_CRS
    from fis import settings

    chambers_gdf = chambers_gdf.to_crs(settings.PROJECTED_CRS)
    openings_gdf = openings_gdf.to_crs(settings.PROJECTED_CRS)
    mock_build_chambers.return_value = chambers_gdf
    mock_build_openings.return_value = openings_gdf

    matches = _identify_embedded_structures([], [])

    # Should only have one match for ch1
    # Since op1 is closer, it should win
    assert len(matches) == 1
    assert "op1" in matches
    assert matches["op1"]["ch_id"] == "ch1"
    assert "op2" not in matches


@patch("fis.lock.graph.build_chambers_gdf")
@patch("fis.bridge.graph.build_openings_gdf")
def test_identify_embedded_structures_global_greedy(
    mock_build_openings, mock_build_chambers
):
    """
    Test that the global greedy matching picks the best overall matches.
    """
    # op1 is close to ch1 and ch2.
    # op2 is close to ch1.

    # ch1 at (0,0), ch2 at (100, 0)
    chambers_gdf = gpd.GeoDataFrame(
        [
            {"id": "ch1", "name": "Lock A", "geometry": Point(0, 0)},
            {"id": "ch2", "name": "Lock B", "geometry": Point(100, 0)},
        ],
        crs="EPSG:28992",
    )

    # op1 at (10, 0) -> Very close to ch1, reasonably close to ch2
    # op2 at (5, 0) -> Even closer to ch1
    openings_gdf = gpd.GeoDataFrame(
        [
            {"id": "op1", "name": "Bridge 1", "geometry": Point(10, 0)},
            {"id": "op2", "name": "Bridge 2", "geometry": Point(5, 0)},
        ],
        crs="EPSG:28992",
    )

    from fis import settings

    mock_build_chambers.return_value = chambers_gdf.to_crs(settings.PROJECTED_CRS)
    mock_build_openings.return_value = openings_gdf.to_crs(settings.PROJECTED_CRS)

    matches = _identify_embedded_structures([], [])

    # op2 is closest to ch1 (dist 5). op1 is then forced to match with ch2 (dist 90)
    # OR if op1 matched ch1 (dist 10), op2 would have nothing.
    # Global greedy: (op2, ch1, dist 5) is best match. Then (op1, ch2, dist 90) is next best.

    assert len(matches) == 2
    assert matches["op2"]["ch_id"] == "ch1"
    assert matches["op1"]["ch_id"] == "ch2"


@patch("fis.lock.graph.build_chambers_gdf")
@patch("fis.bridge.graph.build_openings_gdf")
def test_identify_embedded_structures_score_wins(
    mock_build_openings, mock_build_chambers
):
    """
    Test that semantic score wins over pure spatial distance.
    """
    # ch_west at (0,0), ch_east at (100, 0)
    chambers_gdf = gpd.GeoDataFrame(
        [
            {"id": "ch_west", "name": "Sluis west", "geometry": Point(0, 0)},
            {"id": "ch_east", "name": "Sluis oost", "geometry": Point(100, 0)},
        ],
        crs="EPSG:28992",
    )

    # op_east is at (10, 0)
    # Distance to ch_west is 10. Distance to ch_east is 90.
    # But op_east has 'oost' in name, so it should match ch_east despite distance.
    openings_gdf = gpd.GeoDataFrame(
        [{"id": "op_east", "name": "Brug oost", "geometry": Point(10, 0)}],
        crs="EPSG:28992",
    )

    from fis import settings

    mock_build_chambers.return_value = chambers_gdf.to_crs(settings.PROJECTED_CRS)
    mock_build_openings.return_value = openings_gdf.to_crs(settings.PROJECTED_CRS)

    matches = _identify_embedded_structures([], [])

    # 'oost' match gives +10 score.
    # Dist 10 to West gives score 5 - 10/100 = 4.9. Total 4.9.
    # Dist 90 to East gives score 5 - 90/100 = 4.1. Total 10 + 4.1 = 14.1.
    # So ch_east should win.

    assert len(matches) == 1
    assert matches["op_east"]["ch_id"] == "ch_east"
