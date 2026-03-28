import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
from fis.lock.core import group_complexes
from fis import utils


def test_vlagtweddersluis_complex_grouping():
    """
    Test case for Vlagtweddersluis (Lock 1738).
    Ensures that Lock, Chamber, and Bridge are correctly grouped into a complex
    sharing the same RelatedBuildingComplexName, even without DISK data.
    """
    # 1. Mock FIS Lock
    locks = pd.DataFrame(
        [
            {
                "Id": "1738",
                "Name": "Vlagtweddersluis",
                "RelatedBuildingComplexName": "Vlagtweddersluis",
                "FairwayId": "3749",
                "FairwaySectionId": "33795",
                "IsrsId": "1001",
                "Geometry": Point(7.1393, 53.0241),
                "OperatingTimesId": "5001",
            }
        ]
    )

    # 2. Mock FIS Chamber
    chambers = pd.DataFrame(
        [
            {
                "Id": "18542",
                "ParentId": "1738",
                "Name": "Vlagtweddersluis",
                "Length": 65.0,
                "Width": 7.5,
                "Geometry": Point(7.1393, 53.0241),
                "OperatingTimesId": "5001",
            }
        ]
    )

    # 3. Mock FIS Bridge
    bridges = pd.DataFrame(
        [
            {
                "Id": "26743",
                "Name": "Vlagtwedderbrug",
                "RelatedBuildingComplexName": "Vlagtweddersluis",
                "Geometry": Point(7.1393, 53.0242),
            }
        ]
    )

    # 4. Mock FIS Opening
    openings = pd.DataFrame(
        [
            {
                "Id": "99001",
                "ParentId": "26743",
                "Name": "Vlagtwedderbrug Opening",
                "OperatingTimesId": "5001",
            }
        ]
    )

    # 5. Other required datasets (using strings for IDs to match normalization)
    isrs = gpd.GeoDataFrame(
        [
            {
                "id": "1001",
                "code": "NLWSC000170941100188",
                "geometry": Point(7.1393, 53.0241),
            }
        ],
        crs="EPSG:4326",
    )

    sections = gpd.GeoDataFrame(
        [
            {
                "id": "33795",
                "name": "Vaarwegvak Vlagtwedde",
                "fairway_id": "3749",
                "geometry": LineString([(7.1393, 53.0200), (7.1393, 53.0300)]),
            }
        ],
        crs="EPSG:4326",
    )

    fairways = gpd.GeoDataFrame(
        [
            {
                "id": "3749",
                "name": "Ruiten Aa Kanaal",
                "geometry": LineString([(7.1393, 53.0000), (7.1393, 53.1000)]),
            }
        ],
        crs="EPSG:4326",
    )

    operatingtimes = pd.DataFrame(
        [
            {
                "id": "5001",
                "normal_schedules": [],
                "holiday_schedules": [],
                "exception_schedules": [],
            }
        ]
    )

    # RIS Index mock
    ris_df = pd.DataFrame(
        [
            {
                "isrs_code": "NLWSC000170941100188",
                "name": "Vlagtweddersluis",
                "function": "Lock",
            }
        ]
    )

    # DISK data (EMPTY to simulate Vlagtweddersluis not being in DISK)
    disk_locks = gpd.GeoDataFrame(columns=["id", "geometry"], crs="EPSG:4326")
    disk_bridges = gpd.GeoDataFrame(columns=["id", "geometry"], crs="EPSG:4326")

    # Normalize FIS inputs as load_data would
    schema = utils.load_schema()
    locks = utils.normalize_attributes(locks, "locks", schema)
    chambers = utils.normalize_attributes(chambers, "chambers", schema)
    bridges = utils.normalize_attributes(bridges, "bridges", schema)
    openings = utils.normalize_attributes(openings, "openings", schema)
    operatingtimes = utils.normalize_attributes(
        operatingtimes, "operatingtimes", schema
    )

    data = {
        "locks": locks,
        "chambers": chambers,
        "subchambers": pd.DataFrame(columns=["id", "parent_id"]),
        "isrs": isrs,
        "ris_df": ris_df,
        "fairways": fairways,
        "berths": gpd.GeoDataFrame(columns=["id", "geometry"], crs="EPSG:4326"),
        "sections": sections,
        "disk_locks": disk_locks,
        "disk_bridges": disk_bridges,
        "operatingtimes": operatingtimes,
        "bridges": bridges,
        "openings": openings,
    }

    # ACT
    complexes = group_complexes(data)

    # ASSERT
    assert len(complexes) == 1
    c = complexes[0]

    assert c["name"] == "Vlagtweddersluis"
    assert c["isrs_code"] == "NLWSC000170941100188"
    assert c["fairway_name"] == "Ruiten Aa Kanaal"

    # Check linked components
    assert len(c["locks"]) == 1
    assert len(c["locks"][0]["chambers"]) == 1
    assert c["locks"][0]["chambers"][0]["id"] == 18542

    # Check openings linked via bridge complex name
    assert len(c["openings"]) == 1
    assert c["openings"][0]["id"] == 99001

    # Verify it handled DISK absence gracefully
    assert len(c["disk_locks"]) == 0
    assert len(c["disk_bridges"]) == 0
