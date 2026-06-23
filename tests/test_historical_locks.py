import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from fis.lock.core import group_complexes
from fis import utils


def test_historical_locks_ignored_in_grouping():
    """
    Verify that historical/deactivated locks present in the DISK dataset
    (like Wemeldinge or Limmel) are not matched and are ignored during grouping
    if they do not correspond to any active FIS lock complex.
    """
    # 1. Mock FIS Locks (representing an active lock Volkerak, and a mocked Wemeldinge FIS lock)
    locks = pd.DataFrame(
        [
            {
                "Id": "100",
                "Name": "Sluis Volkerak",
                "RelatedBuildingComplexName": "Volkeraksluizen",
                "FairwayId": "10",
                "FairwaySectionId": "101",
                "IsrsId": None,
                "geometry": Point(4.39, 51.69),
            },
            {
                "Id": "200",
                "Name": "Historical Sluis Wemeldinge",
                "RelatedBuildingComplexName": "Sluizencomplex Wemeldinge",
                "FairwayId": "20",
                "FairwaySectionId": "201",
                "IsrsId": None,
                "geometry": Point(4.0, 51.5),
            },
        ]
    )

    # 2. Mock FIS Chambers
    chambers = pd.DataFrame(
        [
            {
                "Id": "1001",
                "ParentId": "100",
                "Name": "Volkeraksluis oost",
                "Length": 320.0,
                "Width": 24.0,
                "geometry": Point(4.39, 51.69),
            },
            {
                "Id": "2001",
                "ParentId": "200",
                "Name": "Oostsluis",
                "Length": 150.0,
                "Width": 12.0,
                "geometry": Point(4.0, 51.5),
            },
        ]
    )

    # 3. Required empty/minimal dataframes for core inputs
    isrs = gpd.GeoDataFrame(columns=["id", "code", "geometry"], crs="EPSG:4326")
    sections = gpd.GeoDataFrame(
        columns=["id", "name", "fairway_id", "geometry"], crs="EPSG:4326"
    )
    fairways = gpd.GeoDataFrame(columns=["id", "name", "geometry"], crs="EPSG:4326")
    operatingtimes = pd.DataFrame(
        columns=["id", "normal_schedules", "holiday_schedules", "exception_schedules"]
    )
    ris_df = pd.DataFrame(columns=["isrs_code", "name", "function"])
    bridges = pd.DataFrame(
        columns=["id", "name", "related_building_complex_name", "geometry"]
    )
    openings = pd.DataFrame(columns=["id", "parent_id", "name"])

    # 4. Mock DISK Locks:
    # - One matching the active FIS lock (Volkeraksluis)
    # - One historical lock with no active FIS counterpart (Oostsluis in Wemeldinge)
    disk_locks = gpd.GeoDataFrame(
        [
            {
                "id": 1,
                "complexid": 10,
                "complex_naam": "Volkeraksluizen",
                "naam": "Volkeraksluis oost",
                "geometry": Point(4.39, 51.69),
            },
            {
                "id": 7151,
                "complexid": 20,
                "complex_naam": "Sluizencomplex Wemeldinge",
                "naam": "Oostsluis",
                "geometry": Point(4.0, 51.5),  # Far away from Volkerak
            },
        ],
        crs="EPSG:4326",
    )
    disk_bridges = gpd.GeoDataFrame(columns=["id", "geometry"], crs="EPSG:4326")

    # Normalize inputs using schema
    schema = utils.load_schema()
    locks = utils.normalize_attributes(locks, "locks", schema)
    chambers = utils.normalize_attributes(chambers, "chambers", schema)
    bridges = utils.normalize_attributes(bridges, "bridges", schema)
    openings = utils.normalize_attributes(openings, "openings", schema)
    operatingtimes = utils.normalize_attributes(
        operatingtimes, "operatingtimes", schema
    )
    sections = utils.normalize_attributes(sections, "sections", schema)
    fairways = utils.normalize_attributes(fairways, "fairways", schema)

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

    # ACT: Run grouping/schematization logic
    complexes = group_complexes(data)

    # ASSERT:
    # Two lock complexes should be created (Volkerak and Wemeldinge)
    assert len(complexes) == 2

    volkerak_c = next(c for c in complexes if c["id"] == "100")
    wemeldinge_c = next(c for c in complexes if c["id"] == "200")

    assert volkerak_c["name"] == "Sluis Volkerak"
    assert wemeldinge_c["name"] == "Historical Sluis Wemeldinge"

    # The Volkerak lock complex should have successfully matched the active Volkerak DISK lock
    assert len(volkerak_c["disk_locks"]) == 1
    assert volkerak_c["disk_locks"][0]["naam"] == "Volkeraksluis oost"
    assert volkerak_c["disk_locks"][0]["complex_naam"] == "Volkeraksluizen"

    # The historical Wemeldinge complex should NOT match the historical Wemeldinge DISK lock (7151) because it is ignored by configuration
    assert len(wemeldinge_c["disk_locks"]) == 0
