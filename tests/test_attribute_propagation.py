import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from fis import utils
from fis.core import group_complexes


def test_normalize_attributes_preserves_unknown_columns():
    """Should preserve and snake_case unknown columns."""
    df = pd.DataFrame(
        {"Id": [1], "SomeUnknownAttribute": ["Value"], "AlreadySnake": [42]}
    )

    # Using 'locks' section which only maps 'Id' -> 'id'
    schema = {
        "attributes": {"locks": {"Id": "id"}},
        "identifiers": {"columns": ["id"]},
    }
    normalized = utils.normalize_attributes(df, "locks", schema)

    assert "id" in normalized.columns
    assert "some_unknown_attribute" in normalized.columns
    assert "already_snake" in normalized.columns
    assert normalized.iloc[0]["some_unknown_attribute"] == "Value"


def test_sanitize_attrs_preserves_extra_fields():
    """Should convert row to dict and preserve non-geometry fields."""
    row = pd.Series(
        {
            "id": 1,
            "name": "Test",
            "geometry": Point(0, 0),
            "custom_field": "custom",
            "numeric_field": 123,
        }
    )
    sanitized = utils.sanitize_attrs(row)

    assert sanitized["id"] == 1
    assert sanitized["name"] == "Test"
    assert sanitized["custom_field"] == "custom"
    assert sanitized["numeric_field"] == 123
    assert "geometry" in sanitized
    assert isinstance(sanitized["geometry"], str)


def test_group_complexes_preserves_extra_attributes():
    """Integration-style test to ensure group_complexes propagates extra attributes."""
    # Use real geometry objects as GeoDataFrame expects them
    locks = pd.DataFrame(
        [
            {
                "Id": 1,
                "Name": "Lock 1",
                "ExtraLockAttr": "Extra",
                "Geometry": Point(0, 0),
                "IsrsId": None,
                "FairwayId": None,
                "SectionId": None,
                "RelatedBuildingComplexName": None,
                "OperatingTimesId": None,
            }
        ]
    )
    chambers = pd.DataFrame(
        [
            {
                "Id": 101,
                "ParentId": 1,
                "Name": "Chamber 1",
                "ExtraChamberAttr": "ChamberExtra",
                "Length": 100.0,
                "Width": 12.0,
                "Geometry": Point(0, 0),
                "OperatingTimesId": None,
            }
        ]
    )

    # Mock other inputs
    isrs = gpd.GeoDataFrame(
        columns=["id", "geometry", "code"], crs="EPSG:4326", geometry="geometry"
    )
    ris_df = pd.DataFrame(columns=["isrs_code", "name", "function"])
    fairways = gpd.GeoDataFrame(
        columns=["id", "geometry", "name"], crs="EPSG:4326", geometry="geometry"
    )
    berths = gpd.GeoDataFrame(
        columns=["id", "geometry", "name"], crs="EPSG:4326", geometry="geometry"
    )
    sections = gpd.GeoDataFrame(
        columns=["id", "geometry", "fairway_id"], crs="EPSG:4326", geometry="geometry"
    )

    complexes = group_complexes(
        locks, chambers, isrs, ris_df, fairways, berths, sections
    )

    assert len(complexes) == 1
    c = complexes[0]
    assert c["id"] == "1"
    assert c["extra_lock_attr"] == "Extra"

    assert len(c["locks"][0]["chambers"]) == 1
    ch = c["locks"][0]["chambers"][0]
    assert ch["id"] == "101"
    assert ch["extra_chamber_attr"] == "ChamberExtra"
    assert ch["dim_length"] == 100.0


def test_normalize_attributes_enforces_string_ids():
    """Should convert float IDs to clean strings."""
    df = pd.DataFrame(
        {
            "Id": [1.0, 2.0],
            "ParentId": [10.0, 20.0],
            "SomeOther": [1.5, 2.5],  # Should stay float
        }
    )
    schema = {
        "attributes": {"locks": {"Id": "id", "ParentId": "parent_id"}},
        "identifiers": {"columns": ["id", "parent_id"]},
    }
    normalized = utils.normalize_attributes(df, "locks", schema)

    # In pandas 3.0+, string-like columns might have a 'string' or 'StringDtype' instead of 'object'
    assert pd.api.types.is_string_dtype(normalized["id"])
    assert pd.api.types.is_string_dtype(normalized["parent_id"])
    assert normalized.iloc[0]["id"] == "1"
    assert normalized.iloc[0]["parent_id"] == "10"
    assert normalized["some_other"].dtype == "float64"
