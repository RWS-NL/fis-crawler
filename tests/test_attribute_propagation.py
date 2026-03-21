import pandas as pd
from shapely.geometry import Point
from fis import utils
from fis.core import group_complexes


def test_normalize_attributes_preserves_unknown_columns():
    """Should preserve and snake_case unknown columns."""
    df = pd.DataFrame(
        {"Id": [1], "SomeUnknownAttribute": ["Value"], "AlreadySnake": [42]}
    )

    # Using 'locks' section which only maps 'Id' -> 'id'
    schema = {"attributes": {"locks": {"Id": "id"}}}
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
        [{"Id": 1, "Name": "Lock 1", "ExtraLockAttr": "Extra", "Geometry": Point(0, 0)}]
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
            }
        ]
    )

    # Mock other inputs
    isrs = None
    ris_df = None
    fairways = None
    berths = None
    sections = None

    complexes = group_complexes(
        locks, chambers, isrs, ris_df, fairways, berths, sections
    )

    assert len(complexes) == 1
    c = complexes[0]
    assert c["id"] == 1
    assert c["extra_lock_attr"] == "Extra"

    assert len(c["locks"][0]["chambers"]) == 1
    ch = c["locks"][0]["chambers"][0]
    assert ch["id"] == 101
    assert ch["extra_chamber_attr"] == "ChamberExtra"
    assert ch["dim_length"] == 100.0
