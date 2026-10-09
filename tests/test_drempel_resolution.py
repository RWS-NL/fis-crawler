from scripts.lock_validation.validate_lock_dimensions import resolve_sill_nap


def test_explicit_nap_reference():
    val, src, uncertain = resolve_sill_nap(
        raw_val=-5.10,
        height_ref_str="NAP",
        fairway_ref_level=None,
        peil_side=None,
        note_text=None,
        side="bobi",
    )
    assert val == -5.10
    assert "HeightReferenceLevel=NAP" in src
    assert not uncertain


def test_positive_depth_below_streefpeil():
    val, src, uncertain = resolve_sill_nap(
        raw_val=3.65,
        height_ref_str=None,
        fairway_ref_level="KP",
        peil_side=44.05,
        note_text=None,
        side="bobi",
    )
    assert abs(val - 40.40) < 1e-4
    assert "berekend" in src
    assert not uncertain


def test_negative_value_verified_by_1m_bathymetry():
    val, src, uncertain = resolve_sill_nap(
        raw_val=-6.25,
        height_ref_str=None,
        fairway_ref_level=None,
        peil_side=None,
        note_text=None,
        side="bobi",
        measured_1m_nap=-6.32,
    )
    assert val == -6.25
    assert "bevestigd door 1m-bodemhoogte" in src
    assert not uncertain


def test_negative_value_unverified_mismatch():
    val, src, uncertain = resolve_sill_nap(
        raw_val=-6.25,
        height_ref_str=None,
        fairway_ref_level=None,
        peil_side=None,
        note_text=None,
        side="bobi",
        measured_1m_nap=-4.00,
    )
    assert val == -6.25
    assert "onzeker" in src
    assert uncertain


def test_negative_value_without_measurement():
    val, src, uncertain = resolve_sill_nap(
        raw_val=-6.25,
        height_ref_str=None,
        fairway_ref_level=None,
        peil_side=None,
        note_text=None,
        side="bobi",
        measured_1m_nap=None,
    )
    assert val == -6.25
    assert "onzeker" in src
    assert uncertain


def test_note_overridden_when_inconsistent_with_bathymetry():
    # Maasbracht case: Note mentions outdated 28.10m, but 1m measurement is ~24.53m
    # and actual FIS column 7.9m under 32.65m gives 24.75m
    note = "Drempeldiepte boven 4,55m onder KP (Drempelhoogte NAP+ 28,10m)"
    val, src, uncertain = resolve_sill_nap(
        raw_val=7.9,
        height_ref_str=None,
        fairway_ref_level="KP",
        peil_side=32.65,
        note_text=note,
        side="bobi",
        measured_1m_nap=24.53,
    )
    assert abs(val - 24.75) < 1e-4
    assert not uncertain
    assert "Note 28.10m afwijkend" in src
