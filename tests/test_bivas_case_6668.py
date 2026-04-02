import geopandas as gpd
import os
import pytest
from fis.graph.bivas import load_bivas_network, normalize_code, has_km_overlap


def test_case_6668():
    bivas_db = "reference/Bivas.5.10.1.sqlite"
    fis_edges_path = "output/fis-enriched/edges.geoparquet"

    # Skip if local artifacts are missing (e.g. in CI)
    if not os.path.exists(bivas_db) or not os.path.exists(fis_edges_path):
        pytest.skip("Local BIVAS/FIS artifacts missing, skipping diagnostic test.")

    # 1. Load BIVAS data for 6668
    _, bivas_arcs = load_bivas_network(bivas_db)
    bivas_6668_rows = bivas_arcs[bivas_arcs["ID"].astype(str) == "6668"]
    assert not bivas_6668_rows.empty, "BIVAS arc 6668 not found in database."
    bivas_6668 = bivas_6668_rows.iloc[0]

    # 2. Load FIS data for 7070534
    fis_edges = gpd.read_parquet(fis_edges_path)
    fis_7070534_rows = fis_edges[fis_edges["Id"].astype(str) == "7070534"]
    assert not fis_7070534_rows.empty, "FIS edge 7070534 not found in parquet."
    fis_7070534 = fis_7070534_rows.iloc[0]

    # 3. Test Normalization
    b_norm = normalize_code(bivas_6668["TrajectCode"])
    f_norm = normalize_code(fis_7070534["route_code"])
    assert b_norm == f_norm, f"Normalized codes differ: BIVAS={b_norm}, FIS={f_norm}"

    # 4. Test KM Overlap
    combined_row = {**fis_7070534.to_dict(), **bivas_6668.to_dict()}
    overlap = has_km_overlap(combined_row)
    assert overlap, "Expected kilometer ranges to overlap, but they do not."


if __name__ == "__main__":
    # Allow manual execution
    try:
        test_case_6668()
        print("SUCCESS: Both ways match confirmed.")
    except pytest.skip.Exception as e:
        print(f"SKIPPED: {e}")
    except AssertionError as e:
        print(f"FAILURE: {e}")
