import geopandas as gpd
from fis.graph.bivas import load_bivas_network, normalize_code, has_km_overlap


def test_case_6668():
    bivas_db = "reference/Bivas.5.10.1.sqlite"
    fis_edges_path = "output/fis-enriched/edges.geoparquet"

    # 1. Load BIVAS data for 6668
    _, bivas_arcs = load_bivas_network(bivas_db)
    bivas_6668 = bivas_arcs[bivas_arcs["ID"].astype(str) == "6668"].iloc[0]

    # 2. Load FIS data for 7070534
    fis_edges = gpd.read_parquet(fis_edges_path)
    fis_7070534 = fis_edges[fis_edges["Id"].astype(str) == "7070534"].iloc[0]

    print(
        f"BIVAS 6668: TrajectCode={bivas_6668['TrajectCode']}, Range={bivas_6668['StartKilometer']}-{bivas_6668['EndKilometer']}"
    )
    print(
        f"FIS 7070534: route_code={fis_7070534['route_code']}, Range={fis_7070534['RouteKmBegin']}-{fis_7070534['RouteKmEnd']}"
    )

    # 3. Test Normalization
    b_norm = normalize_code(bivas_6668["TrajectCode"])
    f_norm = normalize_code(fis_7070534["route_code"])
    print(f"Normalized: BIVAS={b_norm}, FIS={f_norm}")

    # 4. Test KM Overlap
    # Create a dummy row combining both for the overlap function
    combined_row = {**fis_7070534.to_dict(), **bivas_6668.to_dict()}
    overlap = has_km_overlap(combined_row)
    print(f"KM Overlap Result: {overlap}")

    # Final check
    if b_norm == f_norm and overlap:
        print("SUCCESS: Both ways match confirmed.")
    else:
        print("FAILURE: Match conditions not met.")


if __name__ == "__main__":
    test_case_6668()
