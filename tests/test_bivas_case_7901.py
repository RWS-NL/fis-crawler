from fis.graph.bivas import normalize_code, has_km_overlap


def test_case_7901_30984594():
    # 1. Mock the data found in previous diagnostic
    # BIVAS Arc 7901
    bivas_data = {
        "ID": "7901",
        "TrajectCode": "121",
        "StartKilometer": 14.42,
        "EndKilometer": 17.24,
    }

    # FIS Edge 30984594
    fis_data = {
        "Id": "30984594",
        "route_code": "121",
        "RouteKmBegin": 104.598,
        "RouteKmEnd": 116.71,
    }

    # Combined route max KM (determined from full data previously)
    # The diagnostic showed FIS has ~116km, so max is around that.
    route_max = 121.0

    print(f"Testing BIVAS {bivas_data['ID']} vs FIS {fis_data['Id']} (Route 121)")

    # 2. Test Normalization
    b_norm = normalize_code(bivas_data["TrajectCode"])
    f_norm = normalize_code(fis_data["route_code"])
    print(f"Normalized: BIVAS={b_norm}, FIS={f_norm}")

    # 3. Test KM Overlap with Inversion
    row = {**fis_data, **bivas_data}
    overlap = has_km_overlap(row, route_max_km=route_max)
    print(f"KM Overlap (with max_km={route_max}): {overlap}")

    # 4. Final check
    if b_norm == f_norm and overlap:
        print("SUCCESS: Inverse KM match confirmed.")
    else:
        print("FAILURE: Match conditions not met.")


if __name__ == "__main__":
    test_case_7901_30984594()
