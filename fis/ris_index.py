import pandas as pd
import pathlib


def load_ris_index(path: pathlib.Path) -> pd.DataFrame:
    """
    Load and parse the RIS Index Excel file.

    Args:
        path: Path to the RisIndexNL.xlsx file

    Returns:
        DataFrame with standardized columns [isrs_code, name, function].
    """
    # The RIS Index from vaarweginformatie.nl has header info in row 0
    # Data starts at row 1 (header) and actual records at row 2
    df = pd.read_excel(path, sheet_name="RIS Index", header=1)

    column_map = {
        "ISRS Location Code": "isrs_code",
        "Object name": "name",
        "Function": "function",
        "UN Location code (3 digits, alphanumeric)": "un_loc_code",
        "Fairway section code (5 digits alphanumeric)": "fairway_code",
        "Object Reference Code (5 digits alphanumeric)": "object_code",
        "Fairway Hectometre (5 digits numeric)": "hectometer",
    }

    # Verify expected columns exist
    missing_cols = [col for col in column_map.keys() if col not in df.columns]
    if missing_cols:
        # Fallback logic or error if strict columns are missing
        # For now, let's list available columns to help debugging if this fails
        available = list(df.columns)
        raise ValueError(
            f"Missing expected columns: {missing_cols}. Available: {available}"
        )

    # Rename and select
    df = df.rename(columns=column_map)

    # Filter out metadata/search rows (often found in these exports)
    # Real ISRS codes are typically 20 chars long.
    # We'll drop rows where isrs_code is 'auto' or the concatenation row.
    if "isrs_code" in df.columns:
        # Convert to string to avoid type errors
        df["isrs_code"] = df["isrs_code"].astype(str)
        # Filter for codes that look like valid ISRS (e.g. 20 chars, distinct from 'auto')
        # A simple check is length > 10.
        df = df[df["isrs_code"].str.len() > 10]

    # Return relevant columns
    return df[list(column_map.values())]
