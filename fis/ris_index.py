import pandas as pd
import pathlib


def load_ris_index(path: pathlib.Path) -> pd.DataFrame:
    """
    Load and parse the RIS Index Excel file.

    Args:
        path: Path to the RisIndexNL.xlsx file

    Returns:
        pd.DataFrame: DataFrame with columns ['isrs_code', 'name', 'function']
    """
    # Load the Excel file, strictly assuming header is on row 2 (index 1)
    # The file has a complex header structure, row 1 contains the actual column names
    df = pd.read_excel(path, sheet_name="RIS Index", header=1)

    # Clean column names: replace newlines with spaces and strip whitespace
    df.columns = [str(c).replace("\n", " ").strip() for c in df.columns]

    # Map original column names to our internal schema
    # Based on inspection:
    # 'ISRS Location Code' -> isrs_code
    # 'Object name' -> name
    # 'Function' -> function

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
        # Also could check if it starts with valid country codes if we wanted to be strict
        # df = df[df['isrs_code'].str.match(r'^[A-Z]{2}')]

    # Ensure isrs_code is unique if needed, but for now just unique index
    # df = df.drop_duplicates(subset=['isrs_code'])

    # Return relevant columns
    return df[list(column_map.values())]


if __name__ == "__main__":
    # Test execution
    base_dir = pathlib.Path(__file__).parent.parent
    path = base_dir / "fis-export" / "RisIndexNL.xlsx"
    if path.exists():
        df = load_ris_index(path)
        print(f"Loaded {len(df)} records")
        print(df.head())
    else:
        print(f"File not found at {path}")
