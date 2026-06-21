import unittest
import pathlib
import tempfile
import pandas as pd
import zipfile
from fis.ivs.process import (
    get_zip_year_month,
    read_and_normalize_zip,
    save_year_month,
    STANDARD_COLS,
)


class TestIvsProcess(unittest.TestCase):
    def test_get_zip_year_month_patterns(self):
        # 1. Standard pattern
        p1 = pathlib.Path("/tmp/IVS_weekmonitor_15JAN2024_20240115_123456.zip")
        self.assertEqual(get_zip_year_month(p1), (2024, 1))

        p2 = pathlib.Path("/tmp/IVS_weekmonitor_01DEC2023_20231201_223344.zip")
        self.assertEqual(get_zip_year_month(p2), (2023, 12))

        # 2. Alternative pattern
        p3 = pathlib.Path("/tmp/IVS_weekmonitor_2024_20240510_151617.zip")
        self.assertEqual(get_zip_year_month(p3), (2024, 5))

    def test_get_zip_year_month_csv_fallback(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir)
            zip_file_path = tmp_path / "unknown_filename.zip"

            # Write a zip file containing a dummy csv with year/month headers
            with zipfile.ZipFile(zip_file_path, "w") as zf:
                csv_data = (
                    "Jaar;Maand;Weeknr;v05_06_begindt_evenement\n2022;8;32;2022-08-10\n"
                )
                zf.writestr("data.csv", csv_data)

            ym = get_zip_year_month(zip_file_path)
            self.assertEqual(ym, (2022, 8))

    def test_read_and_normalize_zip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir)
            zip_file_path = tmp_path / "IVS_weekmonitor_15JAN2024_20240115_123456.zip"

            with zipfile.ZipFile(zip_file_path, "w") as zf:
                # CSV with lowercase and uppercase columns, mixed data
                csv_data = (
                    "Jaar;Maand;Weeknr;v05_06_begindt_evenement_iso;v38_vervoerd_gewicht;sk_code\n"
                    "2024;1;2;2024-01-10T12:00:00Z;125.5;M8\n"
                )
                zf.writestr("IVS_weekmonitor_15JAN2024_20240115_123456.csv", csv_data)

            # Run read/normalize (this runs under dask.delayed, so we compute it)
            delayed_df = read_and_normalize_zip(zip_file_path)
            df = delayed_df.compute()

            # Check shape and columns
            self.assertEqual(list(df.columns), STANDARD_COLS)
            self.assertEqual(len(df), 1)
            self.assertEqual(df.loc[0, "jaar"], 2024)
            self.assertEqual(df.loc[0, "maand"], 1)
            self.assertEqual(df.loc[0, "v38_vervoerd_gewicht"], 125.5)
            self.assertEqual(df.loc[0, "sk_code"], "M8")

    def test_save_year_month(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir)

            df = pd.DataFrame(
                [
                    {
                        "jaar": 2024,
                        "maand": 1,
                        "weeknr": 2,
                        "v38_vervoerd_gewicht": 125.5,
                        "sk_code": "M8",
                        "v05_06_begindt_evenement_iso": pd.Timestamp(
                            "2024-01-10T12:00:00Z"
                        ),
                    }
                ]
            )
            # Make sure all standard columns are present
            for col in STANDARD_COLS:
                if col not in df.columns:
                    df[col] = None
            df = df[STANDARD_COLS]

            # Save (runs under dask.delayed, compute it)
            success = save_year_month(2024, 1, [df], tmp_path).compute()
            self.assertTrue(success)

            # Verify file exists at year=2024/month=01/part.0.parquet
            expected_file = tmp_path / "year=2024" / "month=01" / "part.0.parquet"
            self.assertTrue(expected_file.exists())

            saved_df = pd.read_parquet(expected_file)
            self.assertEqual(len(saved_df), 1)
            self.assertEqual(saved_df.loc[0, "jaar"], 2024)
            self.assertEqual(saved_df.loc[0, "maand"], 1)


if __name__ == "__main__":
    unittest.main()
