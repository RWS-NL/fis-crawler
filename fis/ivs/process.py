import logging
import pathlib
import re
import pandas as pd
import dask
from dask.distributed import Client, LocalCluster

logger = logging.getLogger("fis.ivs.process")

# Month abbreviation mapping to integer
MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

# Standard schema definition (all lowercase)
STANDARD_COLS = [
    "jaarmaand",
    "jaar",
    "maand",
    "weeknr",
    "v05_06_begindt_evenement_iso",
    "v05_06_begindt_evenement",
    "unlo_herkomst",
    "unlo_bestemming",
    "v15_1_scheepstype_rws",
    "sk_code",
    "v18_laadvermogen",
    "v28_beladingscode",
    "v38_vervoerd_gewicht",
    "v30_4_containers_teu_s",
    "nstr_nw",
    "nst2007_nw",
]

DTYPES = {
    "jaarmaand": "int64",
    "jaar": "int64",
    "maand": "int64",
    "weeknr": "int64",
    "v05_06_begindt_evenement_iso": "datetime64[us, UTC]",
    "v05_06_begindt_evenement": "object",
    "unlo_herkomst": "object",
    "unlo_bestemming": "object",
    "v15_1_scheepstype_rws": "object",
    "sk_code": "object",
    "v18_laadvermogen": "float64",
    "v28_beladingscode": "float64",
    "v38_vervoerd_gewicht": "float64",
    "v30_4_containers_teu_s": "float64",
    "nstr_nw": "float64",
    "nst2007_nw": "float64",
}


def get_zip_year_month(zf_path: pathlib.Path):
    """Determine the year and month of a ZIP file using its filename or CSV header fallback."""
    # 1. Standard pattern: IVS_weekmonitor_DDMMMYYYY_YYYYMMDD_HHMMSS.zip
    match = re.search(r"IVS_weekmonitor_\d{2}([A-Z]{3})(\d{4})", zf_path.name)
    if match:
        m_str = match.group(1)
        y = int(match.group(2))
        m = MONTH_MAP.get(m_str)
        if m:
            return y, m

    # 2. Alternative pattern: IVS_weekmonitor_YYYY_YYYYMMDD_HHMMSS.zip
    match2 = re.search(
        r"IVS_weekmonitor_(\d{4})_(\d{4})(\d{2})", zf_path.name
    )
    if match2:
        y = int(match2.group(1))
        # Default fallback month to the first digit of export date
        m = int(match2.group(3))
        return y, m

    # 3. Fallback: read first row from the file to extract year/month
    try:
        df = pd.read_csv(zf_path, compression="zip", sep=";", nrows=2)
        df.columns = [c.lower() for c in df.columns]
        if "jaar" in df.columns and "maand" in df.columns:
            return int(df["jaar"].iloc[0]), int(df["maand"].iloc[0])
    except Exception as e:
        logger.warning(
            f"Fallback reading failed for {zf_path.name}: {e}"
        )

    return None


@dask.delayed
def read_and_normalize_zip(zip_path):
    """Read a weekmonitor ZIP file, normalize columns and strictly cast datatypes."""
    try:
        df = pd.read_csv(zip_path, compression="zip", sep=";")
        df.columns = [c.lower() for c in df.columns]

        # Ensure all standard columns are present
        for col in STANDARD_COLS:
            if col not in df.columns:
                df[col] = None

        df = df[STANDARD_COLS]

        # Strict type casting
        for col in STANDARD_COLS:
            dt = DTYPES[col]
            if col == "v05_06_begindt_evenement_iso":
                df[col] = pd.to_datetime(
                    df[col], format="ISO8601", utc=True
                ).astype("datetime64[us, UTC]")
            elif dt == "int64":
                df[col] = (
                    pd.to_numeric(df[col], errors="coerce")
                    .fillna(0)
                    .astype("int64")
                )
            elif dt == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(
                    "float64"
                )
            else:
                df[col] = df[col].astype("object")

        return df

    except Exception as e:
        logger.error(f"Error processing ZIP file {zip_path.name}: {e}")
        # Return empty dataframe with correct types
        empty = pd.DataFrame(columns=STANDARD_COLS)
        for col, dt in DTYPES.items():
            empty[col] = empty[col].astype(dt)
        return empty


@dask.delayed
def save_year_month(year, month, dfs, output_dir):
    """Deduplicates list of dataframes and saves them to partitioned Parquet file under year=YYYY/month=MM/part.0.parquet."""
    logger.info(
        f"Dask task starting combination/saving for year {year}, month {month}..."
    )
    valid_dfs = [df for df in dfs if df is not None and not df.empty]
    if not valid_dfs:
        logger.warning(
            f"No valid data frames processed for year {year}, month {month}."
        )
        return False

    combined = pd.concat(valid_dfs, ignore_index=True)
    dedup = combined.drop_duplicates()

    # Hive style directories: year=YYYY/month=MM
    year_month_dir = output_dir / f"year={year}" / f"month={month}"
    year_month_dir.mkdir(parents=True, exist_ok=True)
    part_file = year_month_dir / "part.0.parquet"

    logger.info(
        f"Writing {len(dedup)} rows for year {year}, month {month} to {part_file}..."
    )
    dedup.to_parquet(str(part_file), engine="pyarrow")
    logger.info(
        f"Dask task finished saving year {year}, month {month} to {part_file}."
    )
    return True


def process_ivs_data(downloads_dir: pathlib.Path, output_dir: pathlib.Path):
    """Processes downloaded IVS weekmonitor ZIP files into year and month partitioned Parquet files using a single Dask graph."""
    downloads_dir = pathlib.Path(downloads_dir)
    output_dir = pathlib.Path(output_dir)

    zip_files = list(downloads_dir.glob("*.zip"))
    if not zip_files:
        logger.warning(f"No ZIP files found in {downloads_dir}")
        return

    logger.info(
        f"Found {len(zip_files)} ZIP files in {downloads_dir}. Grouping by year and month..."
    )

    # Group files by year and month
    files_by_year_month = {}
    for zf in zip_files:
        ym = get_zip_year_month(zf)
        if ym:
            files_by_year_month.setdefault(ym, []).append(zf)
        else:
            logger.warning(f"Could not parse year/month for file: {zf.name}")

    # Spin up Local Dask Cluster using default settings
    logger.info("Starting Dask LocalCluster...")
    cluster = LocalCluster()
    client = Client(cluster)

    logger.info(f"Dask cluster initialized successfully.")
    logger.info(f"Dask dashboard is available at: {client.dashboard_link}")

    try:
        tasks = []
        for ym in sorted(files_by_year_month.keys()):
            year, month = ym
            ym_files = files_by_year_month[ym]
            logger.info(
                f"Adding year {year}, month {month} ({len(ym_files)} files) to Dask graph..."
            )

            # 1. Delayed read for each file in this month partition
            delayed_dfs = [read_and_normalize_zip(zf) for zf in ym_files]

            # 2. Delayed combine and save for the month partition
            save_task = save_year_month(year, month, delayed_dfs, output_dir)
            tasks.append(save_task)

        logger.info(
            f"Dask graph constructed with {len(tasks)} month partition tasks and {len(zip_files)} file read tasks."
        )
        logger.info("Computing all tasks concurrently in Dask...")

        # Compute all tasks together in a single compute call
        results = dask.compute(*tasks)
        logger.info(f"All processing completed. Task results: {results}")

    finally:
        client.close()
        cluster.close()
        logger.info("Dask client and cluster shut down.")

    logger.info("All IVS data processing completed successfully.")
