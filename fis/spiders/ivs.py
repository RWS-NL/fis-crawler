import os
import pathlib
import re
import scrapy
import dask
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client, LocalCluster

class IvsSpider(scrapy.Spider):
    name = "ivs"
    allowed_domains = ["downloads.rijkswaterstaatdata.nl"]
    start_urls = ["https://downloads.rijkswaterstaatdata.nl/scheepvaart/goederenvervoer/archief/"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 16,
        "ITEM_PIPELINES": {
            "fis.pipelines.IvsFilesPipeline": 1,
        },
        "FILES_STORE": "/scratch-shared/fbaart/data/ivs/downloads",
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(IvsSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(
            spider.spider_closed, signal=scrapy.signals.spider_closed
        )
        return spider

    def parse(self, response):
        # Find all zip links in the directory listing
        links = response.css("a::attr(href)").getall()
        zip_links = [link for link in links if link.startswith("IVS_weekmonitor_") and link.endswith(".zip")]
        
        self.logger.info(f"Found {len(zip_links)} IVS weekmonitor zip files to download.")
        
        for zip_link in zip_links:
            # Reconstruct absolute URL
            file_url = response.urljoin(zip_link)
            yield {
                "file_urls": [file_url],
                "filename": zip_link,
            }

    def spider_closed(self, spider):
        self.logger.info("IVS Spider closed. Starting post-processing with Dask LocalCluster...")
        
        downloads_dir = pathlib.Path("/scratch-shared/fbaart/data/ivs/downloads")
        partitioned_dir = pathlib.Path("/scratch-shared/fbaart/data/ivs/partitioned")
        partitioned_dir.mkdir(parents=True, exist_ok=True)
        
        zip_files = list(downloads_dir.glob("*.zip"))
        if not zip_files:
            self.logger.warning("No IVS ZIP files found for processing.")
            return
            
        self.logger.info(f"Found {len(zip_files)} ZIP files. Grouping by year...")
        
        # Group files by year to process them in smaller memory chunks
        files_by_year = {}
        for zf in zip_files:
            match = re.search(r'IVS_weekmonitor_\d{2}[A-Z]{3}(\d{4})', zf.name)
            if match:
                y = int(match.group(1))
                files_by_year.setdefault(y, []).append(zf)
            else:
                self.logger.warning(f"Could not parse year from filename: {zf.name}")
                
        # Standard schema definition (all lowercase)
        standard_cols = [
            "jaarmaand", "jaar", "maand", "weeknr", 
            "v05_06_begindt_evenement_iso", "v05_06_begindt_evenement", 
            "unlo_herkomst", "unlo_bestemming", 
            "v15_1_scheepstype_rws", "sk_code", 
            "v18_laadvermogen", "v28_beladingscode", 
            "v38_vervoerd_gewicht", "v30_4_containers_teu_s", 
            "nstr_nw", "nst2007_nw"
        ]

        # Determine the datetime dtype dynamically
        date_dtype = pd.to_datetime(pd.Series(["2021-02-28T23:00:00+01:00"]), format="ISO8601", utc=True).dtype

        dtypes = {
            "jaarmaand": "int64",
            "jaar": "int64",
            "maand": "int64",
            "weeknr": "int64",
            "v05_06_begindt_evenement_iso": date_dtype,
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
            "nst2007_nw": "float64"
        }

        # Create metadata template matching expected schema types for Dask
        meta_df = pd.DataFrame()
        for col, dt in dtypes.items():
            meta_df[col] = pd.Series(dtype=dt)

        @dask.delayed
        def read_and_normalize(path):
            try:
                # Read directly from zip using pandas
                df = pd.read_csv(path, compression='zip', sep=';')
                
                # Normalize column names to lowercase
                df.columns = [c.lower() for c in df.columns]
                
                # Ensure all standard columns are present
                for col in standard_cols:
                    if col not in df.columns:
                        df[col] = None
                        
                # Reorder to standard columns
                df = df[standard_cols]
                
                # Convert types according to dtypes schema
                for col in standard_cols:
                    dt = dtypes[col]
                    if col == "v05_06_begindt_evenement_iso":
                        df[col] = pd.to_datetime(df[col], format="ISO8601", utc=True).astype("datetime64[us, UTC]")
                    elif dt == "int64":
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype("int64")
                    elif dt == "float64":
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype("float64")
                    else:
                        df[col] = df[col].astype("object")
                return df
            except Exception as e:
                # Return empty template dataframe on failure to read
                empty = pd.DataFrame(columns=list(dtypes.keys()))
                for col, dt in dtypes.items():
                    empty[col] = empty[col].astype(dt)
                return empty

        # Initialize Dask LocalCluster with explicit limits
        self.logger.info("Initializing Dask LocalCluster with 4 workers and 1.5GB limit per worker...")
        cluster = LocalCluster(
            n_workers=4,
            threads_per_worker=1,
            memory_limit="1.5GB",
            dashboard_address=None,
        )
        client = Client(cluster)
        self.logger.info(f"Dask Distributed Client ready: {client}")

        try:
            # Process year-by-year
            for y in sorted(files_by_year.keys()):
                y_files = files_by_year[y]
                self.logger.info(f"Processing year {y} ({len(y_files)} files)...")
                
                delayed_dfs = [read_and_normalize(zf) for zf in y_files]
                df = dd.from_delayed(delayed_dfs, meta=meta_df)
                
                # Drop duplicates for this year
                df = df.drop_duplicates()
                
                # Save to partitioned directory year=YYYY
                year_dir = partitioned_dir / f"year={y}"
                self.logger.info(f"Writing Parquet partition for year {y} to {year_dir}...")
                
                df.to_parquet(
                    str(year_dir),
                    engine="pyarrow",
                    write_metadata_file=True,
                    overwrite=True,
                )
                self.logger.info(f"Finished partition for year {y}.")
                
            self.logger.info("Successfully completed IVS year-by-year processing.")
            
        except Exception as e:
            self.logger.error(f"Dask processing failed: {e}")
            raise e
        finally:
            self.logger.info("Closing Dask Client and LocalCluster...")
            client.close()
            cluster.close()
