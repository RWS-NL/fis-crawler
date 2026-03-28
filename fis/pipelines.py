# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import logging
import os
import pathlib

import geopandas as gpd
import pandas as pd
import scrapy.exporters

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.pipelines.files import FilesPipeline
from tqdm.auto import tqdm

from scrapy.utils.project import get_project_settings
import zipfile

settings = get_project_settings()
version = settings.get("VERSION", "v0.1.0")

logger = logging.getLogger(__name__)


class VaarweginformatiePipeline:
    def process_item(self, item, spider):
        return item


class PerGeoTypeExportPipeline:
    """Distribute items across multiple json files according to their geotype"""

    def open_spider(self, spider):
        self.geo_type_to_exporter = {}

    def close_spider(self, spider):
        for exporter, json_file in self.geo_type_to_exporter.values():
            exporter.finish_exporting()
            json_file.close()

    def _exporter_for_item(self, item, spider):
        adapter = ItemAdapter(item)
        geo_type = adapter["GeoType"]

        data_dir = spider.data_dir

        if geo_type not in self.geo_type_to_exporter:
            json_path = data_dir / f"{geo_type}.jsonl"
            json_file = json_path.open("wb")
            exporter = scrapy.exporters.JsonLinesItemExporter(json_file)
            exporter.start_exporting()
            self.geo_type_to_exporter[geo_type] = (exporter, json_file)
        exporter, _ = self.geo_type_to_exporter[geo_type]
        return exporter

    def process_item(self, item, spider):
        exporter = self._exporter_for_item(item, spider)
        exporter.export_item(item)

        return item


class EurisFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        # Use the filename from the item if available
        return item.get("filename") or super().file_path(
            request, response, info, item=item
        )

    def item_completed(self, results, item, info):
        # Call parent to keep default behavior
        item = super().item_completed(results, item, info)
        # Extract the zip file if download was successful
        for ok, result in results:
            if ok:
                path = result.get("path")
                if path and path.endswith(".zip"):
                    abs_path = os.path.join(self.store.basedir, path)
                    extract_dir = self.store.basedir  # or customize

                    with zipfile.ZipFile(abs_path, "r") as zip_ref:
                        zip_ref.extractall(extract_dir)
                    info.spider.logger.info(f"Extracted {abs_path} to {extract_dir}")
        return item

    def close_spider(self, spider):
        self.process_ris_files(spider)
        # Graph building moved to fis.graph.cli euris command

    def process_ris_files(self, spider):
        # After all downloads and extractions, process RIS Excel files into a GeoDataFrame
        data_dir = pathlib.Path(self.store.basedir)
        excel_files = list(data_dir.glob("RisIndex*.xlsx"))
        spider.logger.info(f"Found {len(excel_files)} RIS Excel files to process.")

        ris_dfs = []
        for excel_file in tqdm(excel_files, desc="Processing RIS Excel files"):
            spider.logger.info(f"Reading {excel_file}")
            # Use calamine engine for much faster excel reading if available
            try:
                df = pd.read_excel(excel_file, engine="calamine")
            except Exception as e:
                spider.logger.warning(
                    f"Calamine engine failed for {excel_file}, falling back: {e}"
                )
                df = pd.read_excel(excel_file)

            if df.empty:
                continue

            # Standardize essential columns for efficient concatenation
            # 1. Coordinates
            lon_col = (
                "long_"
                if "long_" in df.columns
                else (df.columns[0] if len(df.columns) > 0 else None)
            )
            lat_col = (
                "Lat"
                if "Lat" in df.columns
                else (df.columns[1] if len(df.columns) > 1 else None)
            )

            if lon_col and lat_col:
                df = df.rename(columns={lon_col: "_longitude", lat_col: "_latitude"})

            # 2. Country Code (if exists)
            cc_col = next((c for c in df.columns if c.lower() == "countrycode"), None)
            if cc_col:
                df = df.rename(columns={cc_col: "_country_code"})

            # Filter out entirely empty rows/columns before adding to list
            df = df.dropna(axis=0, how="all")
            # Drop columns that are 100% NaN to keep concatenation lean
            df = df.dropna(axis=1, how="all")

            df["_source_path"] = excel_file.name
            ris_dfs.append(df)

        if ris_dfs:
            spider.logger.info("Concatenating %d DataFrames...", len(ris_dfs))
            # Concatenate plain DataFrames (much faster than GeoDataFrames)
            combined_df = pd.concat(ris_dfs, ignore_index=True, sort=False)

            # Final filtering
            if (
                "_longitude" in combined_df.columns
                and "_latitude" in combined_df.columns
            ):
                combined_df = combined_df.dropna(subset=["_longitude", "_latitude"])

            if combined_df.empty:
                spider.logger.info("No valid records remain after filtering.")
                return

            spider.logger.info(
                "Creating GeoDataFrame with %d records...", len(combined_df)
            )
            # Convert to GeoDataFrame ONCE at the end
            geometry = gpd.points_from_xy(
                x=combined_df["_longitude"],
                y=combined_df["_latitude"],
                crs="EPSG:4326",
            )
            ris_gdf = gpd.GeoDataFrame(combined_df, geometry=geometry)

            out_dir = data_dir / version
            out_dir.mkdir(exist_ok=True)
            out_path = out_dir / f"ris_index_{version}.gpkg"

            spider.logger.info(f"Saving RIS GeoDataFrame to {out_path}")
            # Use engine='pyogrio' if available for faster writing
            try:
                ris_gdf.to_file(out_path, engine="pyogrio")
            except Exception:
                ris_gdf.to_file(out_path)

            spider.logger.info(f"Saved RIS GeoDataFrame to {out_path}")
        else:
            spider.logger.info("No RIS Excel files were successfully read.")


# concat_network and generate_graph functions moved to fis/graph/euris.py
# Use: uv run python -m fis.cli graph euris
