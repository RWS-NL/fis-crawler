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
                    import zipfile

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
        ris_gdfs = []
        for excel_file in tqdm(excel_files, desc="Processing RIS Excel files"):
            spider.logger.info(f"Reading {excel_file}")
            ris_df = pd.read_excel(excel_file)
            # Adjust column names as needed
            ris_df_geoms = gpd.points_from_xy(
                x=ris_df.get("long_", ris_df.columns[0]),
                y=ris_df.get("Lat", ris_df.columns[1]),
                crs="EPSG:4326",
            )
            ris_gdf = gpd.GeoDataFrame(ris_df, geometry=ris_df_geoms)
            ris_gdf["path"] = excel_file.name
            ris_gdfs.append(ris_gdf)
        if ris_gdfs:
            ris_gdf = pd.concat(ris_gdfs)

            out_dir = data_dir / version
            out_dir.mkdir(exist_ok=True)
            out_path = out_dir / f"ris_index_{version}.gpkg"
            spider.logger.info(
                f"Saving RIS GeoDataFrame with {len(ris_gdf)} records to {out_path}"
            )
            ris_gdf.to_file(out_path)
            spider.logger.info(f"Saved RIS GeoDataFrame to {out_path}")
        else:
            spider.logger.info("No RIS GeoDataFrames were created.")


# concat_network and generate_graph functions moved to fis/graph/euris.py
# Use: uv run python -m fis.cli graph euris
