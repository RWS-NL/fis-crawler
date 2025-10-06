# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import scrapy.exporters
import pathlib
from scrapy.pipelines.files import FilesPipeline
import os
import pandas as pd
import geopandas as gpd
import re
from tqdm.auto import tqdm


class VaarweginformatiePipeline:
    def process_item(self, item, spider):
        return item


class PerGeoTypeExportPipeline:
    """Distribute items across multiple XML files according to their 'year' field"""

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
        return item.get('filename') or super().file_path(request, response, info, item=item)

    def item_completed(self, results, item, info):
        # Call parent to keep default behavior
        item = super().item_completed(results, item, info)
        # Extract the zip file if download was successful
        for ok, result in results:
            if ok:
                path = result.get('path')
                if path and path.endswith('.zip'):
                    abs_path = os.path.join(self.store.basedir, path)
                    extract_dir = self.store.basedir  # or customize
                    import zipfile
                    with zipfile.ZipFile(abs_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_dir)
                    info.spider.logger.info(f"Extracted {abs_path} to {extract_dir}")
        return item

    def close_spider(self, spider):
        self.process_ris_files(spider)
        self.concat_network(spider)

    def process_ris_files(self, spider):
        # After all downloads and extractions, process RIS Excel files into a GeoDataFrame
        data_dir = pathlib.Path(self.store.basedir)
        excel_files = list(data_dir.glob('RisIndex*.xlsx'))
        spider.logger.info(f"Found {len(excel_files)} RIS Excel files to process.")
        ris_gdfs = []
        for excel_file in tqdm(excel_files, desc="Processing RIS Excel files"):
            spider.logger.info(f"Reading {excel_file}")
            ris_df = pd.read_excel(excel_file)
            # Adjust column names as needed
            ris_df_geoms = gpd.points_from_xy(
                x=ris_df.get("long_", ris_df.columns[0]), 
                y=ris_df.get("Lat", ris_df.columns[1]), 
                crs="EPSG:4326"
            )
            ris_gdf = gpd.GeoDataFrame(ris_df, geometry=ris_df_geoms)
            ris_gdf['path'] = excel_file.name
            ris_gdfs.append(ris_gdf)
        if ris_gdfs:
            ris_gdf = pd.concat(ris_gdfs)
            version = getattr(spider, "version", "v0.1.0")
            out_dir = data_dir / version
            out_dir.mkdir(exist_ok=True)
            out_path = out_dir / f"ris_index_{version}.gpkg"
            spider.logger.info(f"Saving RIS GeoDataFrame with {len(ris_gdf)} records to {out_path}")
            ris_gdf.to_file(out_path)
            spider.logger.info(f"Saved RIS GeoDataFrame to {out_path}")
        else:
            spider.logger.info("No RIS GeoDataFrames were created.")

    def concat_network(self, spider):
        data_dir = pathlib.Path(self.store.basedir)
        version = getattr(spider, "version", "v0.1.0")
        out_dir = data_dir / version
        out_dir.mkdir(exist_ok=True)

        node_paths = list(data_dir.glob('Node_*.geojson'))
        section_paths = list(data_dir.glob('FairwaySection_*.geojson'))

        spider.logger.info(f"Found {len(node_paths)} node files and {len(section_paths)} section files for concatenation.")

        node_gdfs = []
        for node_path in tqdm(node_paths, desc="Reading node files"):
            gdf = gpd.read_file(node_path)
            gdf['path'] = node_path.name
            node_gdfs.append(gdf)
        if node_gdfs:
            node_gdf = pd.concat(node_gdfs)
            uniq_columns = set(node_gdf.columns) - {'path'}
            n_nodes_duplicated = node_gdf.duplicated(subset=uniq_columns).sum()
            node_gdf = node_gdf.drop_duplicates(subset=uniq_columns)
            spider.logger.info(f"Removed {n_nodes_duplicated} duplicated nodes.")
            node_path_re = re.compile(r'Node_(?P<countrycode>[A-Z]+)_\d+.geojson')
            node_gdf['countrycode_locode'] = node_gdf['locode'].apply(lambda x: x[:2])
            node_gdf['countrycode_path'] = node_gdf['path'].apply(lambda x: node_path_re.match(x).group('countrycode'))
            node_gdf['countrycode'] = node_gdf['countrycode_locode']
            node_gdf['node_id'] = node_gdf.apply(lambda row: f"{row['countrycode']}_{row['objectcode']}", axis=1)
            node_gdf.to_file(out_dir / f'nodes-{version}.geojson')
            spider.logger.info(f"Saved concatenated nodes to {out_dir / f'nodes-{version}.geojson'}")
        else:
            spider.logger.info("No node files found for concatenation.")

        section_gdfs = []
        for section_path in tqdm(section_paths, desc="Reading section files"):
            gdf = gpd.read_file(section_path)
            gdf['path'] = section_path.name
            section_gdfs.append(gdf)
        if section_gdfs:
            section_gdf = pd.concat(section_gdfs)
            uniq_columns = set(section_gdf.columns) - {'path'}
            n_sections_duplicated = section_gdf.duplicated(subset=uniq_columns).sum()
            section_gdf = section_gdf.drop_duplicates(subset=uniq_columns)
            spider.logger.info(f"Removed {n_sections_duplicated} duplicated sections.")
            section_gdf.to_file(out_dir / f'sections-{version}.geojson')
            spider.logger.info(f"Saved concatenated sections to {out_dir / f'sections-{version}.geojson'}")
        else:
            spider.logger.info("No section files found for concatenation.")
