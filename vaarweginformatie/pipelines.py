# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import scrapy.exporters
import pathlib


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
