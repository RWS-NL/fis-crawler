import pathlib
import json
import scrapy
import geopandas as gpd
import pandas as pd


class DiskSpider(scrapy.Spider):
    """
    Crawler for DISK Beheerobjecten at Rijkswaterstaat.
    Extracts lock and bridge information and stores it in DISK_EXPORT_DIR (default output/disk-export)

    Fetches:
    - disk_beheerobjecten:schutsluis
    - disk_beheerobjecten:keersluis
    - disk_beheerobjecten:spuisluis
    - disk_beheerobjecten:brug_beweegbaar
    - disk_beheerobjecten:brug_vast

    Implementation assumes PerGeoTypeExportPipeline is connected.
    """

    name = "disk"
    allowed_domains = ["geo.rijkswaterstaat.nl"]

    base_url = "https://geo.rijkswaterstaat.nl/services/ogc/gdr/disk_beheerobjecten/ows?service=WFS&version=2.0.0&request=GetFeature&outputFormat=application/json"

    layers = [
        "disk_beheerobjecten:schutsluis",
        "disk_beheerobjecten:keersluis",
        "disk_beheerobjecten:spuisluis",
        "disk_beheerobjecten:brug_beweegbaar",
        "disk_beheerobjecten:brug_vast",
    ]

    @property
    def data_dir(self):
        data_dir = pathlib.Path(
            self.settings.get("DISK_EXPORT_DIR", "output/disk-export")
        )
        data_dir = data_dir.expanduser()
        data_dir.mkdir(exist_ok=True, parents=True)
        return data_dir

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """instantiate spider and attach spider closed method for post processing"""
        spider = super(DiskSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(
            spider.spider_closed, signal=scrapy.signals.spider_closed
        )
        return spider

    def start_requests(self):
        # We don't actually need Scrapy to make async HTTP requests if we use owslib
        # but to keep it within the Scrapy lifecycle, we can just yield a dummy request
        # and do the work in the callback, or just do the work here and yield items.

        from owslib.wfs import WebFeatureService

        wfs_url = (
            "https://geo.rijkswaterstaat.nl/services/ogc/gdr/disk_beheerobjecten/ows"
        )
        try:
            wfs = WebFeatureService(url=wfs_url, version="2.0.0")
        except Exception as e:
            self.logger.error("Failed to connect to WFS: %s", e)
            return

        for layer in self.layers:
            geo_type = layer.split(":")[1]
            try:
                self.logger.info("Fetching WFS layer: %s", layer)
                response = wfs.getfeature(
                    typename=layer, outputFormat="application/json"
                )

                # The response is a file-like object containing GeoJSON bytes
                geojson_data = response.read()
                resp_json = json.loads(geojson_data)
                features = resp_json.get("features", [])

                for feature in features:
                    props = feature.get("properties", {})
                    geom = feature.get("geometry")

                    row = {**props}

                    from shapely.geometry import shape

                    if geom:
                        try:
                            s = shape(geom)
                            row["Geometry"] = s.wkt
                        except Exception:
                            row["Geometry"] = None

                    row["GeoType"] = geo_type
                    yield row
            except Exception as e:
                self.logger.error("Failed to fetch layer %s: %s", layer, e)

    def spider_closed(self, spider):
        # Log the closure of the spider with its name
        spider.logger.info("Spider closed: %s, postprocessing", spider.name)

        data_dir = self.data_dir
        paths = list(data_dir.glob("*.jsonl"))

        for path in paths:
            # We skip generic file if it exists, but the pipeline outputs {geo_type}.jsonl
            df = pd.read_json(path, lines=True)

            if "GeoType" in df.columns:
                df = df.drop(columns=["GeoType"])

            if "Geometry" in df.columns:
                geometry = gpd.GeoSeries.from_wkt(
                    df["Geometry"].astype(str), on_invalid="warn"
                )
                # RWS WFS returns RD New (EPSG:28992)
                gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:28992")
                # Convert to standard WGS84 (EPSG:4326)
                gdf = gdf.to_crs("EPSG:4326")

                # Re-assign back to WKT string for standard json/parquet serializers
                df["Geometry"] = gdf.geometry.to_wkt()

                df.to_json(path.with_suffix(".json"))
                df.to_parquet(path.with_suffix(".parquet"))

                gdf.to_parquet(path.with_suffix(".geoparquet"))
                gdf.to_file(path.with_suffix(".geojson"))
            else:
                df.to_json(path.with_suffix(".json"))
                df.to_parquet(path.with_suffix(".parquet"))
