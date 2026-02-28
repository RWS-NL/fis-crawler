import pathlib

import scrapy

import geopandas as gpd
import pandas as pd
from tqdm.auto import tqdm


class DataserviceSpider(scrapy.Spider):
    """
    Crawler for dataservices at vaarweginformatie.nl
    Extracts all information and stores it in FIS_EXPORT_DIR (default fis-export)

    The following files are created:
    - vaarweginformatie.jsonl (all exported data)
    - {geo-type}.jsonl  (data grouped per geo-type)
    - {geo-type}.json (data grouped per geo-type, merged with isrs)
    - {geo-type}.parquet (same but in parquet format)
    - {geo-type}.geojson (converted to geojson format)
    - {geo-type}.geoparquet (converted to geoparquet format)

    Implementation assumes PerGeoTypeExportPipeline is connected.
    """

    name = "dataservice"
    allowed_domains = ["www.vaarweginformatie.nl"]

    base_url = "https://www.vaarweginformatie.nl/wfswms/dataservice"
    wadl_url = f"{base_url}/application.wadl"
    namespaces = {"wadl": "http://wadl.dev.java.net/2009/02"}

    @property
    def data_dir(self):
        data_dir = pathlib.Path(self.settings.get("FIS_EXPORT_DIR", "fis-export"))
        data_dir = data_dir.expanduser()
        data_dir.mkdir(exist_ok=True, parents=True)
        return data_dir

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """instantiate spider and attach spider closed method for post-post processing"""
        spider = super(DataserviceSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(
            spider.spider_closed, signal=scrapy.signals.spider_closed
        )
        return spider

    async def start(self):
        yield scrapy.Request(url=self.wadl_url, callback=self.parse_waml)

    def parse_waml(self, response):
        versions = response.xpath(
            f".//wadl:resources/wadl:resource", namespaces=self.namespaces
        )
        last_version = versions[-1]
        # we're looking from the last version
        root = last_version
        # get the version number
        version = last_version.attrib["path"]

        self.version = version
        self.root = root

        self.version_path = f"{self.base_url}/{version}"

        geogeneration_url = f"{self.version_path}/geogeneration"
        yield scrapy.Request(url=geogeneration_url, callback=self.parse_geogeneration)

    def parse_geogeneration(self, response):
        geogeneration = response.json()["GeoGeneration"]
        self.geogeneration = geogeneration
        geotypes_url = f"{self.version_path}/geotype"
        yield scrapy.Request(url=geotypes_url, callback=self.parse_geotypes)

    def parse_geotypes(self, response):
        geo_types = response.json()
        for geo_type in geo_types:
            offset = 0
            geotype_url = (
                f"{self.version_path}/{self.geogeneration}/{geo_type}?offset={offset}"
            )
            yield scrapy.Request(
                url=geotype_url,
                callback=self.parse_geotype,
                cb_kwargs=dict(geo_type=geo_type),
            )

    def parse_geotype(self, response, geo_type):
        resp_json = response.json()
        offset = resp_json["Offset"]
        count = resp_json["Count"]
        total_count = resp_json["TotalCount"]

        result = resp_json["Result"]

        for row in result:
            yield row

        # if we have records left, request the next page
        next_page = offset + count < total_count
        if next_page:
            offset = offset + count
            geotype_url = (
                f"{self.version_path}/{self.geogeneration}/{geo_type}?offset={offset}"
            )
            yield scrapy.Request(
                url=geotype_url,
                callback=self.parse_geotype,
                cb_kwargs=dict(geo_type=geo_type),
            )

    def spider_closed(self, spider):
        # Log the closure of the spider with its name
        spider.logger.info("Spider closed: %s, postprocessing", spider.name)

        # TODO: get data dir from settings
        # Set the directory containing the data files
        data_dir = self.data_dir

        # Get all JSONL files in the data directory
        paths = list(data_dir.glob("*.jsonl"))

        # Read the ISRS data from a JSONL file
        isrs_path = data_dir / "isrs.jsonl"
        isrs_df = None
        if isrs_path.exists():
            isrs_df = pd.read_json(isrs_path, lines=True)

        # Iterate over each JSONL file path in the directory
        for path in paths:
            # Read the JSONL file into a DataFrame
            df = pd.read_json(path, lines=True)

            # Save the DataFrame to a JSON file
            df.to_json(path.with_suffix(".json"))

            # Save the DataFrame to a Parquet file
            df.to_parquet(path.with_suffix(".parquet"))

            # If the DataFrame contains the "IsrsId" column, merge it with the ISRS DataFrame
            if (
                "IsrsId" in df.columns
                and "isrs" not in path.name
                and isrs_df is not None
            ):
                df = pd.merge(
                    df,
                    isrs_df,
                    left_on="IsrsId",
                    how="left",
                    right_on="Id",
                    suffixes=["", "_isrs"],
                )

            # If the DataFrame contains the "Geometry" column, convert it to a GeoDataFrame
            if "Geometry" in df.columns:
                # Create a GeoSeries from the WKT (Well-Known Text) representation of the geometry
                geometry = gpd.GeoSeries.from_wkt(
                    df["Geometry"].astype(str), on_invalid="warn"
                )

                # Create a GeoDataFrame with the geometry and set the coordinate reference system to EPSG:4326
                gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

                # Save the GeoDataFrame to a GeoParquet file
                gdf.to_parquet(path.with_suffix(".geoparquet"))

                # Save the GeoDataFrame to a GeoJSON file
                gdf.to_file(path.with_suffix(".geojson"))
