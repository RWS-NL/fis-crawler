import scrapy
import ipdb


class DataserviceSpider(scrapy.Spider):
    name = "dataservice"
    allowed_domains = ["www.vaarweginformatie.nl"]

    base_url = "https://www.vaarweginformatie.nl/wfswms/dataservice"
    wadl_url = f"{base_url}/application.wadl"
    namespaces = {"wadl": "http://wadl.dev.java.net/2009/02"}

    def start_requests(self):
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
        spider.logger.info("Spider closed: %s", spider.name)
