import scrapy


class QueryserviceSpider(scrapy.Spider):
    name = "queryservice"
    allowed_domains = ["www.vaarweginformatie.nl"]
    start_urls = ["https://www.vaarweginformatie.nl/wfswms/queryservice/application.wadl"]

    def parse(self, response):
        pass
