import scrapy


class IvsSpider(scrapy.Spider):
    name = "ivs"
    allowed_domains = ["downloads.rijkswaterstaatdata.nl"]
    start_urls = [
        "https://downloads.rijkswaterstaatdata.nl/scheepvaart/goederenvervoer/archief/"
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 16,
        "ITEM_PIPELINES": {
            "fis.pipelines.IvsFilesPipeline": 1,
        },
        "FILES_STORE": "/scratch-shared/fbaart/data/ivs/downloads",
    }

    def parse(self, response):
        # Find all zip links in the directory listing
        links = response.css("a::attr(href)").getall()
        zip_links = [
            link
            for link in links
            if link.startswith("IVS_weekmonitor_") and link.endswith(".zip")
        ]

        self.logger.info(
            f"Found {len(zip_links)} IVS weekmonitor zip files to download."
        )

        for zip_link in zip_links:
            # Reconstruct absolute URL
            file_url = response.urljoin(zip_link)
            yield {
                "file_urls": [file_url],
                "filename": zip_link,
            }

