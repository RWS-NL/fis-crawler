import scrapy
import scrapy.utils.defer
import re
import pandas as pd

import logging

logger = logging.getLogger(__name__)

class EurisLatestFilesSpider(scrapy.Spider):
    name = "euris"
    allowed_domains = ["eurisportal.eu"]

    files_url = 'https://www.eurisportal.eu/AWFImportData/api/ExportFile/GetFiles?countryCode={country_code}'
    all_files_url = 'https://www.eurisportal.eu/AWFImportData/api/ExportFile/GetFiles'
    download_url = 'https://www.eurisportal.eu/AWFImportData/api/ExportFile/DownloadFile?fileName={filename}'
    # countries_expected = ['NL', 'BE', 'DE', 'FR', 'CH', 'BG', 'UA', 'HU', 'HR', 'SK', 'RO', 'CS', 'LU', 'AT', 'GE']
    # countries_observed = ['CZ', 'XX', 'BE', 'HU', 'SK', 'FR', 'HR', 'DE', 'LU', 'RS', 'NL', 'BG', 'AT', 'RO']
    # Not available: 'CH', 'UA', 'CS', 'GE'
    # New codes: 'CZ', 'XX', 'RS'
    #
    path_re = re.compile(r'(?P<country>[A-Z]{2})_(?P<dataset>[\w]+)_(?P<date>[\d]+)_(?P<version>v\d+\.\d+)\.zip')

    custom_settings = {
        # not too fast....
        "DOWNLOAD_DELAY": 2.5,
        # it's bit error prone
        "RETRY_TIMES": 5,
        "ITEM_PIPELINES": {
            "vaarweginformatie.pipelines.EurisFilesPipeline": 1,
        },
        "FILES_STORE": "files-store",
    }


    async def start(self):

        # this will download country codes
        # we will loop over all country codes and fire eextra requests in the callback
        all_files_request = scrapy.Request(
            self.all_files_url,
            headers={"Accept": "application/json", "User-Agent": "euris-scraper"},
            callback=self.parse_all_files
        )
        yield all_files_request

    def parse_all_files(self, response):
        data = response.json()
        country_codes = set()
        for row in data:
            country_codes.add(row['countryCode'])
        country_codes = list(country_codes)
        self.country_codes = country_codes
        logger.info('Country codes: %s', country_codes)

        for country_code in self.country_codes:
            url = self.files_url.format(country_code=country_code)
            yield scrapy.Request(
                url,
                headers={"Accept": "application/json", "User-Agent": "euris-scraper"},
                callback=self.parse_files,
                cb_kwargs={"country_code": country_code}
            )

    def parse_files(self, response, country_code):
        data = response.json()
        for row in data:
            name = row['name']
            match = self.path_re.search(name)
            if match:
                row.update(match.groupdict())
        if not data:
            return
        df = pd.DataFrame(data)
        if df.empty:
            return
        last_files = df.sort_values(['countryCode', 'dataset', 'lastModified']).groupby(['countryCode', 'dataset']).last().reset_index()
        for _, row in last_files.iterrows():
            file_url = self.download_url.format(filename=row['name'])
            yield {
                'file_urls': [file_url],
                'filename': row['name'],
                'country': row.get('country'),
                'dataset': row.get('dataset'),
                'date': row.get('date'),
                'version': row.get('version'),
            }


