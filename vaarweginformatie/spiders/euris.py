import scrapy
import re
import pandas as pd


class EurisLatestFilesSpider(scrapy.Spider):
    name = "euris_latest_files"
    allowed_domains = ["eurisportal.eu"]

    files_url = 'https://www.eurisportal.eu/AWFImportData/api/ExportFile/GetFiles?countryCode={country_code}'
    download_url = 'https://www.eurisportal.eu/AWFImportData/api/ExportFile/DownloadFile?fileName={filename}'
    countries = ['NL', 'BE', 'DE', 'FR', 'CH', 'BG', 'UA', 'HU', 'HR', 'SK', 'RO', 'CS', 'LU', 'AT', 'GE']
    path_re = re.compile(r'(?P<country>[A-Z]{2})_(?P<dataset>[\w]+)_(?P<date>[\d]+)_(?P<version>v\d+\.\d+)\.zip')

    custom_settings = {
        "ITEM_PIPELINES": {
            "vaarweginformatie.pipelines.EurisFilesPipeline": 1,
        },
        "FILES_STORE": "files-store",
    }

    def start_requests(self):
        for country_code in self.countries:
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


