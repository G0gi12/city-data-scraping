import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import time
from io import StringIO

import pandas as pd
import json


class CitiesDataSpider(CrawlSpider):
    counter = 0
    name = "cities-data"
    allowed_domains = ["www.city-data.com"]
    start_urls = [
        "https://www.city-data.com/city/Alabama.html",
        "https://www.city-data.com/city/Alaska.html",
        "https://www.city-data.com/city/Arizona.html",
        "https://www.city-data.com/city/Arkansas.html",
        "https://www.city-data.com/city/California.html",
        "https://www.city-data.com/city/Colorado.html",
        "https://www.city-data.com/city/Connecticut.html",
        "https://www.city-data.com/city/Delaware.html",
        "https://www.city-data.com/city/Florida.html",
        "https://www.city-data.com/city/Georgia.html",
        "https://www.city-data.com/city/Hawaii.html",
        "https://www.city-data.com/city/Idaho.html",
        "https://www.city-data.com/city/Illinois.html",
        "https://www.city-data.com/city/Indiana.html",
        "https://www.city-data.com/city/Iowa.html",
        "https://www.city-data.com/city/Kansas.html",
        "https://www.city-data.com/city/Kentucky.html",
        "https://www.city-data.com/city/Louisiana.html",
        "https://www.city-data.com/city/Maine.html",
        "https://www.city-data.com/city/Maryland.html",
        "https://www.city-data.com/city/Massachusetts.html",
        "https://www.city-data.com/city/Michigan.html",
        "https://www.city-data.com/city/Minnesota.html",
        "https://www.city-data.com/city/Mississippi.html",
        "https://www.city-data.com/city/Missouri.html",
        "https://www.city-data.com/city/Montana.html",
        "https://www.city-data.com/city/Nebraska.html",
        "https://www.city-data.com/city/Nevada.html",
        "https://www.city-data.com/city/New-Hampshire.html",
        "https://www.city-data.com/city/New-Jersey.html",
        "https://www.city-data.com/city/New-Mexico.html",
        "https://www.city-data.com/city/New-York.html",
        "https://www.city-data.com/city/North-Carolina.html",
        "https://www.city-data.com/city/North-Dakota.html",
        "https://www.city-data.com/city/Ohio.html",
        "https://www.city-data.com/city/Oklahoma.html",
        "https://www.city-data.com/city/Oregon.html",
        "https://www.city-data.com/city/Pennsylvania.html",
        "https://www.city-data.com/city/Rhode-Island.html",
        "https://www.city-data.com/city/South-Carolina.html",
        "https://www.city-data.com/city/South-Dakota.html",
        "https://www.city-data.com/city/Tennessee.html",
        "https://www.city-data.com/city/Texas.html",
        "https://www.city-data.com/city/Utah.html",
        "https://www.city-data.com/city/Vermont.html",
        "https://www.city-data.com/city/Virginia.html",
        "https://www.city-data.com/city/Washington.html",
        "https://www.city-data.com/city/West-Virginia.html",
        "https://www.city-data.com/city/Wisconsin.html",
        "https://www.city-data.com/city/Wyoming.html"
    ]


    rules = (
        Rule(LinkExtractor(allow=r"https://www\.city-data\.com/city/[^/]+-[A-Za-z]+\.html"),
             callback="parse_city", follow=True),
    )


    def parse_city(self, response):
        self.counter += 1
        # if self.counter == 50:
        #     print("Waiting for 50sec")
        #     time.sleep(50)
        #     self.counter = 0
        city_state = response.xpath("//h1[@class='city']/span/text()").extract_first() # example Milwaukee, WI
        table_crimes = response.xpath('//table[@class="table tabBlue tblsort tblsticky sortable"]').extract_first()

        if table_crimes:
            crimes_df = pd.read_html(StringIO(str(table_crimes)))[0]

            # crimes_df.to_json()
            crime_data = {
                'city_state': city_state,
                'crime_stats': crimes_df.to_dict()
            }

            yield crime_data

    # def close(self, reason):
    #     # This method is called when the spider finishes executing
    #     # It gathers all items and writes to a JSON file
    #     all_city_data = []

    #     # Open the file to write JSON
    #     with open('city_crime_data.json', 'w') as f:
    #         for item in self.crawled_items:
    #             all_city_data.append(item)
            
    #         # Dump all city crime data to a JSON file
    #         json.dump(all_city_data, f, indent=4)
