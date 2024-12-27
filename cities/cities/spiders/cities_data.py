import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import time
from io import StringIO

import pandas as pd
import json

import warnings
from scrapy.exceptions import ScrapyDeprecationWarning
warnings.filterwarnings('ignore', category=ScrapyDeprecationWarning)
warnings.filterwarnings('ignore', message='.*_get_slot_key.*')


class CitiesDataSpider(CrawlSpider):
    name = "cities-data"
    allowed_domains = ["www.city-data.com"]
    start_urls = [
        "https://www.city-data.com/city/Alabama.html"
        # "https://www.city-data.com/city/Alaska.html",
        # "https://www.city-data.com/city/Arizona.html",
        # "https://www.city-data.com/city/Arkansas.html",
        # "https://www.city-data.com/city/California.html",
        # "https://www.city-data.com/city/Colorado.html",
        # "https://www.city-data.com/city/Connecticut.html",
        # "https://www.city-data.com/city/Delaware.html",
        # "https://www.city-data.com/city/Florida.html",
        # "https://www.city-data.com/city/Georgia.html",
        # "https://www.city-data.com/city/Hawaii.html",
        # "https://www.city-data.com/city/Idaho.html",
        # "https://www.city-data.com/city/Illinois.html",
        # "https://www.city-data.com/city/Indiana.html",
        # "https://www.city-data.com/city/Iowa.html",
        # "https://www.city-data.com/city/Kansas.html",
        # "https://www.city-data.com/city/Kentucky.html",
        # "https://www.city-data.com/city/Louisiana.html",
        # "https://www.city-data.com/city/Maine.html",
        # "https://www.city-data.com/city/Maryland.html",
        # "https://www.city-data.com/city/Massachusetts.html",
        # "https://www.city-data.com/city/Michigan.html",
        # "https://www.city-data.com/city/Minnesota.html",
        # "https://www.city-data.com/city/Mississippi.html",
        # "https://www.city-data.com/city/Missouri.html",
        # "https://www.city-data.com/city/Montana.html",
        # "https://www.city-data.com/city/Nebraska.html",
        # "https://www.city-data.com/city/Nevada.html",
        # "https://www.city-data.com/city/New-Hampshire.html",
        # "https://www.city-data.com/city/New-Jersey.html",
        # "https://www.city-data.com/city/New-Mexico.html",
        # "https://www.city-data.com/city/New-York.html",
        # "https://www.city-data.com/city/North-Carolina.html",
        # "https://www.city-data.com/city/North-Dakota.html",
        # "https://www.city-data.com/city/Ohio.html",
        # "https://www.city-data.com/city/Oklahoma.html",
        # "https://www.city-data.com/city/Oregon.html",
        # "https://www.city-data.com/city/Pennsylvania.html",
        # "https://www.city-data.com/city/Rhode-Island.html",
        # "https://www.city-data.com/city/South-Carolina.html",
        # "https://www.city-data.com/city/South-Dakota.html",
        # "https://www.city-data.com/city/Tennessee.html",
        # "https://www.city-data.com/city/Texas.html",
        # "https://www.city-data.com/city/Utah.html",
        # "https://www.city-data.com/city/Vermont.html",
        # "https://www.city-data.com/city/Virginia.html",
        # "https://www.city-data.com/city/Washington.html",
        # "https://www.city-data.com/city/West-Virginia.html",
        # "https://www.city-data.com/city/Wisconsin.html",
        # "https://www.city-data.com/city/Wyoming.html"
    ]


    rules = (
        Rule(LinkExtractor(allow=r"https://www\.city-data\.com/city/[^/]+-[A-Za-z]+\.html"),
             callback="parse_city", follow=True),
    )


    def parse_city(self, response):
        city_state = response.xpath("//h1[@class='city']/span/text()").extract_first()
        city_name = city_state.split(',')[0].strip() if city_state else None
        
        city_data = {
            'city_state': city_state.strip() if city_state else None,
            'population': {
                'total': self.clean_number(response.xpath('//section[@id="city-population"]/text()').extract_first()),
                'urban_percentage': 62,  # You'll need to extract these values
                'rural_percentage': 38   # You'll need to extract these values
            },
            'demographics': {
                'male_population': self.clean_number(response.xpath('//section[@class="population-by-sex"]//td/text()').extract()[0]),
                'female_population': self.clean_number(response.xpath('//section[@class="population-by-sex"]//td/text()').extract()[2]),
                'median_age': self.clean_number(response.xpath('//section[@class="median-age"]//td/text()').extract()[0]),
            },
            'economic_data': {
                'median_household_income': self.clean_currency(response.xpath('//section[@class="median-income"]//td/text()').extract()[0]),
                'median_house_value': self.clean_currency(response.xpath('//section[@class="median-income"]//td/text()').extract()[2]),
                'median_rent': self.clean_currency(response.xpath('//section[@class="median-rent"]/p/text()').extract_first()),
                'cost_living_index': self.clean_number(response.xpath('//section[@class="cost-of-living-index"]/text()').extract_first()),
                'poverty_rate': self.clean_percentage(response.xpath('//section[@class="poverty-level"]/text()').extract_first()),
                'median_property_tax': self.clean_tax_data(response.xpath('//section[@class="real-estate-taxes"]/p/text()').extract()[0]),
            },
            'geographic_data': {
                'elevation_ft': self.clean_number(response.xpath('//section[@class="elevation"]/p/text()').extract_first()),
                'population_density': self.clean_number(response.xpath('//section[@class="population-density"]/p/text()').extract()[2]),
            },
            'race_distribution': self.parse_race_distribution(response),
            'building_permits': self.parse_building_permits(response),
            'natural_disasters': self.parse_natural_disasters(response),
            'crime_statistics': self.clean_crime_statistics(self.parse_crime_table(response)),
            'religion_statistics': self.clean_religion_statistics(self.parse_religion_table(response))
        }

        yield {city_name: city_data}

    def clean_number(self, value):
        if not value:
            return None
        return int(''.join(filter(str.isdigit, value))) if any(c.isdigit() for c in value) else None

    def clean_currency(self, value):
        if not value:
            return None
        return int(''.join(filter(str.isdigit, value)))

    def clean_percentage(self, value):
        if not value:
            return None
        return float(value.strip(' %'))

    def clean_tax_data(self, value):
        if not value:
            return None
        amount = ''.join(filter(str.isdigit, value.split('(')[0]))
        return int(amount) if amount else None

    def parse_race_distribution(self, response):
        race_response = response.xpath('//ul/li/ul[@class="list-group"]')
        race_list = race_response.xpath('.//li')
        race_distribution = {}
        for race_data in race_list:
            population = self.clean_number(race_data.xpath('./span/text()').extract_first())
            race = race_data.xpath('./b/text()').extract_first()
            if race:
                race = race.strip()
                race_distribution[race] = population
        return race_distribution

    def parse_building_permits(self, response):
        building_permits = []
        permits_data = response.xpath('//section[@class="building-permits-info"]//ul/li')
        
        for permit in permits_data:
            # Extract all text components
            text_parts = permit.xpath('.//text()').extract()
            # Clean and join the text
            text = ''.join(text_parts).strip()
            
            # Extract year and numbers using regex
            import re
            year_match = re.search(r'(\d{4}):', text)
            buildings_match = re.search(r'(\d+)\s+buildings', text)
            cost_match = re.search(r'\$([0-9,]+)', text)
            
            if year_match and buildings_match and cost_match:
                year = int(year_match.group(1))
                buildings = int(buildings_match.group(1))
                cost = int(cost_match.group(1).replace(',', ''))
                
                building_permits.append({
                    'year': year,
                    'number_of_buildings': buildings,
                    'average_cost': cost
                })
        
        return building_permits

    def parse_natural_disasters(self, response):
        # Initialize the structure for natural disasters data
        disasters_data = {
            'summary': {},
            'declarations': {},
            'disaster_types': {}
        }
        
        # Get all text from the natural disasters section
        disaster_text = ' '.join(response.xpath('//section[@class="natural-disasters"]//text()').extract())
        
        # Parse total number of disasters
        import re
        county_disasters = re.search(r'number of natural disasters in .* County \((\d+)\)', disaster_text)
        us_average = re.search(r'than the US average \((\d+)\)', disaster_text)
        
        if county_disasters and us_average:
            disasters_data['summary'] = {
                'total_disasters': int(county_disasters.group(1)),
                'us_average': int(us_average.group(1)),
                'comparison': 'greater' if 'greater' in disaster_text else 'near' if 'near' in disaster_text else 'less'
            }
        
        # Parse declarations
        major_disasters = re.search(r'Major Disasters .* Declared: (\d+)', disaster_text)
        emergencies = re.search(r'Emergencies Declared: (\d+)', disaster_text)
        
        disasters_data['declarations'] = {
            'major_disasters': int(major_disasters.group(1)) if major_disasters else 0,
            'emergencies': int(emergencies.group(1)) if emergencies else 0
        }
        
        # Parse disaster types
        disaster_types_text = disaster_text.split('Causes of natural disasters:')[-1]
        type_matches = re.findall(r'([A-Za-z\s]+):\s*(\d+)', disaster_types_text)
        
        for disaster_type, count in type_matches:
            disasters_data['disaster_types'][disaster_type.strip()] = int(count)
        
        return disasters_data

    def clean_crime_statistics(self, crime_data):
        if not crime_data:
            return None
        
        # Clean up the crime statistics dictionary
        cleaned_data = {}
        # Implement cleaning logic
        
        return cleaned_data

    def clean_religion_statistics(self, religion_data):
        if not religion_data:
            return None
        
        # Clean up the religion statistics dictionary
        cleaned_data = {}
        # Implement cleaning logic
        
        return cleaned_data

    def parse_crime_table(self, response):
        crime_data = {
            'yearly_statistics': {},
            'crime_index_by_year': {}
        }
        
        # Get all rows from the crime table
        rows = response.xpath('//table[@class="table tabBlue tblsort tblsticky sortable"]//tr')
        
        # Get years from header row
        years = rows[0].xpath('.//h4/text()').extract()[1:]  # Skip the "Type" header
        
        # Process each crime type row
        for row in rows[1:]:  # Skip header row
            # Get crime type
            crime_type = row.xpath('.//b/text()').extract_first()
            if not crime_type:
                continue
                
            # Remove HTML tags and clean up crime type
            crime_type = crime_type.strip()
            
            # Initialize dictionary for this crime type
            crime_data['yearly_statistics'][crime_type] = {}
            
            # Get all cells for this row (skip first cell which is crime type)
            cells = row.xpath('.//td')[1:]
            
            for year, cell in zip(years, cells):
                # Extract total number and rate per 100k
                numbers = cell.xpath('.//text()').extract()
                numbers = [n.strip() for n in numbers if n.strip() and '(' in n or n.strip().isdigit()]
                
                if len(numbers) >= 2:
                    total = int(numbers[0])
                    # Extract rate from string like "(11.9)" and convert to float
                    rate = float(numbers[1].strip('()').strip())
                    
                    # Get US average from title attribute if present
                    us_avg_text = cell.xpath('.//a/@title').extract_first()
                    us_avg = None
                    if us_avg_text and 'US average is' in us_avg_text:
                        us_avg = float(us_avg_text.split('US average is')[1].split('per')[0].strip())
                    
                    crime_data['yearly_statistics'][crime_type][year] = {
                        'total': total,
                        'rate_per_100k': rate,
                        'us_average': us_avg,
                        'comparison': 'above_average' if cell.xpath('.//span[contains(@class, "text-danger")]') else 
                                    'below_average' if cell.xpath('.//span[contains(@class, "text-success")]') else 
                                    'average'
                    }
        
        # Parse Crime Index from footer
        crime_index_row = response.xpath('//tfoot//tr/td[position()>1]/text()').extract()
        for year, index in zip(years, crime_index_row):
            try:
                crime_data['crime_index_by_year'][year] = float(index)
            except (ValueError, TypeError):
                crime_data['crime_index_by_year'][year] = None
                
        return crime_data

    def parse_religion_table(self, response):
        religion_data = {
            'summary': {},
            'source': None,
            'metadata': {
                'base_location': 'Milwaukee County',
                'year': 2010
            }
        }
        
        # Parse the table
        table = response.xpath('//table[@id="religiontable"]')
        rows = table.xpath('.//tr')[1:]  # Skip header row
        
        total_adherents = 0
        total_congregations = 0
        
        # Process each row
        for row in rows:
            cols = row.xpath('.//td/text()').extract()
            if len(cols) == 3:  # Make sure we have all three columns
                religion = cols[0].strip()
                try:
                    adherents = int(cols[1].replace(',', ''))
                    # Handle '-' in congregations column
                    congregations = int(cols[2].replace(',', '')) if cols[2].strip() != '-' else 0
                    
                    religion_data['summary'][religion] = {
                        'adherents': adherents,
                        'congregations': congregations,
                    }
                    
                    # Add to totals (excluding 'None' category)
                    if religion.lower() != 'none':
                        total_adherents += adherents
                        total_congregations += congregations
                    
                except (ValueError, IndexError):
                    continue
        
        # Add totals and percentages
        religion_data['totals'] = {
            'total_adherents': total_adherents,
            'total_congregations': total_congregations,
        }
        
        # Calculate percentages for each religion
        for religion in religion_data['summary']:
            if religion.lower() != 'none':
                religion_data['summary'][religion]['percentage'] = round(
                    (religion_data['summary'][religion]['adherents'] / total_adherents) * 100, 2
                )
        
        # Get source information
        source = response.xpath('//font[@size="-3"]/text()').extract_first()
        if source:
            religion_data['source'] = source.strip()
        
        return religion_data   