import scrapy
import os
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import enum
import json
import math

class BusinessType(enum.Enum):
    RENTAL = enum.auto,
    SALE = enum.auto
    
class ListingType(enum.Enum):
    USED = enum.auto,
    DEVELOPMENT = enum.auto
    
class LocationType(enum.Enum):
    street = enum.auto,
    neighborhood = enum.auto,
    city = enum.auto

def create_url(url, *args, **kwargs):
    url = urlparse(url)
    if args:
        url = url._replace(path=os.path.normpath(os.path.join(url.path or '/', *args)))
    if kwargs:
        params = dict(parse_qsl(url.query))
        params.update(kwargs)
        url = url._replace(query=urlencode(params))
    return urlunparse(url)

class VivarealSpider(scrapy.Spider):
    name = "vivareal"

    def start_requests(self):
        location = getattr(self, 'location', 'São Paulo')
        location_type = LocationType[getattr(self, 'location_type', 'city')]
        business_type = BusinessType[getattr(self, 'business_type', 'SALE')]       
        yield from self.request_location(location, location_type, business_type)

    def request_location(self, location, location_type, business_type):
        yield scrapy.Request(
            create_url(
                "https://glue-api.vivareal.com/v3/locations",
                q=location,
                fields=location_type.name,
                businessType=business_type.name,
                portal="VIVAREAL",
                size=2
            ), 
            self.parse_location)
        
    def parse_location(self, response):
        for type_name, type_data in response.json().items():
            for location in type_data["result"]["locations"]:
                print(f"Scrapping {location['url']}")
                yield from self.request_listing(location)
                return
                
    def request_listing(self, location, limit=200, offset=0):
        yield scrapy.Request(
            create_url(
                "https://glue-api.vivareal.com/v2/listings",
                #addressLocationId=location["address"]["locationId"],
                categoryPage="RESULT",
                sort="pricingInfos.price",
                addressPointLonMin='-47.12',
                addressPointLonMax='-47.12',
                addressPointLat='-22.93,-22.92',
                #unitTypes='APARTMENT',
                **{"from": offset, "size": limit}
            ), 
            self.parse_listing,
            headers={
                "x-domain": "www.vivareal.com.br"
            },
            meta={
                "location": location
            })
        pass
    
    def parse_listing(self, response):
        data = response.json()
        pagination = data["page"]["uriPagination"]
        
        # Yield listings
        yield from data["search"]["result"]["listings"]
        
        # Request Next page
        new_offset = pagination["from"] + pagination["size"]
        page = new_offset / pagination["size"]
        total_pages = math.ceil(pagination['total'] / pagination["size"])
        print(f"Scrapped {new_offset} / {pagination['total']}  (Page {page} / {total_pages}")
        if (new_offset < pagination["total"]):
            yield from self.request_listing(response.meta["location"], pagination["size"], pagination["from"] + pagination["size"])
        
