import scrapy
import os
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import enum
import json
import math
import logging

from dataclasses import dataclass

MAX_LISTINGS = 1000

@dataclass
class Viewport:
    north: float
    south: float
    east: float
    west: float
    path: tuple = ()
    
    @property
    def path_str(self):
        return "->".join(self.path or ["root"])

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
        #location = getattr(self, 'location', 'São Paulo')
        #location_type = LocationType[getattr(self, 'location_type', 'city')]
        #business_type = BusinessType[getattr(self, 'business_type', 'SALE')]       
        -22.792271, -47.156545
        -22.999883, -46.959713
        #yield from self.request_listing(viewport=Viewport(south=-22.856665, north=-22.850910, west=-47.054845, east=-47.045173))
        yield from self.request_listing(viewport=Viewport(south=-22.999883, north=-22.792271, west=-47.156545, east=-46.959713))

    #def request_location(self, location, location_type, business_type):
        #yield scrapy.Request(
            #create_url(
                #"https://glue-api.vivareal.com/v3/locations",
                #q=location,
                #fields=location_type.name,
                #businessType=business_type.name,
                #portal="VIVAREAL",
                #size=2
            #), 
            #self.parse_location)
        
    #def parse_location(self, response):
        #for type_name, type_data in response.json().items():
            #for location in type_data["result"]["locations"]:
                #print(f"Scrapping {location['url']}")
                #yield from self.request_listing(location=location)
                #return
                
    def request_listing(self, filters={}, viewport=None, limit=200, offset=0):
        yield scrapy.Request(
            create_url(
                "https://glue-api.vivareal.com/v2/listings",
                categoryPage="RESULT",
                sort="pricingInfos.price",
                viewport=f"{viewport.east:.6f},{viewport.north:.6f}|{viewport.west:.6f},{viewport.south:.6f}",
                **filters,
                **{"from": offset, "size": limit}
            ), 
            self.parse_listing,
            headers={
                "x-domain": "www.vivareal.com.br"
            },
            meta={
                "filters": filters,
                "viewport": viewport
            })
        pass
    
    
    def parse_listing(self, response):
        data = response.json()
        pagination = data["page"]["uriPagination"]
        filters = viewport=response.meta["filters"]
        viewport = viewport=response.meta["viewport"]
                
        if pagination['total'] > MAX_LISTINGS:
            logging.info(f"Viewport {viewport.path_str} has {pagination['total']} listings, which is more than the max allowed {MAX_LISTINGS} -- Breaking up in smaller viewports")
            # Split viewport in smaller sections
            center_lng = (viewport.west + viewport.east) / 2
            center_lat = (viewport.north + viewport.south) / 2
            nested_viewports = [
                Viewport(path=viewport.path + ("nw",), south=center_lat, north=viewport.north, west=viewport.west, east=center_lng),
                Viewport(path=viewport.path + ("ne",), south=center_lat, north=viewport.north, west=center_lng, east=viewport.east),
                Viewport(path=viewport.path + ("sw",), south=viewport.south, north=center_lat, west=viewport.west, east=center_lng),
                Viewport(path=viewport.path + ("se",), south=viewport.south, north=center_lat, west=center_lng, east=viewport.east)
            ]
            for nested_viewport in nested_viewports:
                assert viewport.south < viewport.north
                assert viewport.west < viewport.east
                yield from self.request_listing(filters=filters, viewport=nested_viewport)
                
        else:
            # Yield listings
            listings = data["search"]["result"]["listings"] + data["superPremium"]["search"]["result"]["listings"]
            yield from listings
            
            # Request Next page
            new_offset = pagination["from"] + len(listings)
            logging.info(f"Viewport {viewport.path_str}: scrapped {new_offset} / {pagination['total']}")
            if (new_offset < pagination["total"]):
                # Need to lookup the next page
                yield from self.request_listing(filters=filters, viewport=viewport, limit=pagination["size"], offset=pagination["from"] + pagination["size"])
