# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Pj2GetdetailsItem(scrapy.Item):
    link = scrapy.Field()
    country = scrapy.Field()
    category = scrapy.Field()
    director = scrapy.Field()
    actor = scrapy.Field()
    year = scrapy.Field()
    company = scrapy.Field()
    content = scrapy.Field()
    srcimg = scrapy.Field()
    imdb = scrapy.Field()
    votes = scrapy.Field()
    pass
