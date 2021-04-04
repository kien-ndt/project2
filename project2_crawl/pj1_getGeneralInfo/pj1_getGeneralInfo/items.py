# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Pj1GetgeneralinfoItem(scrapy.Item):
    kindfilms = scrapy.Field()
    id = scrapy.Field()
    name1 = scrapy.Field()
    name2 = scrapy.Field()
    link = scrapy.Field()
    pass
