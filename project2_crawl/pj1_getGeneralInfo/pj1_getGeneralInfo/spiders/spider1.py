import scrapy
from ..items import Pj1GetgeneralinfoItem
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor, defer
from scrapy.utils.log import configure_logging

class FirstSpider(scrapy.Spider):
    name = 'spider1'
    start_urls=[
        'http://www.phimmoiz.net/phim-bo/'
    ]
    url=[
        'http://www.phimmoiz.net/phim-le/'
    ]
    id = 0
    linknumber=0
    pagenumber=0
    kindfilms="Phim bộ"
    def parse(self, response):
        items = Pj1GetgeneralinfoItem()
        for data in response.xpath("//li[@class='movie-item']"):
            name1 = data.xpath(".//a/div/span[@class='movie-title-1']/text()").extract_first()
            name2 = data.xpath(".//a/div/span[@class='movie-title-2']/text()").extract_first()
            link = 'http://www.phimmoiz.net/' + data.xpath(".//a/@href").extract_first()
            items['id'] = FirstSpider.id
            items['kindfilms'] = FirstSpider.kindfilms
            items['name1'] = name1
            items['name2'] = name2
            items['link'] = link
            FirstSpider.id += 1
            yield items
        next_page = None
        for data in response.xpath("//div/div/ul[@class='pagination pagination-lg']/li/a"):
            if data.xpath(".//text()").extract_first().find('k')>0:
                next_page=data.xpath(".//@href").extract_first()
        if next_page is not None:
            # and FirstSpider.pagenumber < 100
            next_page_link = 'http://www.phimmoiz.net/'+next_page
            FirstSpider.pagenumber += 1
            yield response.follow(next_page_link, callback=self.parse, dont_filter=True)
        elif FirstSpider.linknumber < 1:
            FirstSpider.pagenumber = 0
            FirstSpider.linknumber += 1
            FirstSpider.kindfilms="Phim lẻ"
            items.clear()
            yield scrapy.Request(url=FirstSpider.url[FirstSpider.pagenumber-1], callback=self.parse)



#
# class GetKindSpider(scrapy.Spider):
#     name = 'getkind'
#     start_urls=[
#         'http://www.phimmoi.net/'
#     ]
#     def parse(self, response):
#         for data1 in response.xpath("//ul[@class='sub']/li/a"):
#             # if data.xpath(".//text()").extract_first().find('Phim')>0:
#             # kind = data1.xpath(".//@href").extract_first()
#             # print(kind)
#             print("aaaaaaaaaaaaa")
#             yield {
#                 'kind': data1.xpath(".//@href").extract_first()
#             }

# class SecondSpider(scrapy.Spider):
#     name = 'test'
#     start_urls=[
#         'http://www.phimmoi.net/the-loai/phim-hai-huoc/'
#     ]
#
#     def parse(self, response):
#         items = FilmscrawlerItem()
#         for data in response.xpath("//li[@class='movie-item']"):
#             name1 = data.xpath(".//a/div/span[@class='movie-title-1']/text()").extract_first()
#             name2 = data.xpath(".//a/div/span[@class='movie-title-2']/text()").extract_first()
#             link = 'http://www.phimmoi.net/' + data.xpath(".//a/@href").extract_first()
#             items['name1'] = name1
#             items['name2'] = name2
#             items['link'] = link
#             yield items
#         next_page = None
#         for data in response.xpath("//div/div/ul[@class='pagination pagination-lg']/li/a"):
#             if data.xpath(".//text()").extract_first().find('k')>0:
#                 next_page=data.xpath(".//@href").extract_first()
#         if next_page is not None:
#             next_page_link = 'http://www.phimmoi.net/'+next_page
#             print(next_page_link)
#             yield response.follow(next_page_link, callback=self.parse)
#         else:
#             yield scrapy.Request('http://www.phimmoi.net/the-loai/phim-hanh-dong/', callback=self.parse)

# configure_logging()
# runner = CrawlerRunner(settings={
#     'FEED_FORMAT': 'json',
#     'FEED_URI': 'data.json'
# })
# @defer.inlineCallbacks
# def crawl():
#     yield runner.crawl(FirstSpider)
#     # yield runner.crawl(SecondSpider)
#     # yield runner.crawl(GetKindSpider)
#     reactor.stop()
#
# crawl()
# reactor.run()



