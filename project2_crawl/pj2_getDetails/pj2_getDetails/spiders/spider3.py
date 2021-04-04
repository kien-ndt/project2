import scrapy
from neo4j import GraphDatabase
from ..items import Pj2GetdetailsItem
class GetDetail(scrapy.Spider):
    name = 'spider3'
    start_urls = []
    urls = []
    linknumber=0;
    def __init__(self):
        self.create_connection()
        GetDetail.start_urls.insert(0,GetDetail.urls[0])
    def create_connection(self):
        self.NEO4J_DRIVER = GraphDatabase.driver(
            uri="bolt://localhost:7687",
            auth=("neo4j", "123456"),
            encrypted=False,
            max_connection_lifetime=30 * 6000,
            max_connection_pool_size=150000,
            connection_acquisition_timeout=2 * 60,
            connection_timeout=300000,
            max_retry_time=1,
        )
        self.session = self.NEO4J_DRIVER.session()
        q = "match(n:Movie) return n.link"
        urls = self.session.run(q)
        z=0
        for da in urls:
            GetDetail.urls.insert(z, str(da).lstrip('<Record n.link=\'').rstrip('\'>'))

            # GetDetail.urls.insert(z, da[0])
            z+=1
            print(GetDetail.urls[z-1])
        self.NEO4J_DRIVER.close()

    def start_requests(self):
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        items=Pj2GetdetailsItem()
        data = response.xpath("//dl")
        dataactor = response.xpath("//a[@class='actor-profile-item']/div[@class='actor-name']")
        data0 = response.xpath("//dl/dt[@class='movie-dt']")
        data1 = response.xpath("//dl/dd[@class='movie-dd']")
        data2 = response.xpath("//dl/dd")
        tmp = 0
        tmpnam = 0
        tmpcompany = 0
        for i in data0:
            s = i.xpath(".//text()").extract_first()
            if (s.find('m:')>0):
                tmpnam = tmp
            if (s.find('SX:')>0):
                tmpcompany = tmp
            tmp += 1
        if (tmpcompany>0):
            if (data2[tmpcompany].xpath(".//text()").extract()[0].find('him')>0):           #co chu phim
                tmpcompany +=1
        content=response.xpath("//div[@class='content']/p")
        items['link'] = GetDetail.urls[GetDetail.linknumber],
        items['imdb'] = data.xpath(".//dd[@class='movie-dd imdb']/text()").extract_first()
        items['votes']= data.xpath(".//dd[@class='movie-dd']/text()").extract_first()
        items['director'] = data.xpath(".//dd[@class='movie-dd dd-director']/a/text()").extract()
        items['country'] = data.xpath(".//dd[@class='movie-dd dd-country']/a/text()").extract()
        items['year'] = data1.xpath(".//a/text()").extract_first()
        items['category'] = data.xpath(".//dd[@class='movie-dd dd-cat']/a/text()").extract()
        items['company'] = data2[tmpcompany].xpath(".//text()").extract_first().split(", ")
        items['actor'] = dataactor.xpath(".//span[@class='actor-name-a']/text()").extract()
        items['content'] = content.xpath(".//text()").extract()
        items['srcimg'] = response.xpath("//div[@class='movie-l-img']/img/@src").extract_first()
        yield items