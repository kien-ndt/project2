# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from neo4j import GraphDatabase


class Pj2GetdetailsPipeline(object):
    def __init__(self):
        self.create_connection()

    def create_connection(self):
        self.NEO4J_DRIVER = GraphDatabase.driver(
            uri="bolt://localhost:7687",
            auth=("neo4j", "123456"),
            encrypted=False,
            max_connection_lifetime=30 * 600000,
            max_connection_pool_size=1500000000,
            connection_acquisition_timeout=2 * 6000,
            connection_timeout=3000,
            max_retry_time=2,
        )
        self.session = self.NEO4J_DRIVER.session()

    def process_item(self, item, spider):
        self.store_db(item)
        return item

    def store_db(self, item):
        self.store_country(item)
        self.store_category(item)
        self.store_director(item)
        self.store_actor(item)
        self.store_year(item)
        self.store_company(item)
        self.store_content(item)
        self.store_image(item)
        self.store_imdb(item)

    def store_country(self,item):
        q1 = "match(a:Movie) where a.link=\'" + item['link'][0] +"\'\n"

        q2="match(b:Country) where b.name=\'"+ item['country'][0] + "\' return b\n"
        b=self.session.run(q2)
        flag = None
        for i in b:
            flag = 1
        if flag is None:
            q3="create (b:Country{name:\'"+item['country'][0]+"\'}), (a)-[:MADE_IN]->(b)"
        else:
            q3="match(b:Country) where b.name=\'"+ item['country'][0] + "\'\n create (a)-[:MADE_IN]->(b)"
            flag = None
        self.session.run(q1+q3)

    def store_category(self,item):
        for i in item['category']:
            data = i
            q1 = "match(a:Movie) where a.link=\'" + item['link'][0] + "\'\n"
            q2 = "match(b:Genres) where b.name=\'" + data + "\' return b\n"
            b = self.session.run(q2)
            flag = None
            for i in b:
                flag = 1
            if flag is None:
                q3 = "create (b:Genres{name:\'" + i + "\'}), (a)-[:IN_GENRES]->(b)"
            else:
                q3="match(b:Genres) where b.name=\'"+ data + "\'\n create (a)-[:IN_GENRES]->(b)"
                flag = None
            self.session.run(q1 + q3)

    def store_director(self,item):
        for k in item['director']:
            data = k
            if data is not None:
                q1 = "match(a:Movie) where a.link=\'" + item['link'][0] + "\'\n"
                q2 = "match(b:Person) where b.name=\'" + data + "\' return b\n"
                b = self.session.run(q2)
                flag = None
                for i in b:
                    flag = 1
                if flag is None:
                    q3 = "create (b:Person{name:\'" + data + "\'}), (b)-[:DIRECTED]->(a)"
                else:
                    q3="match(b:Person) where b.name=\'"+ data + "\'\n create (b)-[:DIRECTED]->(a)"
                    flag = None
                self.session.run(q1 + q3)

    def store_actor(self,item):
        for k in item['actor']:
            data = k
            q1 = "match(a:Movie) where a.link=\'" + item['link'][0] + "\'\n"
            q2 = "match(b:Person) where b.name=\'" + data + "\' return b\n"
            b = self.session.run(q2)
            flag = None
            for i in b:
                flag = 1
            if flag is None:
                q3 = "create (b:Person{name:\'" + data + "\'}), (b)-[:ACT_IN]->(a)"
            else:
                q3="match(b:Person) where b.name=\'"+ data + "\'\n create (b)-[:ACT_IN]->(a)"
                flag = None
            self.session.run(q1 + q3)
    def store_year(self,item):
        data1 = str(item['year']).lstrip('(\'')
        data = data1.rstrip('\',)')
        print(data)
        if data is not None:
            q1 = "match(a:Movie) where a.link=\'" + item['link'][0] + "\'\n"
            q2 = "match(b:Year) where b.name=\'" + data + "\' return b\n"
            b = self.session.run(q2)
            flag = None
            for i in b:
                flag = 1
            if flag is None:
                q3 = "create (b:Year{name:\'" + data + "\'}), (a)-[:PRODUCTED_IN]->(b)"
            else:
                q3 = "match(b:Year) where b.name=\'" + data + "\'\n create (a)-[:PRODUCTED_IN]->(b)"
                flag = None
            self.session.run(q1 + q3)
    def store_imdb(self,item):
        if item['imdb'] is not None and (float(item['imdb'])<=10.0):
            data= item['imdb']
            q1 = "match(a:Movie) where a.link=\'" + item['link'][0] + "\'\n"

            vote = item['votes'].replace(",","").replace("vote","").replace("s","").strip(" () ")
            # if vote.isdigit and vote.find('p')==-1 and vote.find('/'==-1):
            q2 = "merge(b:IMDb {grade:\'" + data + "\'}) create (a)-[r:IMDB{votes:\'%s\'}]->(b)" %vote
            # else:
            #     q2 = "merge(b:IMDb {grade:\'" + data + "\'}) create (a)-[r:IMDB]->(b)"
            self.session.run(q1+q2)

    def store_company(self, item):
        for i in item['company']:
            data = i
            q1 = "match(a:Movie) where a.link=\'" + item['link'][0] + "\'\n"
            q2 = "match(b:Company) where b.name=\'" + data + "\' return b\n"
            b = self.session.run(q2)
            flag = None
            for i in b:
                flag = 1
            if flag is None:
                q3 = "create (b:Company{name:\'" + i + "\'}), (a)-[:MADE_BY]->(b)"
            else:
                q3="match(b:Company) where b.name=\'"+ data + "\'\n create (a)-[:MADE_BY]->(b)"
                flag = None
            self.session.run(q1 + q3)

    def store_content(self, item):
        data = ""
        for i in item['content']:
            data+=i.replace("\'","\\(\')")
        q1 = "match(a:Movie) where a.link=\'" + item['link'][0] +"\'\n"
        q2 = "create(c:Content{content:\'%s\'})-[:CONTENT]->(a)" % data
        self.session.run(q1+q2)

    def store_image(self, item):
        q1 = "match(a:Movie) where a.link=\'" + item['link'][0] + "\'\n"
        q2 = "create(c:SrcImg{link:\'%s\'})<-[:HAS_SRCIMGLINK]-(a)" % item['srcimg']
        self.session.run(q1+q2)
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.NEO4J_DRIVER.close()