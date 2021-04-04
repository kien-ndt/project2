# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from neo4j import GraphDatabase
# from .spiders.SQLsend import QuerySQL

class Pj1GetgeneralinfoPipeline:
    sqlquery = None
    def __init__(self):
        self.create_connection()
        # self.sqlquery = QuerySQL()
    def create_connection(self):
        self.NEO4J_DRIVER = GraphDatabase.driver(
            uri="bolt://localhost:7687",
            auth=("neo4j", "123456"),
            encrypted=False,
            max_connection_lifetime=30 * 600000,
            max_connection_pool_size=150000000000,
            connection_acquisition_timeout=2 * 60,
            connection_timeout=30000,
            # max_retry_time=5,
        )
        self.session = self.NEO4J_DRIVER.session()
        self.session.run("create(:Kindfilms {name:\"Phim bộ\"})")
        self.session.run("create(:Kindfilms {name:\"Phim lẻ\"})")

    def process_item(self, item, spider):
        self.store_db(item)
        # QuerySQL.createTable_Movie(item['id'],item['name1'],item['name2'],item['link'],item['kindfilms'])
        return item

    def store_db(self, item):
        id = str(item['id'])
        q1 = "match (n:Kindfilms {name:\"" + item['kindfilms'] + "\"})\n"
        q2 = "create(a:Movie {id: \"" + id + "\", name1:\"" + item['name1'] + "\", name2:\"" + item[
            'name2'] + "\", link:\"" + item['link'] + "\",kind:\"" + item['kindfilms'] + "\"})\n"
        self.session.run(q1 + q2)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.NEO4J_DRIVER.close()
