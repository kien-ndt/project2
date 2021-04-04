# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pyodbc

class Pj3GetdetailssqlPipeline:
    def __init__(self):
        self.create_connection()
        self.clearTable()
    def clearTable(self):
        self.cursor.execute("truncate table dbo.Genres")
        self.cursor.execute("truncate table dbo.Movie_Genres")
        self.cursor.execute("truncate table dbo.Company")
        self.cursor.execute("truncate table dbo.Movie_Company")
        self.cursor.execute("truncate table dbo.Country")
        self.cursor.execute("truncate table dbo.Movie_Country")
        self.cursor.execute("truncate table dbo.Person")
        self.cursor.execute("truncate table dbo.Movie_Person")
        self.cursor.execute("truncate table dbo.Movie_Year")
        self.cursor.execute("truncate table dbo.Movie_Content")
        self.cursor.execute("truncate table dbo.Movie_srcImg")
        self.cursor.execute("truncate table dbo.Movie_IMDb")

        self.conn.commit()
    def create_connection(self):
        self.conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=DESKTOP-Q5MH825;'
                              'Database=MovieDB;'
                              'Trusted_Connection=yes;')
        self.cursor = self.conn.cursor()


    def process_item(self, item, spider):
        self.store_db(item)
        return item
    def store_db(self, item):
        self.store_category(item)
        self.store_company(item)
        self.store_country(item)
        self.store_director(item)
        self.store_actor(item)
        self.store_year(item)
        self.store_content(item)
        self.store_image(item)
        self.store_imdb(item)
    def store_category(self,item):
        if item['category'] is not None:
            for i in item['category']:
                data = i.replace('\'','\\\'')
                q = "select id from Movie where link=\'%s\'" % item['link'][0]
                for row in self.cursor.execute(q):
                    q1 = "insert into dbo.Genres(name)" \
                         "select N\'%s\' Where not exists(select * from Genres where name=N\'%s\')" % (data,data)
                    q2 = "insert into dbo.Movie_Genres(idMovie,idGenres) " \
                         "select %d, Genres.id from dbo.Genres where dbo.Genres.name=N\'%s\'" % (int(row[0]), data)
                    self.cursor.execute(q1)
                    self.conn.commit()
                    self.cursor.execute(q2)
                    self.conn.commit()
                    break
    def store_company(self, item):
        if item['company'] is not None:
            for i in item['company']:
                data = i
                q = "select id from Movie where link=\'%s\'" % item['link'][0]
                for row in self.cursor.execute(q):
                    q1 = "insert into dbo.Company(name)" \
                         "select N\'%s\' Where not exists(select * from Company where name=N\'%s\')" % (data,data)
                    q2 = "insert into dbo.Movie_Company(idMovie,idCompany) " \
                         "select %d, Company.id from dbo.Company where dbo.Company.name=N\'%s\'" % (int(row[0]), data)
                    self.cursor.execute(q1)
                    self.cursor.execute(q2)
                    self.conn.commit()
                    break
    def store_country(self, item):
        if item['country'] is not None:
            for i in item['country']:
                data = i
                q = "select id from Movie where link=\'%s\'" % item['link'][0]
                for row in self.cursor.execute(q):
                    q1 = "insert into dbo.Country(name) " \
                         "select N\'%s\' Where not exists(select * from Country where name=N\'%s\')" % (data, data)
                    q2 = "insert into dbo.Movie_Country(idMovie,idCountry) " \
                         "select %d, Country.id from dbo.Country where dbo.Country.name=N\'%s\'" % (int(row[0]), data)
                    self.cursor.execute(q1)
                    self.conn.commit()
                    self.cursor.execute(q2)
                    self.conn.commit()
                    break
    def store_director(self, item):
        if item['director'] is not None:
            for i in item['director']:
                data = i.replace("\'", "\\(\')")
                q = "select id from Movie where link=\'%s\'" % item['link'][0]
                for row in self.cursor.execute(q):
                    q1 = "insert into dbo.Person(name)" \
                         "select N\'%s\' Where not exists(select * from Person where name=N\'%s\')" % (data,data)
                    q2 = "insert into dbo.Movie_Person(idMovie,idPerson, isDirector) " \
                         "select %d, Person.id, 1 from dbo.Person where dbo.Person.name=N\'%s\'" % (int(row[0]), data)
                    self.cursor.execute(q1)
                    self.cursor.execute(q2)
                    self.conn.commit()
                    break
    def store_actor(self, item):
        if item['actor'] is not None:
            for i in item['actor']:
                data = i.replace("\'", "\\(\')")
                q = "select id from Movie where link=\'%s\'" % item['link'][0]
                for row in self.cursor.execute(q):
                    q1 = "insert into dbo.Person(name)" \
                         "select N\'%s\' Where not exists(select * from Person where name=N\'%s\')" % (data,data)
                    q2 = "insert into dbo.Movie_Person(idMovie,idPerson, isActor) " \
                         "select %d, Person.id, 1 from dbo.Person where dbo.Person.name=N\'%s\'" % (int(row[0]), data)
                    self.cursor.execute(q1)
                    self.cursor.execute(q2)
                    self.conn.commit()
                    break
    def store_year(self, item):
        if item['year'] is not None:
                data = item['year']
                q = "select id from Movie where link=\'%s\'" % item['link'][0]
                for row in self.cursor.execute(q):
                    q1 = "insert into dbo.Movie_Year(idMovie, year)" \
                         "values(%d,\'%s\')" % (int(row[0]),data)
                    self.cursor.execute(q1)
                    self.conn.commit()
                    break
    def store_content(self, item):
        if item['content'] is not None:
            data = ""
            for i in item['content']:
                data += i.replace("\'", "\\(\')")

            q = "select id from Movie where link=\'%s\'" % item['link'][0]
            for row in self.cursor.execute(q):
                q1 = "insert into dbo.Movie_Content(idMovie, content)" \
                        "values(%d,\'%s\')" % (int(row[0]),data)
                self.cursor.execute(q1)
                self.conn.commit()
                break

    def store_image(self, item):
        if item['srcimg'] is not None:
                data = item['srcimg']
                q = "select id from Movie where link=\'%s\'" % item['link'][0]
                for row in self.cursor.execute(q):
                    q1 = "insert into dbo.Movie_srcImg(idMovie, link)" \
                         "values(%d,\'%s\')" % (int(row[0]),data)
                    self.cursor.execute(q1)
                    self.conn.commit()
                    break
    def store_imdb(self, item):
        if item['imdb'] is not None and (float(item['imdb'])<=10.0):
                data = item['imdb']
                q = "select id from Movie where link=\'%s\'" % item['link'][0]
                for row in self.cursor.execute(q):
                    vote = item['votes'].replace(",", "").replace("vote", "").replace("s", "").strip(" () ")
                    if vote.isdigit and vote.find('p') == -1:
                        q1 = "insert into dbo.Movie_IMDb(idMovie, grade, votes)" \
                             "values(%d,%f,%f)" % (int(row[0]),float(data),float(vote))
                    else:
                        q1 = "insert into dbo.Movie_IMDb(idMovie, grade)" \
                             "values(%d,%f)" % (int(row[0]), float(data))
                    self.cursor.execute(q1)
                    self.conn.commit()
                    break
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()