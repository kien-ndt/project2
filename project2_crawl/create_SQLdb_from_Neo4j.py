from neo4j import  GraphDatabase
import pyodbc
NEO4J_DRIVER = GraphDatabase.driver(
            uri="bolt://localhost:7687",
            auth=("neo4j", "123456"),
            encrypted=False,
            max_connection_lifetime=30 * 6000,
            max_connection_pool_size=150000,
            connection_acquisition_timeout=2 * 60,
            connection_timeout=30,
            max_retry_time=1,
        )
session = NEO4J_DRIVER.session()

conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=DESKTOP-Q5MH825;'
                              'Database=Movie1;'
                              'Trusted_Connection=yes;')
cursor = conn.cursor()
count =0
def createMovie():
    count = 0
    q = "match(n:Movie) return n.id, n.link, n.name1, n.name2, n.kind"
    res = session.run(q)
    cursor.execute("truncate table dbo.Movie")
    for i in res:
        try:
            q1 = 'insert into dbo.Movie ' \
                'values(%d,N\'%s\',N\'%s\',N\'%s\',N\'%s\')' % (int(i[0]),str(i[2]).replace('\'','`'),str(i[3]).replace('\'','`'),i[1],i[4])
            cursor.execute(q1)
            conn.commit()
            count+=1
        except:
            print(q)
    print("inserted: ",count)
def createGenres():
    count = 0
    q = "match (n:Genres) return n.name"
    res = session.run(q)
    cursor.execute("truncate table dbo.Genres")
    for i in res:
        try:
            q1 = 'insert into dbo.Genres(name) ' \
                 'values(N\'%s\')' % (str(i[0]))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            print(q)
    print("inserted: ", count)
def createMovie_Genres():
    count = 0
    q = "match(n:Movie) " \
        "with n " \
        "optional match (n)-[:IN_GENRES]-(g) " \
        "return n.id, g.name"
    res = session.run(q)
    cursor.execute("truncate table dbo.Movie_Genres")
    for i in res:
        try:
            q1 = "insert into dbo.Movie_Genres(idMovie,idGenres) " \
                         "select %d, Genres.id from dbo.Genres where dbo.Genres.name=N\'%s\'" % (int(i[0]),str(i[1]))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            print(q)
    print("inserted: ", count)
def createMovie_srcImg():
    count = 0
    q = "match(n:Movie) " \
        "with n " \
        "optional match (n)-[:HAS_SRCIMGLINK]-(g) " \
        "return n.id, g.link"
    res = session.run(q)
    cursor.execute("truncate table dbo.Movie_srcImg")
    for i in res:
        try:
            q1 = "insert into dbo.Movie_srcImg(idMovie,link) " \
                 "values(%d,N\'%s\')" % (int(i[0]), str(i[1]))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            print(q)
    print("inserted: ", count)

def createCompany():
    count = 0
    q = "match (n:Company) return n.name"
    res = session.run(q)
    cursor.execute("truncate table dbo.Company")
    for i in res:
        try:
            q1 = 'insert into dbo.Company(name) ' \
                 'values(N\'%s\')' % (str(i[0]).replace('\'', '`'))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            print(q1)
    print("inserted: ", count)
def createMovie_Company():
    count = 0
    q = "match(n:Movie) " \
        "with n " \
        "optional match (n)-[:MADE_BY]-(g) " \
        "return n.id, g.name"
    res = session.run(q)
    cursor.execute("truncate table dbo.Movie_Company")
    for i in res:
        try:
            q1 = "insert into dbo.Movie_Company(idMovie,idCompany) " \
                 "select %d, Company.id from dbo.Company where dbo.Company.name=N\'%s\'" % (int(i[0]), str(i[1]))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            print(q)
    print("inserted: ", count)

def createMovie_Year():
    count = 0
    q = "match(n:Movie) " \
        "with n " \
        "optional match (n)-[:PRODUCTED_IN]-(g) " \
        "return n.id, g.name"
    res = session.run(q)
    cursor.execute("truncate table dbo.Movie_Year")
    for i in res:
        try:
            q1 = "insert into dbo.Movie_Year(idMovie,year) " \
                 "values(%d,%d)" % (int(i[0]), int(i[1]))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            q1 = "insert into dbo.Movie_Year(idMovie) " \
                 "values(%d)" % (int(i[0]))
            cursor.execute(q1)
            conn.commit()
            count += 1

    print("inserted: ", count)
def createPerson():
    count = 0
    q = "match (n:Person) return n.name"
    res = session.run(q)
    cursor.execute("truncate table dbo.Person")
    for i in res:
        try:
            q1 = 'insert into dbo.Person(name) ' \
                 'values(N\'%s\')' % (str(i[0]).replace('\'', '`'))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            print(q1)
    print("inserted: ", count)

def createMovie_Person_Actor():
    count = 0
    q = "match(n:Movie) " \
        "with n " \
        "optional match (n)-[:ACT_IN]-(g) " \
        "return n.id, g.name"
    res = session.run(q)
    cursor.execute("truncate table dbo.Movie_Person_Actor")
    for i in res:
        try:
            q1 = "insert into dbo.Movie_Person_Actor(idMovie,idPerson) " \
                 "select %d, Person.id from dbo.Person where dbo.Person.name=N\'%s\'" % (int(i[0]), str(i[1]))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            print(q)
    print("inserted: ", count)

def createMovie_Person_Director():
    count = 0
    q = "match(n:Movie) " \
        "with n " \
        "optional match (n)-[:DIRECTED]-(g) " \
        "return n.id, g.name"
    res = session.run(q)
    cursor.execute("truncate table dbo.Movie_Person_Director")
    for i in res:
        try:
            q1 = "insert into dbo.Movie_Person_Director(idMovie,idPerson) " \
                 "select %d, Person.id from dbo.Person where dbo.Person.name=N\'%s\'" % (int(i[0]), str(i[1]))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            print(q)
    print("inserted: ", count)
def createUser():
    count = 0
    q = "match(n:User) RETURN n.userId"
    res = session.run(q)
    cursor.execute("truncate table dbo.UserDB")
    for i in res:
        try:
            q1 = "insert into dbo.UserDB " \
                 "values(%d)" % (int(i[0]))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            print(q1)
    print("inserted: ", count)
def createMovie_Uservote():
    count = 0
    q = "match(n:Movie) " \
        "with n " \
        "match (n)-[r:RATING]-(l:User) " \
        "return n.id, r.rating, l.userId"
    res = session.run(q)
    cursor.execute("truncate table dbo.Movie_Uservote")
    for i in res:
        q1 = "insert into dbo.Movie_Uservote(idMovie,idUser,rating) " \
                 "values(%d,%d,%f)" % (int(i[0]), int(i[2]),float(i[1]))
        try:
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            print(q1)
    print("inserted: ", count)
def createCountry():
    count = 0
    q = "match (n:Country) return n.name"
    res = session.run(q)
    cursor.execute("truncate table dbo.Country")
    for i in res:
        try:
            q1 = 'insert into dbo.Country(name) ' \
                 'values(N\'%s\')' % (str(i[0]).replace('\'', '`'))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            print(q1)
    print("inserted: ", count)
def createMovie_Country():
    count = 0
    q = "match(n:Movie) " \
        "with n " \
        "optional match (n)-[:MADE_IN]-(g) " \
        "return n.id, g.name"
    res = session.run(q)
    cursor.execute("truncate table dbo.Movie_Country")
    for i in res:
        try:
            q1 = "insert into dbo.Movie_Country(idMovie,idCountry) " \
                 "select %d, Country.id from dbo.Country where dbo.Country.name=N\'%s\'" % (int(i[0]), str(i[1]))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            print(q)
    print("inserted: ", count)

def createMovie_Content():
    count = 0
    q = "match(n:Movie) " \
        "with n " \
        "optional match (n)-[:CONTENT]-(g) " \
        "return n.id, g.content"
    res = session.run(q)
    cursor.execute("truncate table dbo.Movie_Content")
    for i in res:
        try:
            q1 = "insert into dbo.Movie_Content(idMovie,content) " \
                 "values(%d,N\'%s\')" % (int(i[0]), str(i[1]).replace('\'','`'))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            q1 = "insert into dbo.Movie_Content(idMovie) " \
                 "values(%d)" % (int(i[0]))
            cursor.execute(q1)
            conn.commit()
            count += 1
    print("inserted: ", count)
def createMovie_IMDb():
    count = 0
    q = "match(n:Movie) " \
        "with n " \
        "match (n)-[k:IMDB]-(g) " \
        "return n.id, k.votes, g.grade"
    res = session.run(q)
    cursor.execute("truncate table dbo.Movie_IMDb")
    for i in res:
        try:
            q1 = "insert into dbo.Movie_IMDb(idMovie,votes,grade) " \
                 "values(%d,%f,%f)" % (int(i[0]), float(i[1]), float(i[2]))
            cursor.execute(q1)
            conn.commit()
            count += 1
        except:
            q1 = "insert into dbo.Movie_IMDb(idMovie,grade) " \
                 "values(%d,%f)" % (int(i[0]), float(i[2]))
            cursor.execute(q1)
            conn.commit()
            count += 1
    print("inserted: ", count)

# createGenres()
# createMovie_Genres()
# createMovie_srcImg()
# createCompany()
# createMovie_Company()
# createMovie_Year()
# createPerson()
# createMovie_Person_Actor()
# createMovie_Person_Director()
# createUser()
# createMovie_Uservote()
# createCountry()
# createMovie_Country()
# createMovie_Content()
createMovie_IMDb()






# for row in cursor.execute(q1).fetchall():
#     id = row[0];
#     name1 = str(row[1]).replace("\'",'`')
#     name2 = str(row[2]).replace("\'",'`')
#     link = row[3];
#     if str(row[4].find('b')>1):
#         kind = "Phim bộ"
#     else:
#         kind = "Phim lẻ"
#     q2 = "merge(a:Movie {id: \"%d\", name1:\"%s\", name2:\"%s\", link:\"%s\",kind:\"%s\"})\n" % (id,name1,name2,link,kind)
#     try:
#         session.run(q2);
#     except:
#         print(id)