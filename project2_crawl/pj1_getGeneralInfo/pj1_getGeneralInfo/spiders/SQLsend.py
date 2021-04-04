# import pyodbc
# class QuerySQL:
#     conn = pyodbc.connect('Driver={SQL Server};'
#                           'Server=DESKTOP-Q5MH825;'
#                           'Database=MovieDB;'
#                           'Trusted_Connection=yes;')
#     cursor = conn.cursor()
#     def __init__(self):
#         q = "drop table dbo.Movie;"
#         q1 = "create table dbo.Movie (\
#                     id int NOT NULL PRIMARY KEY,\
#                     name1 nvarchar(MAX),\
#                     name2 nvarchar(MAX),\
#                     link nvarchar(MAX),\
#                     kindFilms char(50),\
#                 );"
#         self.cursor.execute(q+q1)
#         self.conn.commit()
#     def createTable_Movie(id, name1, name2, link, kindFilms):
#         q = "insert into dbo.Movie values(%d,N\'%s\',N\'%s\',N\'%s\',N\'%s\')" % (id, name1, name2, link, kindFilms)
#         print(q)
#         QuerySQL.cursor.execute(q)
#         QuerySQL.conn.commit()
