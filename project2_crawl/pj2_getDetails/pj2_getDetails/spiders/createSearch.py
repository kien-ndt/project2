import csv
from neo4j import GraphDatabase
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
def create_searchTimes(session):
    with open('findtimes.csv', mode='r') as csv_file:
        print("\nInsering search times: ")
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            if line_count != 0:
                id = row['movieID']
                times = row['times']
                q = "match(n:Movie) where n.id=\'%s\'" \
                    "merge(m:Search{times:\'%s\'})" \
                    "create(n)-[:BE_FOUND]->(m)" % (id,times)
                a = session.run(q)
            line_count += 1
        print(f'Processed {line_count} lines.')

def create_user_ratings(session):
    with open('ratings.csv', mode='r') as csv_file:
        print("\nInsering user and ratings: ")
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            if line_count != 0:
                userId = row['userId']
                movieId = row['movieId']
                if float(movieId)>10000:
                    continue
                rating = row['rating']
                q = "match(n:Movie) where n.id=\'%s\'" \
                    "merge(m:User{userId:\'%s\'})" \
                    "create(m)-[:RATING{rating:\'%s\'}]->(n)" % (movieId,userId,rating)
                a = session.run(q)
            line_count += 1
        print(f'Processed {line_count} lines.')
create_searchTimes(session)
create_user_ratings(session)
NEO4J_DRIVER.close()