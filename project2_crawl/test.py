
from neo4j import GraphDatabase
NEO4J_DRIVER = GraphDatabase.driver(
            uri="bolt://localhost:7687",
            auth=("neo4j", "123456"),
            encrypted=False,
            max_connection_lifetime=30 * 6000,
            max_connection_pool_size=150000,
            connection_acquisition_timeout=2 * 60,
            connection_timeout=300000,
            max_retry_time=1,
        )
session = NEO4J_DRIVER.session()
q = "match(n:Movie) return n.link"
urls = session.run(q)
z=0
for da in urls:
        print(da[0])
            # print(str(da).lstrip('<Record n.link=\'').rstrip('\'>'))
NEO4J_DRIVER.close()