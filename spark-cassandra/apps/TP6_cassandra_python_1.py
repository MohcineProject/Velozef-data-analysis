from cassandra.cluster import Cluster
clstr=Cluster(['172.22.0.1'])
session=clstr.connect()
session.execute("create keyspace mykeyspace with replication={'class': 'SimpleStrategy', 'replication_factor' : 1};")

qry='''    
CREATE TABLE test.kv1 (
    user text,
    title text ,
    uuid text,
    PRIMARY KEY(uuid)
);'''

# DESCRIBE TABLE test.kv1;

session.execute(qry)  
