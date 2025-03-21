from cassandra.cluster import Cluster
clstr=Cluster(['172.22.0.3'])
session=clstr.connect()

qry=''' 
CREATE KEYSPACE IF NOT EXISTS demo 
WITH replication = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};'''
	
session.execute(qry) 

qry=''' 
CREATE TABLE IF NOT EXISTS demo.transactions1 (
   date text,
   numero int,
   capteur text,
   valeur float,
   PRIMARY KEY (numero)
);'''

session.execute(qry) 
