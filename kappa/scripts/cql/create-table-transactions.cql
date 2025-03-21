CREATE KEYSPACE IF NOT EXISTS demo 
WITH replication = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 1
};
	
CREATE TABLE IF NOT EXISTS demo.transactions (
   date int,
   numero int,
   pression int,
   temperature float,
   humidite float,
   PRIMARY KEY (date)
);

CREATE TABLE IF NOT EXISTS demo.transactions1 (
   date text,
   numero int,
   capteur text,
   valeur float,
   PRIMARY KEY (date)
);
