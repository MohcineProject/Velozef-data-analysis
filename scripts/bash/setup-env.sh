#!/bin/bash

# KAFKA
nohup $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties & > /dev/null && sleep 10
nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties & > /dev/null && sleep 10

$KAFKA_HOME/bin/kafka-topics.sh \
--create \
--bootstrap-server localhost:9092 \
--replication-factor 1 \
--partitions 1 \
--topic spark-demo-events

# CASSANDRA
nohup $CASSANDRA_HOME/bin/cassandra -R & > /dev/null && sleep 40

$CASSANDRA_HOME/bin/cqlsh -f /spark-demo/scripts/cql/create-table-transactions.cql
