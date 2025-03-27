#!/usr/bin/python 

import sys, time, json, random
from datetime import datetime
from kafka import KafkaProducer, KafkaClient 
from kafka.cluster import ClusterMetadata
from kafka.admin import KafkaAdminClient, NewTopic

def generate_random_event():
    now = datetime.now() # current date and time
    capteur = random.randrange(403, 406)
    if capteur == 405:
    	x = {
		"date": now.strftime("%Y-%m-%d %H:%M:%S"),
		"numero": round(random.uniform(1, 4)),
        	"capteur": "humidite",
        	"valeur": round(random.uniform(50.00, 70.00), 2)
    	}
    if capteur == 403:
    	x = {
		"date": now.strftime("%Y-%m-%d %H:%M:%S"),
		"numero": round(random.uniform(1, 4)),
        	"capteur": "pression",
        	"valeur": round(random.uniform(1015.00, 995.00))
    	}
    if capteur == 404:
    	x = {
		"date": now.strftime("%Y-%m-%d %H:%M:%S"),
		"numero": round(random.uniform(1, 4)),
        	"capteur": "temperature",
        	"valeur": round(random.uniform(10.00, 21.00), 1)
    	}
    return x

def produce_to_kafka(data, topic):
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
    producer = KafkaProducer(
        bootstrap_servers=["kafka:29092"], 
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    producer.send(topic, data)
    producer.flush()

def main():
    topic = "kafka-demo-events"

    kafka_client = KafkaClient(bootstrap_servers='kafka:29092')

    clusterMetadata = kafka_client.cluster
    server_topics = clusterMetadata.topics()

    if topic not in server_topics:
        try:
            print("create new topic :", topic)
            admin = KafkaAdminClient(bootstrap_servers='kafka:29092')

            topic1 = NewTopic(name=topic,
                             num_partitions=1,
                             replication_factor=1)
            admin.create_topics([topic1])
        except Exception:
            pass

    producer = KafkaProducer(bootstrap_servers="kafka:29092")

    while True:
        data = generate_random_event()
        print(data)
        produce_to_kafka(data, topic)
        time.sleep(random.randrange(1, 6))

if __name__ == "__main__":
    main()
