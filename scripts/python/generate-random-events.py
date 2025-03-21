#!/usr/bin/python 

import sys, time, json, random
from datetime import datetime
from kafka import KafkaProducer

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
        bootstrap_servers=["localhost:9092"], 
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    producer.send(topic, data)
    producer.flush()

def main():
    topic = sys.argv[1]
    while True:
        data = generate_random_event()
        print(data)
        produce_to_kafka(data, topic)
        time.sleep(random.randrange(1, 6))

if __name__ == "__main__":
    main()
