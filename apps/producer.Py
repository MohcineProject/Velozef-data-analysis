from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic 
import urllib.request
import json 

station_information_URL = "https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/station_status.json"
STATION_INFORMATION = "station_information"
KAFKA_URL= "kafka:9092"

# Creating the required topics 
def create_topics() : 

    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_URL,
    )

    num_partition=1 

    server_topics = admin_client.list_topics() 

    if STATION_INFORMATION not in server_topics : 
        try : 
            print("[Producer] Creating station_status topic : ")
            new_topic = NewTopic(name=STATION_INFORMATION,num_partition=num_partition, replication_factor=1)
            admin_client.create_topics([new_topic])
        except Exception:
            print ("[Producer] error creating the kafka topic ") 



# Send json data to topics 
def send_json_to_kafka(topic , url) : 
    producer = KafkaProducer(bootstrap_servers=KAFKA_URL)
    while True : 
        response = urllib.request.urlopen(url)
        json_response = json.loads(response.read().decode()) 
        stations = json_response["data"]["stations"]
        for station in stations : 
            producer.send(topic, json.dumps(station).encode()) 
        


if __name__ == "main" : 
    create_topics()
    send_json_to_kafka(STATION_INFORMATION , station_information_URL )

