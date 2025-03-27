import json
import time
import urllib.request
import json
from datetime import datetime
from kafka import KafkaProducer, KafkaClient 
from kafka.admin import KafkaAdminClient, NewTopic

def main():
    """_summary_
    
    Returns:
        _type_: _description_
    """    
    url = "https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/station_information.json"
    urls = ['https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/station_information.json',
              'https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/system_information.json',
              'https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/station_status.json',
              "https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/vehicle_types.json",
              "https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/free_bike_status.json",
             "https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/system_pricing_plans.json",

    ]

    # topic = sys.argv[1]
    topic = 'velozef-station-information'
    
    kafka_client = KafkaClient(bootstrap_servers='kafka:29092')
    
    admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
    server_topics = admin.list_topics()

    topic = 'velozef-station-information'
    num_partition = 2

    print(server_topics)
    # création du topic si celui-ci n'est pas déjà créé
    if topic not in server_topics:
        try:
            print("create new topic :", topic)

            topic1 = NewTopic(name=topic,
                             num_partitions=num_partition,
                             replication_factor=1)
            admin.create_topics([topic1])
        except Exception:
            print("error")
            pass
    else:
        print(topic,"est déjà créé")

    producer = KafkaProducer(bootstrap_servers="kafka:29092")

    while True:
        response = urllib.request.urlopen(url)
        stations = json.loads(response.read().decode())
        stations = stations['data']['stations']
        for station in stations:
            producer.send(topic, json.dumps(stations).encode())
            
        print("{} Produced {} station records".format(datetime.fromtimestamp(time.time()), len(stations)))
        time.sleep(60)

if __name__ == "__main__":
    main()
