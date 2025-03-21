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
    # ]
    # stockage dans un dictionnaire des urls des fichiers ainsi que la clé d'accès aux données d'intérêt
    topics_urls_keys = {
        "station_information":['https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/station_information.json','stations'],
        "station_status":['https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/station_status.json','stations'],
        "vehicle_types":['https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/vehicle_types.json','vehicle_types'],
        "free_bike_status":['https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/free_bike_status.json','bikes'],
        "system_pricing_plans":['https://gbfs.partners.fifteen.eu/gbfs/2.2/brest/en/system_pricing_plans.json','plans']
    }
    # topic = sys.argv[1]
    
    kafka_client = KafkaClient(bootstrap_servers='kafka:29092')
    
    admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
    server_topics = admin.list_topics()
    num_partition = 2
    # création du topic si celui-ci n'est pas déjà créé
    for topic, _ in topics_urls_keys.items():
        
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
    while True : 
        for topic, url_key_list in topics_urls_keys.items():
            response = urllib.request.urlopen(url_key_list[0])
            data = json.loads(response.read().decode())
            data = data['data'][str(url_key_list[1])]
            for row in data:
                # print('row:',row)   
                producer.send(topic, json.dumps(row).encode())
            print("{} Produced {} station records".format(datetime.fromtimestamp(time.time()), len(data)))
        time.sleep(60)

if __name__ == "__main__":
    main()
