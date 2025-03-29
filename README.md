# Velozef Big data project

# General

This project establishes a big data processing infrastructure to analyze the upcoming data from the [ velozef API ](https://www.data.gouv.fr/fr/datasets/velos-a-assistance-electrique-en-libre-service-velozef-sur-brest/#/resources), a company that offers bikes for public usage in Brest and Plouzané at France. 

The whole project is containerized and can be deployed directly on docker.

# Installation

To run the project you need these simple prerequisites : 

* Docker installed

* Docker compose  installed

Lastly, make sure to have docker on, navigate to the source folder `src`, and run the docker-compose command to deploy your architecture :

```sh
docker-compose up -d
```

And that's all, your application is ready ! 


# Steps to follow :

### Running the producer 

First you need to run the producer to capture the data sent from the velozef API and store it in kafka. To do that you should run the producer script inside the container.

Run the following command : 

```bash
 docker exec -it producer python producer_velozef.py
```

### Start the spark jobs 

After storing the records in kafka, you need to start the spark jobs. We have two scripts for this purpose, one for **stream processing** and another for **batch processing**.
The stream processing script also handles storing data in a parquet format using a partition of days and hours. This data is used afterwards for batch processing, so before executing the second script you need to run the stream process for a while ( 24h idealy since our goal is to compute metrics for a day, but you can go shorter and have the mesures for your specific window ).

Run the following command to start the streaming process : 

```bash
docker exec -it spark-master python3 speed_layer.py 
```

Then after saving enough data into parquet format ( check the directory src/spark-scripts/parquet/station_status to check your data partition), run the batch process : 

```bash
 docker exec -it spark-master  python3 batch.py 
```

### Visualize your data ! 
Finally, you can access the django app to visualize your computations : 

Visit the following (URL)[http://localhost:8000/]


# Notes :

- If you are using windows, avoid modifying the script start-spark.sh, as it introduces some characters to the script that that prevents bash from running it on linux containers.

- If your pc ressources are low you can run the `docker-compose-mini`. It deploys the same architecture but without the spark workers and using only one cassandra node. 

- **IMPORTANT NOTE** : We have set the consistency level to one for both input and output in our scripts with a replication factor of 2. This was done to ensure both configurations `docker-compose` and `docker-compose-mini` work for the same script. In fact, the spark cassandra extension needs to read/write data from a quorum of nodes. In the case of two cassandra nodes, the cassandra extension must ensure reads and writes to at least two nodes. For this reason, and to make our scripts working for the two architectures, we set the consistency level to one, this way spark only requires data written and read to at least one node, which is functionnal for both cases.
