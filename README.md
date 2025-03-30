# Velozef Big Data Project

# General

This project establishes a big data processing infrastructure to analyze the upcoming data from the [Velozef API](https://www.data.gouv.fr/fr/datasets/velos-a-assistance-electrique-en-libre-service-velozef-sur-brest/#/resources), a company that offers bikes for public usage in Brest and Plouzané, France.

The whole project is containerized and can be deployed directly on Docker.

# Installation

To run the project, you need these simple prerequisites:

* Docker installed
* Docker Compose installed

Lastly, make sure to have Docker running, navigate to the source folder `src`, and run the Docker Compose command to deploy your architecture:

```sh
docker-compose up -d
```

And that's all, your application is ready!

# Steps to Follow:

### Running the Producer

First, you need to run the producer to capture the data sent from the Velozef API and store it in Kafka. To do that, you should run the producer script inside the container.

Run the following command:

```bash
docker exec -it producer python producer_velozef.py
```

### Start the Spark Jobs

After storing the records in Kafka, you need to start the Spark jobs. We have two scripts for this purpose, one for **stream processing** and another for **batch processing**.  
The stream processing script also handles storing data in a Parquet format using a partition of days and hours. This data is used afterward for batch processing, so before executing the second script, you need to run the stream process for a while (24 hours ideally since our goal is to compute metrics for a day, but you can go shorter and have the measures for your specific window).

Run the following command to start the streaming process:

```bash
docker exec -it spark-master python3 speed_layer.py
```

Then, after saving enough data into Parquet format (check the directory `src/spark-scripts/parquet/station_status` to verify your data partition), run the batch process and specify the day you want to run your batch on:

```bash
docker exec -it spark-master python3 batch.py DAY_NUMBER
```

`DAY_NUMBER` can be 27, for example, to process the data of the 27th day of this month.

### Visualize Your Data!
Finally, you can access the Django app to visualize your computations:

Visit the following [URL](http://localhost:8000/).

# Notes:

- If you are using Windows, avoid modifying the script `start-spark.sh`, as it introduces some characters to the script that prevent Bash from running it on Linux containers.

- If your PC resources are low, you can run the `docker-compose-mini`. It deploys the same architecture but without the Spark workers and using only one Cassandra node.

- **IMPORTANT NOTE**: We have set the consistency level to one for both input and output in our scripts with a replication factor of 2. This was done to ensure both configurations `docker-compose` and `docker-compose-mini` work for the same script. In fact, the Spark Cassandra extension needs to read/write data from a quorum of nodes. In the case of two Cassandra nodes, the Cassandra extension must ensure reads and writes to at least two nodes. For this reason, and to make our scripts work for the two architectures, we set the consistency level to one. This way, Spark only requires data written and read to at least one node, which is functional for both cases.
