# Architecture Kappa with Docker & docker-compose

# General

A simple spark standalone cluster for your testing environment purposses. A *docker-compose up* away from you solution for your spark development environment.

The Docker compose will create the following containers:

container|Exposed ports
---|---
spark-master|9090 7077
spark-worker-1|9091
spark-worker-2|9092
demo-database|5432

# Installation

The following steps will make you run your spark cluster's containers.

## Pre requisites

* Docker installed

* Docker compose  installed

## Build the image


```sh
docker build -t cluster-apache-spark:3.0.2 .
```

## Run the docker-compose

The final step to create your test cluster will be to run the compose file:

```sh
docker-compose up -d
```

## Validate your cluster

Just validate your cluster accesing the spark UI on each worker & master URL.

### Spark Master

http://localhost:9090/

![alt text](./images/spark-master.png "Spark master UI")

### Spark Worker 1

http://localhost:9091/

![alt text](./images/spark-worker-1.png "Spark worker 1 UI")

### Spark Worker 2

http://localhost:9092/

![alt text](./images/spark-worker-2.png "Spark worker 2 UI")


# Resource Allocation 

This cluster is shipped with three workers and one spark master, each of these has a particular set of resource allocation(basically RAM & cpu cores allocation).

* The default CPU cores allocation for each spark worker is 1 core.

* The default RAM for each spark-worker is 1024 MB.

* The default RAM allocation for spark executors is 256mb.

* The default RAM allocation for spark driver is 128mb

* If you wish to modify this allocations just edit the env/spark-worker.sh file.

# Binded Volumes

To make app running easier I've shipped two volume mounts described in the following chart:

Host Mount|Container Mount|Purposse
---|---|---
apps|/opt/spark-apps|Used to make available your app's jars on all workers & master
data|/opt/spark-data| Used to make available your app's data on all workers & master

This is basically a dummy DFS created from docker Volumes...(maybe not...)

# Run Sample applications

```sh
docker exec -it tp3_docker-spark-worker-a bash
```

To submit the app connect to one of the workers or the master and execute:

```sh
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--driver-memory 1G \
--executor-memory 1G \
/opt/spark-apps/TP3_exercice1_DataFrame.py
```

```sh
/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
/opt/spark-apps/TP3_exercice1_DataFrame.py
```

```sh
python3 /opt/spark-apps/TP3_exercice1_DataFrame.py
```

![alt text](./images/pyspark-demo.png "Spark UI with pyspark program running")



# Summary

* We compiled the necessary docker image to run spark master and worker containers.

* We created a spark standalone cluster using 2 worker nodes and 1 master node using docker && docker-compose.

* Copied the resources necessary to run demo applications.

* We ran a distributed application at home(just need enough cpu cores and RAM to do so).

# Why a standalone cluster?

* This is intended to be used for test purposes, basically a way of running distributed spark apps on your laptop or desktop.

* This will be useful to use CI/CD pipelines for your spark apps(A really difficult and hot topic)

# Steps to connect and use a pyspark shell interactively

* Follow the steps to run the docker-compose file. You can scale this down if needed to 1 worker. 

```sh
docker exec -it tp3_docker-spark-worker-a bash
apt update
apt install python3-pip
pip3 install pyspark
pyspark
```

# What's left to do?

* Right now to run applications in deploy-mode cluster is necessary to specify arbitrary driver port.

* The spark submit entry in the start-spark.sh is unimplemented, the submit used in the demos can be triggered from any worker