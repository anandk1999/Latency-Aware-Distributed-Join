# CS 511 Research Project: Latency-Aware Distributed Join (R)

To get started, install the tables and spin up the cluster.

```bash
pip install gdown
gdown https://drive.google.com/drive/u/1/folders/1SfJhSPCvUfHI2Vc_toVZIIzz1_50LpOx -O . --folder
unzip data/tables.zip
bash start-all.sh
```

To induce latency between the nodes, first, enter main node terminal:

```bash
docker-compose -f spark-cluster-compose.yaml exec main bash
```

Then, from the main node terminal enter one of these commands to induce latency

```bash
bash /opt/spark/scripts/induce_latency.sh
```

Submit Spark Job (in a new terminal)
```bash
docker-compose -f spark-cluster-compose.yaml exec main spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark/scripts/join_optimizer.py
```

Restart the cluster and rebuild.
```bash
docker-compose -f spark-cluster-compose.yaml down
```
<!-- 
Create Kafka Topic
```bash
docker-compose -f spark-cluster-compose.yaml exec kafka \
    kafka-topics --create \
    --topic network_latency_metrics \                            
    --bootstrap-server localhost:9092 \       
    --partitions 1 \
    --replication-factor 1
``` -->
