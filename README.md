# CS 511 Research Project: Latency-Aware Distributed Join (R)

To get started, spin up the cluster.
```bash
bash start-all.sh
```

Restart the cluster and rebuild.
```bash
docker-compose -f spark-cluster-compose.yaml down
```

Create Kafka Topic
```bash
docker-compose -f spark-cluster-compose.yaml exec kafka \
    kafka-topics --create \
    --topic network_latency_metrics \                            
    --bootstrap-server localhost:9092 \       
    --partitions 1 \
    --replication-factor 1
```

Submit Spark Job
```bash
docker-compose -f spark-cluster-compose.yaml exec main \ 
    spark-submit \         
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/spark/scripts/kafka_join_optimizer.py
```
