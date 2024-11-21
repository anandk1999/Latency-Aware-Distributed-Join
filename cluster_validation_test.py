from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum
import time

def test_spark_cluster_functionality():
    """
    Comprehensive test suite to validate Spark cluster functionality
    """
    # Create Spark Session
    spark = SparkSession.builder \
        .appName("Cluster Validation Test") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    print("===== Spark Cluster Validation Tests =====")

    # 1. Cluster Connection Test
    try:
        print("\n1. Spark Cluster Connection Test")
        workers = spark.sparkContext.getConf().get("spark.executor.instances")
        print(f"Number of Spark Workers: {workers}")
        assert int(workers) >= 2, "Insufficient worker nodes"
    except Exception as e:
        print(f"Cluster Connection Test Failed: {e}")
        return False

    # 2. Distributed Data Processing Test
    try:
        print("\n2. Distributed Data Processing Test")
        # Generate large distributed dataset
        data = spark.range(0, 1000000).toDF("id")
        data = data.withColumn("group", col("id") % 10)
        
        # Perform distributed aggregation
        result = data.groupBy("group") \
            .agg(
                avg("id").alias("avg_id"),
                sum("id").alias("total")
            )
        
        print("Distributed Aggregation Results:")
        result.show()
        
        # Validate results
        assert result.count() == 10, "Incorrect grouping"
    except Exception as e:
        print(f"Distributed Processing Test Failed: {e}")
        return False

    # 3. Kafka Integration Test
    try:
        print("\n3. Kafka Streaming Integration Test")
        # Read from Kafka topic
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "network_latency_metrics") \
            .load()
        
        # Simple query to validate Kafka stream
        query = kafka_df \
            .selectExpr("CAST(value AS STRING)") \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
        
        # Run for 10 seconds
        time.sleep(10)
        query.stop()
        
        print("Kafka Streaming Test Completed")
    except Exception as e:
        print(f"Kafka Integration Test Failed: {e}")
        return False

    # 4. Network Latency Metrics Test
    try:
        print("\n4. Network Latency Metrics Validation")
        from kafka import KafkaConsumer
        import json

        consumer = KafkaConsumer(
            'network_latency_metrics',
            bootstrap_servers=['kafka:29092'],
            auto_offset_reset='latest',
            group_id='cluster-validation-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # Collect metrics for 5 seconds
        start_time = time.time()
        metrics_received = 0
        
        for message in consumer:
            metrics = message.value
            print("Latency Metrics Sample:", metrics)
            metrics_received += 1
            
            if time.time() - start_time > 5 or metrics_received >= 3:
                break
        
        assert metrics_received > 0, "No latency metrics received"
        print(f"Received {metrics_received} latency metric messages")
    except Exception as e:
        print(f"Latency Metrics Test Failed: {e}")
        return False

    print("\n===== All Cluster Validation Tests PASSED =====")
    spark.stop()
    return True

def main():
    # Run validation tests
    test_result = test_spark_cluster_functionality()
    
    # Exit with appropriate status code
    exit(0 if test_result else 1)

if __name__ == "__main__":
    main()