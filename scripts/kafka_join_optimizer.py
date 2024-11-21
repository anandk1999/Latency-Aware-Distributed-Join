from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from kafka import KafkaConsumer
import json
import threading
import time
import ast

class KafkaLatencyAwareJoinOptimizer:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.current_latency_matrix = {}
        self.latency_lock = threading.Lock()
        
        # Start Kafka consumer in background
        self.start_latency_listener()

    def start_latency_listener(self):
        """Start a background thread to consume Kafka latency metrics"""
        def consume_latency():
            consumer = KafkaConsumer(
                'network_latency_metrics',
                bootstrap_servers=['kafka:29092'],
                group_id='join-optimizer-group',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

            for message in consumer:
                try:
                    # Thread-safe update of latency matrix
                    with self.latency_lock:
                        self.current_latency_matrix = message.value.get('latency_metrics', {})
                        print("Updated latency metrics received")
                except Exception as e:
                    print(f"Error processing Kafka message: {e}")

        # Start Kafka consumer in a daemon thread
        threading.Thread(target=consume_latency, daemon=True).start()

    def choose_join_strategy(self, left_size, right_size):
        """
        Dynamically choose join strategy based on:
        1. Table sizes
        2. Current network latency
        """
        # Get average latency
        avg_latency = self._get_average_latency()
        
        # Broadcast join heuristics
        if (left_size < 1000 or right_size < 1000) and avg_latency < 30:
            return 'broadcast'
        
        # Shuffle join for larger tables or high latency
        return 'shuffle'

    def _get_average_latency(self):
        """Calculate average network latency"""
        with self.latency_lock:
            if not self.current_latency_matrix:
                return 20  # Default reasonable latency
            
            all_latencies = [
                latency 
                for source in self.current_latency_matrix 
                for latency in self.current_latency_matrix[source].values()
            ]
            
            return sum(all_latencies) / len(all_latencies) if all_latencies else 20

    def distributed_join(self, left_df, right_df, join_key):
        """
        Perform a distributed join with latency-aware strategy
        """
        # Estimate table sizes
        left_size = left_df.count()
        right_size = right_df.count()

        # Choose join strategy
        strategy = self.choose_join_strategy(left_size, right_size)

        # Apply appropriate join strategy
        if strategy == 'broadcast':
            print(f"Using Broadcast Join (Left: {left_size}, Right: {right_size})")
            return left_df.join(broadcast(right_df), left_df[join_key] == right_df[join_key])
        else:
            print(f"Using Shuffle Join (Left: {left_size}, Right: {right_size})")
            return left_df.join(right_df, left_df[join_key] == right_df[join_key])

def main():
    # Create Spark Session with Kafka dependencies
    spark = SparkSession.builder \
        .appName("Kafka-Enabled Latency-Aware Distributed Join") \
        .master("spark://main:7077") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    # Create sample distributed dataframes
    left_df = spark.createDataFrame([
        (1, "Alice", 500), (2, "Bob", 600), (3, "Charlie", 700)
    ], ["id", "name", "value"])

    right_df = spark.createDataFrame([
        (1, 25, "Tech"), (2, 30, "Finance"), (3, 35, "Marketing")
    ], ["id", "age", "department"])

    # Initialize Latency-Aware Join Optimizer
    join_optimizer = KafkaLatencyAwareJoinOptimizer(spark)

    # Perform distributed join
    result_df = join_optimizer.distributed_join(left_df, right_df, "id")

    # Show results
    result_df.show()

    # Keep the application running to observe Kafka metrics
    time.sleep(120)

    spark.stop()

if __name__ == "__main__":
    main()