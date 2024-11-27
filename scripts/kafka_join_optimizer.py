from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import json
import threading
import time
import ast
import subprocess
import platform

from pyspark.sql.types import *

def estimate_dataframe_size(df):
    # Get approximate row count
    approx_rows = df.rdd.countApprox(timeout=1000, confidence=0.95)
    
    # Calculate estimated size per row
    def get_type_size(dataType):
        """Estimate size of different data types"""
        if isinstance(dataType, ByteType):
            return 1
        elif isinstance(dataType, BooleanType):
            return 1
        elif isinstance(dataType, IntegerType):
            return 4
        elif isinstance(dataType, LongType):
            return 8
        elif isinstance(dataType, FloatType):
            return 4
        elif isinstance(dataType, DoubleType):
            return 8
        elif isinstance(dataType, StringType):
            return 100  # Approximate average string length
        elif isinstance(dataType, TimestampType):
            return 8
        elif isinstance(dataType, DateType):
            return 4
        elif isinstance(dataType, ArrayType):
            # Recursively calculate for array elements
            return 8 + get_type_size(dataType.elementType)
        elif isinstance(dataType, StructType):
            # Sum sizes of struct fields
            return sum(get_type_size(field.dataType) for field in dataType.fields)
        else:
            return 16  # Default fallback size
    
    # Calculate estimated size per row
    row_size = sum(get_type_size(field.dataType) for field in df.schema.fields)
    
    # Estimate total size in bytes
    estimated_size_bytes = approx_rows * row_size
    
    return {
        'approximate_rows': approx_rows,
        'estimated_row_size_bytes': row_size,
        'estimated_total_size_bytes': estimated_size_bytes,
        'estimated_total_size_mb': estimated_size_bytes / (1024 * 1024)
    }




class NetworkLatencyMetricsCollector:
    def __init__(self, target_hosts=None):
        self.latency_metrics = {}
        self.stop_event = threading.Event()
        
        if target_hosts is None:
            self.target_hosts = [
                '192.168.10.3',   # Worker 1
                '192.168.10.4'    # Worker 2
            ]
        else:
            self.target_hosts = target_hosts
        
        # Lock for thread-safe metric access
        self.metrics_lock = threading.Lock()

    def ping_host(self, host, count=5):
        """
        Measure network latency to a specific host using ping
        
        Args:
            host (str): IP address or hostname to ping
            count (int): Number of ping attempts
        
        Returns:
            dict: Latency metrics for the host
        """
        try:
            # Determine ping command based on OS
            if platform.system().lower() == "windows":
                ping_cmd = ["ping", "-n", str(count), host]
            else:
                ping_cmd = ["ping", "-c", str(count), host]
            
            # Execute ping
            result = subprocess.run(
                ping_cmd, 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            
            # Parse ping output
            if platform.system().lower() == "windows":
                # Windows parsing logic
                latencies = [float(line.split('time=')[1].split('ms')[0]) 
                             for line in result.stdout.split('\n') 
                             if 'time=' in line]
            else:
                # Unix/Linux parsing logic
                latencies = [float(line.split('time=')[1].split(' ')[0]) 
                             for line in result.stdout.split('\n') 
                             if 'time=' in line]
            
            return {
                'avg_latency': sum(latencies) / len(latencies) if latencies else None,
                'min_latency': min(latencies) if latencies else None,
                'max_latency': max(latencies) if latencies else None,
                'packet_loss': (count - len(latencies)) / count * 100
            }
        except Exception as e:
            print(f"Ping error for {host}: {e}")
            return None

    def collect_network_metrics(self):
        """
        Continuously collect network latency metrics
        """
        while not self.stop_event.is_set():
            current_metrics = {}
            for host in self.target_hosts:
                latency = self.ping_host(host)
                if latency:
                    current_metrics[host] = latency
            
            # Thread-safe update of metrics
            with self.metrics_lock:
                self.latency_metrics = current_metrics
            
            # Wait before next collection
            time.sleep(30)

    def get_average_latency(self):
        """
        Get the average latency across all hosts
        
        Returns:
            float: Average network latency
        """
        with self.metrics_lock:
            all_latencies = []
            for host, metrics in self.latency_metrics.items():
                if metrics and metrics['avg_latency'] is not None:
                    all_latencies.append(metrics['avg_latency'])
            
            # print("####### All ")
            return sum(all_latencies) / len(all_latencies) if all_latencies else 20

    def get_max_latency(self):
        """
        Get the average latency across all hosts
        
        Returns:
            float: Average network latency
        """
        with self.metrics_lock:
            all_latencies = []
            for host, metrics in self.latency_metrics.items():
                if metrics and metrics['max_latency'] is not None:
                    all_latencies.append(metrics['max_latency'])
                    print("max latency", metrics['max_latency'])
                
            return sum(all_latencies) / len(all_latencies) if all_latencies else 20

    def start_collection(self):
        """
        Start background thread for metric collection
        """
        collection_thread = threading.Thread(
            target=self.collect_network_metrics, 
            daemon=True
        )
        collection_thread.start()
        return collection_thread

    def stop_collection(self):
        """
        Stop the metric collection process
        """
        self.stop_event.set()

class KafkaLatencyAwareJoinOptimizer:
    def __init__(self, spark_session, metrics_collector):
        self.spark = spark_session
        self.metrics_collector = metrics_collector
        # self.current_latency_matrix = {}
        # self.latency_lock = threading.Lock()
        
        # # Start Kafka consumer in background
        # self.start_latency_listener()

    # def start_latency_listener(self):
    #     """Start a background thread to consume Kafka latency metrics"""
    #     def consume_latency():
    #         consumer = KafkaConsumer(
    #             'network_latency_metrics',
    #             bootstrap_servers=['kafka:29092'],
    #             group_id='join-optimizer-group',
    #             auto_offset_reset='latest',
    #             enable_auto_commit=True,
    #             value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    #         )

    #         for message in consumer:
    #             try:
    #                 # Thread-safe update of latency matrix
    #                 with self.latency_lock:
    #                     self.current_latency_matrix = message.value.get('latency_metrics', {})
    #                     print("Updated latency metrics received")
    #             except Exception as e:
    #                 print(f"Error processing Kafka message: {e}")

    #     # Start Kafka consumer in a daemon thread
    #     threading.Thread(target=consume_latency, daemon=True).start()

    def choose_join_strategy(self, left_size, right_size, BROADCAST_THRESHOLD = 10*1024*1024, HIGH_LATENCY_THRESHOLD = 250):
        """
        Dynamically choose join strategy based on:
        1. Table sizes
        2. Current network latency
        """
        # Get average latency
        avg_latency = self.metrics_collector.get_max_latency()

        print("\n\n\nAverage latency: ", avg_latency)
        
        smaller_size = min(left_size, right_size)
        print("smaller size", smaller_size)

        if smaller_size <= BROADCAST_THRESHOLD:
            return 'broadcast'
        elif avg_latency >= HIGH_LATENCY_THRESHOLD:
            return 'merge' #sort merge join
        else:
            return 'shuffle_hash'
        

    def distributed_join(self, left_df, right_df, join_key, smart=False):
        """
        Perform a distributed join with latency-aware strategy
        """


        # Estimate table sizes
        # left_size_details = estimate_dataframe_size(left_df)
        left_size_details = {"estimated_total_size_bytes": 719000000}

        # right_size_details = estimate_dataframe_size(right_df)
        right_size_details = {"estimated_total_size_bytes": 719000000}
        # Choose join strategy
        strategy = self.choose_join_strategy(left_size_details["estimated_total_size_bytes"], right_size_details["estimated_total_size_bytes"])
        print("\n\n\n######################")
        print("Join strategy to be used ", strategy, "\n\n\n")

        if not smart:
            print("\n\n\n######################")
            print("No strategy. Dumb Join.\n\n\n")
            return left_df.join(right_df, left_df[join_key] == right_df[join_key])
        return left_df.hint(strategy).join(right_df, left_df[join_key] == right_df[join_key])

def main():
    # Create Spark Session with Kafka dependencies
    

    spark = SparkSession.builder \
        .appName("Kafka-Enabled Latency-Aware Distributed Join") \
        .master("spark://main:7077") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    
    # Initialize Network Metrics Collector
    metrics_collector = NetworkLatencyMetricsCollector()
    metrics_collector.start_collection()

    start_time = time.time()

    # Create sample distributed dataframes
    left_df = spark.read.csv('/opt/spark/data/tables/lineitem.csv', header=False, sep="|")
    l_headers = ['orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    left_df = left_df.toDF(*l_headers)

    right_df = spark.read.csv('/opt/spark/data/tables/orders.csv', header=False, sep="|")
    r_headers = ['orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']
    right_df = right_df.toDF(*r_headers)

    # Initialize Latency-Aware Join Optimizer
    join_optimizer = KafkaLatencyAwareJoinOptimizer(spark, metrics_collector)

    # Perform distributed join
    result_df = join_optimizer.distributed_join(left_df, right_df, "orderkey", smart=False)

    end_time = time.time()
    # Show results
    result_df.show()
    print("\nTotal time taken for join: ", end_time-start_time," seconds")
    print("\nStopping metrics collection...")
    metrics_collector.stop_collection()
    spark.stop()

    # try:
    #     while True:
    #         time.sleep(30)
    #         # Periodically print current latency metrics
    #         print("\nCurrent Latency Metrics:")
    #         for host, metrics in metrics_collector.latency_metrics.items():
    #             print(f"{host}: {metrics}")
    # except KeyboardInterrupt:
    #     print("\nStopping metrics collection...")
    #     metrics_collector.stop_collection()
    #     spark.stop()

    # # Keep the application running to observe Kafka metrics
    # time.sleep(120)

    # spark.stop()

if __name__ == "__main__":
    main()