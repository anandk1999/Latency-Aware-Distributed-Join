import numpy as np
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

    # def choose_join_strategy(self, left_size, right_size, BROADCAST_THRESHOLD = 10*1024*1024, HIGH_LATENCY_THRESHOLD = 250):
    #     """
    #     Dynamically choose join strategy based on:
    #     1. Table sizes
    #     2. Current network latency
    #     """
    #     # Get average latency
    #     avg_latency = self.metrics_collector.get_max_latency()

    #     print("\n\n\nAverage latency: ", avg_latency)
        
    #     smaller_size = min(left_size, right_size)
    #     print("smaller size", smaller_size)

    #     if smaller_size <= BROADCAST_THRESHOLD:
    #         return 'broadcast'
    #     elif avg_latency >= HIGH_LATENCY_THRESHOLD:
    #         return 'merge' #sort merge join
    #     else:
    #         return 'shuffle_hash'

    def calculate_cost(self, network_latency, table_sizes, join_type):
        """
        Calculate total cost based on realistic computation and resource factors.
        """
        left_size, right_size = table_sizes
        
        # Network Cost
        if join_type == 'broadcast':
            data_transfer_volume = min(left_size, right_size)
        elif join_type == 'shuffle':
            data_transfer_volume = left_size + right_size
        elif join_type == 'sort_merge':
            data_transfer_volume = left_size + right_size  # Sorting minimizes network shuffles
        else:  # Nested-loop
            data_transfer_volume = left_size if right_size < left_size else right_size
        
        network_cost = network_latency * data_transfer_volume
        
        # Computation Cost (realistic factors)
        if join_type == 'broadcast':
            computation_cost = left_size * right_size * 0.05  # Factor based on benchmarks
        elif join_type == 'shuffle':
            computation_cost = left_size * right_size * 0.3
        elif join_type == 'sort_merge':
            computation_cost = (left_size * np.log2(left_size) + right_size * np.log2(right_size)) * 0.2
        else:  # Nested-loop
            computation_cost = left_size * right_size * 0.8
        
        # Resource Cost (realistic factors)
        if join_type == 'broadcast':
            resource_cost = 0.4 * min(left_size, right_size)  # Memory-intensive
        elif join_type == 'shuffle':
            resource_cost = 0.6 * (left_size + right_size)   # High shuffle overhead
        elif join_type == 'sort_merge':
            resource_cost = 0.5 * (left_size + right_size)   # Sorting requires moderate resources
        else:  # Nested-loop
            resource_cost = 0.7 * max(left_size, right_size) # High memory/CPU usage
        
        return network_cost + computation_cost + resource_cost

    def choose_join_strategy(self, left_size, right_size):
        """
        Dynamically choose the best join strategy based on the cost model.
        """
        avg_latency = self.metrics_collector.get_average_latency()
        
        # Calculate costs for each strategy
        costs = {
            'broadcast': self.calculate_cost(avg_latency, (left_size, right_size), 'broadcast'),
            'shuffle': self.calculate_cost(avg_latency, (left_size, right_size), 'shuffle'),
            'sort_merge': self.calculate_cost(avg_latency, (left_size, right_size), 'sort_merge'),
            'nested_loop': self.calculate_cost(avg_latency, (left_size, right_size), 'nested_loop')
        }
        
        # Select the strategy with the lowest cost
        return min(costs, key=costs.get)

    def distributed_join(self, left_df, right_df, join_key, smart=False):
        """
        Perform a distributed join with latency-aware strategy.
        """
        left_size = left_df.count()
        right_size = right_df.count()
        
        strategy = self.choose_join_strategy(left_size, right_size)

        if not smart:
            print("\n\n\n######################")
            print("No strategy. Dumb Join.\n\n\n")
            # self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
            left_df.createOrReplaceTempView("l_df")
            right_df.createOrReplaceTempView("r_df")

            # result = self.spark.sql(f"""
            #     SELECT /*+ SHUFFLE_HASH(l_df) */ *
            #     FROM l_df
            #     JOIN r_df ON l_df.{join_key} = r_df.{join_key}
            # """)

            result = left_df.join(right_df, left_df[join_key] == right_df[join_key])
            print(result.explain())
        
        elif strategy == 'broadcast':
            print(f"Using Broadcast Join (Left: {left_size}, Right: {right_size})")
            if left_size < right_size:
                result = broadcast(left_df).join(right_df, left_df[join_key] == right_df[join_key])

            else:
                result = left_df.join(broadcast(right_df), left_df[join_key] == right_df[join_key])

        
        elif strategy == 'shuffle':
            print(f"Using Shuffle Join (Left: {left_size}, Right: {right_size})")
            left_df.createOrReplaceTempView("l_df")
            right_df.createOrReplaceTempView("r_df")

            result = self.spark.sql(f"""
                SELECT /*+ SHUFFLE_HASH(l_df) */ *
                FROM l_df
                JOIN r_df ON l_df.{join_key} = r_df.{join_key}
            """)
        
        elif strategy == 'sort_merge':
            print(f"Using Sort-Merge Join (Left: {left_size}, Right: {right_size})")
            sorted_left = left_df.sort(join_key)
            sorted_right = right_df.sort(join_key)
            result =  sorted_left.join(sorted_right, sorted_left[join_key] == sorted_right[join_key])
        
        else:  # Nested-loop
            print(f"Using Nested-Loop Join (Left: {left_size}, Right: {right_size})")
            result=  left_df.crossJoin(right_df).filter(left_df[join_key] == right_df[join_key])
        
        print("####### Explain!!!!")
        print(result.explain())
        return result

    # def distributed_join(self, left_df, right_df, join_key, smart=False):
    #     """
    #     Perform a distributed join with latency-aware strategy
    #     """


    #     # Estimate table sizes
    #     # left_size_details = estimate_dataframe_size(left_df)
    #     left_size_details = {"estimated_total_size_bytes": 719000000}

    #     # right_size_details = estimate_dataframe_size(right_df)
    #     right_size_details = {"estimated_total_size_bytes": 719000000}
    #     # Choose join strategy
    #     strategy = self.choose_join_strategy(left_size_details["estimated_total_size_bytes"], right_size_details["estimated_total_size_bytes"])
    #     print("\n\n\n######################")
    #     print("Join strategy to be used ", strategy, "\n\n\n")

    #     if not smart:
    #         print("\n\n\n######################")
    #         print("No strategy. Dumb Join.\n\n\n")
    #         return left_df.join(right_df, left_df[join_key] == right_df[join_key])
    #     return left_df.hint(strategy).join(right_df, left_df[join_key] == right_df[join_key])

def main():
    # Create Spark Session with Kafka dependencies
    

    spark = SparkSession.builder \
        .appName("Kafka-Enabled Latency-Aware Distributed Join") \
        .master("spark://main:7077") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.locality.wait", "0s") \
        .config("spark.scheduler.disable.localFallback", "true") \
        .config("spark.scheduler.minRegisteredResourcesRatio", "1.0") \
        .config("spark.network.timeout", "300s") \
        .config("spark.executor.heartbeatInterval","60s") \
        .config("spark.rpc.askTimeout","300s") \
        .config("spark.task.maxFailures","10") \
        .config("spark.locality.wait","5s") \
        .getOrCreate()
    

    # Initialize Network Metrics Collector
    metrics_collector = NetworkLatencyMetricsCollector()
    metrics_collector.start_collection()

    start_time = time.time()

    # Create sample distributed dataframes
    # left_df = spark.read.csv('/opt/spark/data/tables/lineitem.csv', header=False, sep="|")
    # l_headers = ['orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
    #                     'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
    #                     'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    # left_df = left_df.toDF(*l_headers)
    # left_df = left_df.repartition(32)
    left_df = spark.read.csv('/opt/spark/data/tables/customer.csv', header=False, sep="|")
    l_headers = ['custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                        'c_comment']
    left_df = left_df.toDF(*l_headers)
    left_df = left_df.repartition(32)

    right_df = spark.read.csv('/opt/spark/data/tables/orders.csv', header=False, sep="|")
    r_headers = ['orderkey', 'custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']
    right_df = right_df.toDF(*r_headers)
    right_df = right_df.repartition(32)

    # Initialize Latency-Aware Join Optimizer
    join_optimizer = KafkaLatencyAwareJoinOptimizer(spark, metrics_collector)

    # Perform distributed join
    result_df = join_optimizer.distributed_join(left_df, right_df, "custkey", smart=True)

    end_time = time.time()
    # Show results
    result_df.show()
    print("\nTotal time taken for join: ", end_time-start_time," seconds")
    # metrics_collector.stop_collection()
    # spark.stop()

    try:
        while True:
            time.sleep(30)
            # Periodically print current latency metrics
            print("\nCurrent Latency Metrics:")
            for host, metrics in metrics_collector.latency_metrics.items():
                print(f"{host}: {metrics}")
    except KeyboardInterrupt:
        print("\nStopping metrics collection...")
        metrics_collector.stop_collection()
        spark.stop()

    # # Keep the application running to observe Kafka metrics
    # time.sleep(120)

    # spark.stop()

if __name__ == "__main__":
    main()