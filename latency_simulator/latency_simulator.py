from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import json
import time
import random

class KafkaLatencySimulator:
    def __init__(self, bootstrap_servers=['kafka:29092']):
        self.producer = None
        self.topic = 'network_latency_metrics'
        
        try:
            print(f"Attempting to connect to Kafka at {bootstrap_servers}...")
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka producer initialized successfully.")
        except NoBrokersAvailable as e:
            print(f"No Kafka brokers available: {e}")
        except Exception as e:
            print(f"Unexpected error connecting to Kafka: {e}")
        
        if not self.producer:
            print("Kafka Producer could not be initialized.")

    def generate_network_conditions(self):
        """
        Simulate advanced network conditions with Kafka
        """
        node_topology = {
            'nodes': [
                {'name': 'main', 'ip': '192.168.10.2'},
                {'name': 'worker-1', 'ip': '192.168.10.3'},
                {'name': 'worker-2', 'ip': '192.168.10.4'}
            ]
        }

        while True:
            # Generate latency matrix
            latency_matrix = {}
            for source in node_topology['nodes']:
                latency_matrix[source['name']] = {}
                
                for dest in node_topology['nodes']:
                    if source != dest:
                        # Base latency simulation
                        base_latency = random.uniform(5, 50)
                        
                        # Network congestion simulation
                        congestion_factor = random.uniform(1, 3)
                        latency = base_latency * congestion_factor
                        
                        # Occasional latency spike
                        if random.random() < 0.1:
                            latency += random.uniform(50, 200)
                        
                        latency_matrix[source['name']][dest['name']] = round(latency, 2)
            
            # Prepare message
            message = {
                'timestamp': time.time(),
                'latency_metrics': latency_matrix
            }
            
            # Send to Kafka
            try:
                future = self.producer.send(self.topic, message)
                record_metadata = future.get(timeout=10)
                print(f"Sent latency metrics to {record_metadata.topic} "
                      f"partition {record_metadata.partition}")
            except KafkaError as e:
                print(f"Kafka error: {e}")
            
            # Wait before next update
            time.sleep(2)

    def close(self):
        """Close Kafka producer"""
        if self.producer:
            self.producer.close()

def main():
    simulator = KafkaLatencySimulator()
    try:
        simulator.generate_network_conditions()
    except KeyboardInterrupt:
        print("Stopping latency simulator")
    finally:
        simulator.close()

if __name__ == "__main__":
    main()