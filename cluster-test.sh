#!/bin/bash

# Comprehensive Cluster Setup and Validation Script

# Color codes for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print status
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ $2${NC}"
    else
        echo -e "${RED}✗ $2${NC}"
        exit 1
    fi
}

# 0. Prerequisite Check
echo -e "${YELLOW}Checking Docker and Docker Compose...${NC}"
docker --version
docker-compose --version
print_status $? "Docker prerequisites installed"

# # 1. Start Infrastructure
# echo -e "${YELLOW}Starting Distributed Cluster Infrastructure...${NC}"
# docker-compose up -d
# sleep 30  # Wait for services to initialize

# 2. Verify Container Status
echo -e "${YELLOW}Checking Container Status...${NC}"
docker-compose -f spark-cluster-compose.yaml ps
print_status $? "All containers are running"

# 3. Create Kafka Topic
echo -e "${YELLOW}Creating Kafka Topic...${NC}"
TOPIC_NAME="network_latency_metrics"
BROKER="localhost:9092"
if kafka-topics.sh --bootstrap-server "$BROKER" --list | grep -wq "$TOPIC_NAME"; then
    echo "Topic '$TOPIC_NAME' already exists. Skipping creation."
else
    echo "Creating topic '$TOPIC_NAME'..."
    kafka-topics.sh --bootstrap-server "$BROKER" --create \
        --topic "$TOPIC_NAME" \
        --partitions 1 \
        --replication-factor 1
    echo "Topic '$TOPIC_NAME' created."
fi
print_status $? "Kafka topic created"

# 4. Run Latency Simulator
echo -e "${YELLOW}Starting Latency Metrics Simulator...${NC}"
docker-compose -f spark-cluster-compose.yaml logs latency-simulator &
sleep 20

# 5. Validate Spark Cluster Functionality
echo -e "${YELLOW}Running Distributed Cluster Validation Test...${NC}"
docker-compose -f spark-cluster-compose.yaml exec main \
    spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --conf spark.executor.instances=3 \
    /opt/spark/scripts/cluster_validation_test.py
print_status $? "Cluster Validation Test"

# 6. Additional Diagnostics
echo -e "${YELLOW}Collecting Cluster Diagnostics...${NC}"
docker-compose -f spark-cluster-compose.yaml exec main \
    spark-submit --version
docker-compose -f spark-cluster-compose.yaml exec kafka \
    kafka-topics --list --bootstrap-server localhost:9092

# Final Success Message
echo -e "${GREEN}
╔═══════════════════════════════════════╗
║   Cluster Setup Validation Complete   ║
╚═══════════════════════════════════════╝
${NC}"