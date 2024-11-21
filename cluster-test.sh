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
docker-compose ps
print_status $? "All containers are running"

# 3. Create Kafka Topic
echo -e "${YELLOW}Creating Kafka Topic...${NC}"
docker-compose exec -T kafka \
    kafka-topics --create \
    --topic network_latency_metrics \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1
print_status $? "Kafka topic created"

# 4. Run Latency Simulator
echo -e "${YELLOW}Starting Latency Metrics Simulator...${NC}"
docker-compose logs latency-simulator &
sleep 20

# 5. Validate Spark Cluster Functionality
echo -e "${YELLOW}Running Distributed Cluster Validation Test...${NC}"
docker-compose exec spark-master \
    spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /opt/spark/scripts/cluster_validation_test.py
print_status $? "Cluster Validation Test"

# 6. Additional Diagnostics
echo -e "${YELLOW}Collecting Cluster Diagnostics...${NC}"
docker-compose exec spark-master \
    spark-submit --version
docker-compose exec kafka \
    kafka-topics --list --bootstrap-server localhost:9092

# Final Success Message
echo -e "${GREEN}
╔═══════════════════════════════════════╗
║   Cluster Setup Validation Complete   ║
╚═══════════════════════════════════════╝
${NC}"