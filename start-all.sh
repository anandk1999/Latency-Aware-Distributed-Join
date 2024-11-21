#!/bin/bash
docker build -t spark-cluster-common -f spark-cluster-common.Dockerfile . && \
    docker build -t spark-cluster-main -f spark-cluster-main.Dockerfile . && \
    docker build -t spark-cluster-worker -f spark-cluster-worker.Dockerfile . && \
    docker-compose -f spark-cluster-compose.yaml up
