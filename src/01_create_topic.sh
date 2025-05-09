#!/bin/bash

# Wait for Kafka to be ready
sleep 10

# Create the topic
docker exec kafka kafka-topics.sh \
    --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic iot_data

# List topics to verify
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092 