#!/bin/bash

# Kafka Topics Setup
echo "Creating Kafka topics..."
kafka-topics.sh --create --topic sol.blocks --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 || echo "Topic already exists"
echo "Kafka topics created."
