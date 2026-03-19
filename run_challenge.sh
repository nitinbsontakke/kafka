#!/bin/bash

set -e

echo "=== Starting Kafka environment ==="


# Wait for brokers to be ready
echo "Waiting for Kafka brokers to start..."
sleep 20

echo "=== Scenario 1: Normal Replication ==="
echo "Producing 1000 messages to primary commit-log..."
docker exec -i producer sh -c "java -jar ProducerApp.jar --count 1000"

echo "Waiting 10s for MirrorMaker 2 to replicate..."
sleep 10

echo "Check MirrorMaker 2 logs for replication..."
docker logs enhanced-mm2 --tail 50

echo "=== Scenario 2: Log Truncation / Fail-Fast Simulation ==="
echo "Triggering truncation (simulated by reducing retention to 5s)..."
docker exec -i primary-kafka sh -c "kafka-configs --bootstrap-server primary-kafka:9092 --entity-type topics --entity-name commit-log --alter --add-config retention.ms=5000"

echo "Producing 50 more messages..."
docker exec -i producer sh -c "java -jar ProducerApp.jar --count 50"

sleep 7

echo "Check MirrorMaker 2 logs for fail-fast detection..."
docker logs enhanced-mm2 --tail 50

echo "=== Scenario 3: Topic Reset Simulation ==="
echo "Deleting and recreating commit-log topic..."
docker exec -i primary-kafka sh -c "kafka-topics --bootstrap-server primary-kafka:9092 --delete --topic commit-log"
sleep 3
docker exec -i primary-kafka sh -c "kafka-topics --bootstrap-server primary-kafka:9092 --create --topic commit-log --partitions 1 --replication-factor 1"

echo "Producing 20 messages after reset..."
docker exec -i producer sh -c "java -jar ProducerApp.jar --count 20"

sleep 5

echo "Check MirrorMaker 2 logs for automatic recovery..."
docker logs enhanced-mm2 --tail 50

echo "=== All scenarios complete ==="