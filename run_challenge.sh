#!/bin/bash

set -e

docker-compose down

docker-compose up 

echo "=== Starting Kafka environment ==="

echo "Waiting for Kafka brokers to start..."
sleep 25

# ----------------------------
# Ensure topic exists
# ----------------------------
echo "Creating topic commit-log (if not exists)..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --create --if-not-exists \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1"

# ----------------------------
# Scenario 1
# ----------------------------
echo "=== Scenario 1: Normal Replication ==="
echo "Producing 1000 messages..."

docker exec producer sh -c "java -jar /app/app.jar --count 1000"

echo "Waiting for replication..."
sleep 10

echo "MM2 logs:"
docker logs mm2 --tail 50

# ----------------------------
# Scenario 2
# ----------------------------
echo "=== Scenario 2: Log Truncation ==="

echo "Reducing retention to 5 seconds..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server primary-kafka:9092 \
  --entity-type topics \
  --entity-name commit-log \
  --alter --add-config retention.ms=5000"

echo "Producing 50 messages..."
docker exec producer sh -c "java -jar /app/app.jar --count 50"

echo "Waiting for truncation..."
sleep 10

echo "MM2 logs (expect fail-fast):"
docker logs mm2 --tail 50

# ----------------------------
# Scenario 3
# ----------------------------
echo "=== Scenario 3: Topic Reset ==="

echo "Deleting topic..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --delete --topic commit-log"

sleep 5

echo "Recreating topic..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --create \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1"

echo "Producing 20 messages..."
docker exec producer sh -c "java -jar /app/app.jar --count 20"

sleep 10

echo "MM2 logs (expect recovery):"
docker logs mm2 --tail 50

echo "=== All scenarios complete ==="