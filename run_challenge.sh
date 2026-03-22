#!/bin/bash

set -e

echo "=== 🚀 MM2 Test Script (On-demand Producer) ==="

# ----------------------------
# Detect docker network
# ----------------------------
NETWORK=$(docker inspect primary-kafka \
  --format='{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')

echo "🌐 Using Docker network: $NETWORK"

# ----------------------------
# Wait for Kafka brokers
# ----------------------------
echo "⏳ Waiting for primary Kafka..."

until docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server primary-kafka:9092 --list" >/dev/null 2>&1; do
  echo "Primary Kafka not ready..."
  sleep 5
done

echo "⏳ Waiting for standby Kafka..."

until docker exec standby-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server standby-kafka:9092 --list" >/dev/null 2>&1; do
  echo "Standby Kafka not ready..."
  sleep 5
done

echo "✅ Kafka clusters are ready"

# ----------------------------
# Wait for MM2
# ----------------------------
echo "⏳ Waiting for MirrorMaker2..."

until docker logs mm2 2>&1 | grep -q "Herder started"; do
  echo "MM2 not ready..."
  sleep 5
done

echo "✅ MirrorMaker2 is running"

# ----------------------------
# Create topic
# ----------------------------
echo "📌 Creating topic: commit-log"

docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --create --if-not-exists \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1"

# ----------------------------
# Helper: Run producer
# ----------------------------
run_producer() {
  COUNT=$1
  echo "📤 Producing $COUNT messages..."

  docker run --rm --network $NETWORK \
    commit-log-producer \
    java -jar /app/app.jar --count $COUNT
}

# ----------------------------
# Scenario 1: Normal Replication
# ----------------------------
echo ""
echo "==============================="
echo "✅ Scenario 1: Normal Replication"
echo "==============================="

run_producer 20

sleep 5

echo "⏳ Waiting for topic replication in standby..."

until docker exec standby-kafka sh -c \
  "/opt/kafka/bin/kafka-topics.sh --bootstrap-server standby-kafka:9092 --list | grep primary.commit-log" >/dev/null 2>&1
  do
    echo "Topic not replicated yet..."
    sleep 3
done

echo "✅ Topic replicated"

echo "📥 Consuming from standby..."

docker exec standby-kafka sh -c "/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server standby-kafka:9092 \
  --topic primary.commit-log \
  --from-beginning \
  --timeout-ms 10000 \
  --max-messages 20"

echo "Replication done"

docker exec standby-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server standby-kafka:9092 --list"

echo "✅ Scenario 1 completed"

echo "Checking.. if topics are present in primary kafka"

docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server primary-kafka:9092 --list"

echo "Breakpoint: press Enter to continue"
read -r


echo ""
echo "==============================="
echo "🔥 Scenario 2: Log Truncation (No MM2 Stop)"
echo "==============================="

# Step 1: Set very low retention
echo "⏳ Setting aggressive retention (10 sec)..."

docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server primary-kafka:9092 \
  --entity-type topics \
  --entity-name commit-log \
  --alter --add-config retention.ms=10000"

# Step 2: Produce large data to create lag
echo "📤 Producing 100 messages..."

run_producer 100

# Step 3: Let MM2 process partially
echo "⏳ Letting MM2 consume partially..."
sleep 5

# Step 4: Produce more to push offsets forward
echo "📤 Producing more messages to create offset gap..."

run_producer 100

# Step 5: Wait for retention cleanup
echo "⏳ Waiting for log cleanup..."
sleep 20

# Step 6: Observe logs
echo "📄 Checking MM2 logs (expect truncation)..."

docker logs mm2 --tail 100 | grep -E "TRUNCATION|OffsetOutOfRange"

echo "✅ Scenario 2 completed"

echo ""
echo "==============================="
echo "🔥 Scenario 3: Topic Reset (No MM2 Stop)"
echo "==============================="

# Step 1: Produce initial data
echo "📤 Producing initial messages..."
run_producer 50

sleep 5

# Step 2: Delete topic while MM2 is running
echo "🗑️ Deleting topic while MM2 is active..."

docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --delete --topic commit-log"

# Step 3: Wait for deletion detection
echo "⏳ Waiting for MM2 to detect deletion..."
sleep 10

# Step 4: Recreate topic
echo "♻️ Recreating topic..."

docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --create \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1"

# Step 5: Wait for reassignment
echo "⏳ Waiting for reassignment..."
sleep 10

# Step 6: Produce new data
echo "📤 Producing new messages after reset..."
run_producer 50

# Step 7: Consume from standby
echo "📥 Verifying replication after reset..."

docker exec standby-kafka sh -c "/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server standby-kafka:9092 \
  --topic primary.commit-log \
  --from-beginning \
  --max-messages 100"

# Step 8: Check logs
echo "📄 Checking MM2 logs (expect reset detection)..."

docker logs mm2 --tail 100 | grep -E "RESET|RECREATED|assignment"

echo "✅ Scenario 3 completed"