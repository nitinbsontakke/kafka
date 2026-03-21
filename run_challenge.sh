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

run_producer 100

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
  --max-messages 100"

echo "✅ Scenario 1 completed"

# ----------------------------
# Scenario 2: Log Truncation
# ----------------------------


echo "⏸️ Stopping MirrorMaker2..."
docker stop mm2

echo "⏳ Setting retention to 60 sec..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server primary-kafka:9092 \
  --entity-type topics \
  --entity-name commit-log \
  --alter --add-config retention.ms=60000"

echo "📤 Producing 100 messages (MM2 is stopped)..."
docker run --rm --network $NETWORK commit-log-producer \
  java -jar /app/app.jar --count 100

echo "⏳ Waiting for retention cleanup..."
sleep 70

echo "▶️ Restarting MirrorMaker2..."
docker start mm2

echo "⏳ Waiting for MM2 recovery..."
sleep 10

echo "📄 MM2 logs (expect offset reset / truncation):"
docker logs mm2 --tail 50
# ----------------------------
# Scenario 3: Topic Reset
# ----------------------------
echo ""
echo "==============================="
echo "🔥 Scenario 3: Topic Reset"
echo "==============================="


run_producer 100

sleep 5

echo "🗑️ Deleting topic..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --delete --topic commit-log"

echo "⏳ Waiting for topic deletion to complete..."

# Wait until topic disappears
while docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server primary-kafka:9092 --list" | grep -q "commit-log"; do
  echo "Topic still exists... waiting"
  sleep 3
done

echo "✅ Topic deleted"

echo "♻️ Recreating topic..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --create \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1"

echo "⏳ Waiting..."
sleep 10

echo "📥 Consuming after reset..."

docker exec standby-kafka sh -c "/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server standby-kafka:9092 \
  --topic primary.commit-log \
  --from-beginning \
  --max-messages 100"

echo "📄 MM2 logs (recovery expected):"
docker logs mm2 --tail 50

echo ""
echo "🎉 ALL TEST SCENARIOS COMPLETED SUCCESSFULLY"