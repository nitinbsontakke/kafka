#!/bin/bash

set -e

LOG_FILE="/tmp/mm2.log"

capture_logs() {
  echo "📡 Capturing MM2 logs..."
  docker logs mm2 --since 10s > "$LOG_FILE" 2>&1
}

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
    --count $COUNT
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
echo "🔥 Scenario 2: Log Truncation (Verify Custom Logs)"
echo "==============================="

echo "⏳ Setting aggressive retention (10 sec)..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server primary-kafka:9092 \
  --entity-type topics \
  --entity-name commit-log \
  --alter --add-config retention.ms=10000"

echo "📤 Producing 500 messages..."
run_producer 500

sleep 5

echo "📤 Producing more messages..."
run_producer 500

echo "⏳ Waiting for truncation..."
sleep 20

# 🔍 Capture logs
capture_logs

echo "📄 Checking for YOUR truncation log..."

if grep -q "LOG TRUNCATION DETECTED" "$LOG_FILE"; then
  echo "✅ PASS: Your truncation log is printed"
  grep "LOG TRUNCATION DETECTED" "$LOG_FILE"
else
  echo "❌ FAIL: Your truncation log NOT found"
  echo "---- MM2 LOGS ----"
  tail -n 100 "$LOG_FILE"
  exit 1
fi


echo ""
echo "==============================="
echo "🔥 Scenario 3: Topic Reset (Verify Custom Logs)"
echo "==============================="

echo "📤 Producing initial messages..."
run_producer 500

sleep 5

echo "🗑️ Deleting topic..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --delete --topic commit-log"

sleep 10

echo "♻️ Recreating topic..."
docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server primary-kafka:9092 \
  --create \
  --topic commit-log \
  --partitions 1 \
  --replication-factor 1"

sleep 10

echo "📤 Producing new messages..."
run_producer 500

sleep 10

# 🔍 Capture logs
capture_logs

echo "📄 Checking for YOUR reset detection log..."

if grep -q "TOPIC RESET DETECTED" "$LOG_FILE"; then
  echo "✅ PASS: Reset detection log printed"
  grep "TOPIC RESET DETECTED" "$LOG_FILE"
else
  echo "❌ FAIL: Reset detection log NOT found"
  tail -n 100 "$LOG_FILE"
  exit 1
fi

echo "📄 Checking for YOUR recovery log..."

if grep -q "Recovery successful" "$LOG_FILE"; then
  echo "✅ PASS: Recovery log printed"
  grep "Recovery successful" "$LOG_FILE"
else
  echo "❌ FAIL: Recovery log NOT found"
  tail -n 100 "$LOG_FILE"
  exit 1
fi