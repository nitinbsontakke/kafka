# Kafka MirrorMaker 2 Enhancement Project

## 📋 Project Summary

This project implements critical enhancements to **Apache Kafka MirrorMaker 2 (MM2)** to handle edge cases in data replication, particularly focusing on **fail-fast mechanisms** for log truncation and topic reset scenarios. The improvements ensure data consistency and prevent silent failures during Kafka topic replication between primary and standby clusters.

### 🎯 Objectives

- **Fail-Fast on Log Truncation**: Detect and immediately fail when consumer offsets become unreachable due to log deletion
- **Topic Reset Detection**: Identify when topics are deleted and recreated, triggering automatic recovery mechanisms
- **Enhanced Monitoring**: Provide detailed logging and metrics for troubleshooting replication issues
- **Data Consistency**: Prevent silent data loss by aggressively detecting replication failures

---

## 📁 Project Structure

```
KafkaMirrorMakerImprovement/
│
├── Kafka/                              # Apache Kafka source fork
│   └── kafka/
│       ├── connect/mirror/             # MirrorMaker 2 source code
│       │   └── src/main/java/...
│       ├── core/                       # Kafka core modules
│       ├── clients/                    # Kafka clients
│       └── build.gradle                # Kafka build configuration
│
├── Producer/                           # Test producer application
│   ├── ProducerApp.java                # Java producer for generating test messages
│   ├── build.gradle                    # Producer build configuration
│   ├── Dockerfile/                     # Docker image for producer
│   └── build/                          # Producer build artifacts
│
├── config/                             # Configuration files
│   └── mm2.properties                  # MirrorMaker 2 configuration
│
├── docker/                             # Docker setup
│   └── kafka/
│       └── Dockerfile/
│           └── dockerfile              # Dockerfile for custom MM2 image
│
├── docker-compose.yml                  # Docker Compose orchestration
├── run_challenge.sh                    # Test scenario runner script
└── README.md                           # This file
```

---

## 🔧 Implemented Changes

### 1. **MirrorSourceTask.java Enhancements**

**File**: `Kafka/kafka/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java`

#### Key Improvements:

##### a) **Log Truncation Detection (Fail-Fast)**
```java
catch (OffsetOutOfRangeException e) {
    // Detects when consumer offset is beyond the end of the log
    // This occurs when brokers delete old log segments
    log.error("LOG TRUNCATION DETECTED: topic={}, partition={}, "
        + "earliestAvailableOffset={}. Data loss has occurred.",
        topicPartition.topic(), topicPartition.partition(), earliestOffset);
    throw new KafkaException("Fail-fast due to log truncation", e);
}
```

**What it does**:
- Catches `OffsetOutOfRangeException` when consumer seeks to an offset that has been deleted
- Logs the earliest available offset for forensic analysis
- Fails immediately to prevent inconsistent state replication
- Forces the operator to investigate and handle the data loss

##### b) **Topic Reset Detection & Auto-Recovery**
```java
catch (UnknownTopicOrPartitionException e) {
    // Detects when topic is deleted and recreated
    log.warn("TOPIC RESET DETECTED. Topics affected: {}. "
        + "Re-subscribing and seeking to beginning.", topics);
    
    consumer.unsubscribe();
    consumer.subscribe(topics);
    consumer.seekToBeginning(newAssignments);
    log.info("Recovery successful. MirrorMaker resumed replication.");
}
```

**What it does**:
- Catches `UnknownTopicOrPartitionException` when topic no longer exists
- Automatically re-subscribes to the topic partition
- Seeks to the beginning of the new topic
- Resumes replication without manual intervention
- Logs recovery actions for auditing

##### c) **Improved Exception Handling**
- Added specific exception handling for different failure scenarios
- Enhanced logging with detailed context (topic, partition, offsets)
- Preserved stack traces for debugging

##### d) **Code Quality Improvements**
- Added missing imports for new exception types
- Improved code formatting and readability
- Added null-safe operations
- Better separation of concerns in polling logic

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Java 17+
- Gradle

### Build & Run

#### 1. **Build the Kafka Custom Image**

First, build the Kafka artifacts with the MM2 enhancements:

```bash
cd Kafka/kafka
./gradlew build -x test
cd ../../
```

Build the custom MM2 Docker image:

```bash
docker build -f docker/kafka/Dockerfile/dockerfile -t kafka-mirror-maker:latest .
```

#### 2. **Build the Producer Docker Image**

```bash
cd Producer
./gradlew build
cd ..
docker build -f Producer/Dockerfile/dockerfile -t producer:latest ./Producer
```

#### 3. **Start the Docker Environment**

```bash
docker-compose up -d
```

This will start:
- **Primary Kafka Broker** (KRaft mode, port 9092)
- **Standby Kafka Broker** (KRaft mode, port 9094)
- **MirrorMaker 2** (replicating from primary → standby)
- **Producer** (generating test messages)

#### 4. **Run Test Scenarios**

```bash
./run_challenge.sh
```

This executes three test scenarios:
1. **Normal Replication**: 1000 messages replicated normally
2. **Log Truncation**: Simulates log deletion to test fail-fast behavior
3. **Topic Reset**: Deletes and recreates topic to test auto-recovery

### 5. **Monitor Logs**

```bash
# Watch MM2 logs for replication events
docker logs mm2 -f

# Watch producer logs
docker logs producer -f

# Check consumer lag from primary to standby
docker exec primary-kafka kafka-consumer-groups \
  --bootstrap-server primary-kafka:9092 \
  --group mirrormaker-cluster \
  --describe
```

---

## 📊 Test Scenarios

### Scenario 1: Normal Replication
- **Goal**: Verify basic replication works
- **Input**: 1000 JSON messages to `commit-log` topic
- **Expected**: All messages replicated from primary → standby within 10 seconds
- **Validation**: Check consumer lag = 0

### Scenario 2: Log Truncation (Fail-Fast)
- **Goal**: Test detection of log deletion
- **Action**: Set retention to 5 seconds, produce 50 more messages
- **Expected**: MM2 detects `OffsetOutOfRangeException` and logs fail-fast notice
- **Validation**: Check MM2 logs for "LOG TRUNCATION DETECTED" message

### Scenario 3: Topic Reset
- **Goal**: Test automatic recovery after topic recreation
- **Action**: Delete and recreate `commit-log` topic, produce 20 messages
- **Expected**: MM2 detects `UnknownTopicOrPartitionException` and auto-recovers
- **Validation**: New messages replicated successfully after recovery

---

## 🔍 Configuration

### MirrorMaker 2 Configuration

**File**: `config/mm2.properties`

```properties
clusters = primary, standby

# Broker endpoints
primary.bootstrap.servers = primary-kafka:9092
standby.bootstrap.servers = standby-kafka:9094

# Enable Primary → Standby replication
primary->standby.enabled = true

# Replicate all topics and consumer groups
primary->standby.topics = .*
primary->standby.groups = .*

# Replication settings
replication.factor = 1
emit.checkpoints.enabled = true
sync.group.offsets.enabled = true
```

---

## 🐳 Docker Compose Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| primary-kafka | apache/kafka:4.0.0 | 9092 | Primary Kafka broker (KRaft) |
| standby-kafka | apache/kafka:4.0.0 | 9094 | Standby Kafka broker (KRaft) |
| mm2 | kafka-mirror-maker:latest | - | MirrorMaker 2 (enhanced) |
| producer | producer:latest | - | Test message producer |

---

## 📝 Cleanup

Clean up Docker resources:

```bash
# Stop and remove all containers
docker-compose down

# Remove all dangling containers
docker container prune -f

# Remove images
docker rmi kafka-mirror-maker:latest producer:latest

# Clear KRaft logs (if using local volumes)
rm -rf /tmp/kraft-combined-logs
```

---

## 🎓 Key Learnings

### Fail-Fast Strategy
- Detecting issues early prevents silent data loss
- Forcing failures allows operators to react and investigate
- Better to fail loudly than lose data silently

### Exception Handling Hierarchy
- `OffsetOutOfRangeException` → Log truncation (permanent data loss)
- `UnknownTopicOrPartitionException` → Topic reset (recoverable)
- Different scenarios require different recovery strategies

### Monitoring & Alerting
- Critical to log detailed context: topic, partition, offset information
- Enables faster debugging and forensic analysis
- Integration with observability platforms recommended

---

## 🔗 References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [MirrorMaker 2 User Guide](https://kafka.apache.org/documentation/#mirroring)
- [Kafka Source Code](https://github.com/apache/kafka)

---

## 📄 License

This project is based on Apache Kafka, which is licensed under the Apache License, Version 2.0.
See [LICENSE](Kafka/kafka/LICENSE) for details.

---

## 👥 Contributing

For improvements or bug reports, please follow the Apache Kafka [CONTRIBUTING](Kafka/kafka/CONTRIBUTING.md) guidelines.
