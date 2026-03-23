# Kafka MirrorMaker 2 Enhancement Project

## 📋 Project Overview

This project implements critical enhancements to **Apache Kafka MirrorMaker 2 (MM2)** to handle edge cases in data replication, particularly focusing on **fail-fast mechanisms** for log truncation and topic reset scenarios. The improvements ensure data consistency and prevent silent failures during Kafka topic replication between primary and standby clusters.

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

## 1️⃣ Repository Links

- **Kafka Fork**: [Apache Kafka - Main Fork](https://github.com/apache/kafka)
  - Location in this project: `./Kafka/kafka/`
  
- **Pull Request**: [MirrorMaker 2 Enhancement PR](https://github.com/apache/kafka/pulls)
  - Baseline: Apache Kafka main branch
  - Modifications: `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java`

---

## 2️⃣ Docker Hub Images

The following container images are used in this project:

| Image | Tag | Purpose | Source |
|-------|-----|---------|--------|
| `apache/kafka` | `4.0.0` | Kafka Primary Cluster | Apache Kafka Official |
| `apache/kafka` | `4.0.0` | Kafka Standby Cluster | Apache Kafka Official |
| `kafka-mirror-maker:latest` | `latest` | Custom MirrorMaker 2 with Enhancements | Built locally from `docker/kafka/Dockerfile/dockerfile` |
| `commit-log-producer` | `latest` | Test Producer Application | Built from `Producer/Dockerfile/dockerfile` |

**Building Custom Images**:
```bash
# Build custom MM2 image with enhancements
docker build -t kafka-mirror-maker:latest ./docker/kafka/Dockerfile/

# Build test producer image
docker build -t commit-log-producer ./Producer/Dockerfile/
```

---

## 3️⃣ Setup Instructions

### Prerequisites
- Docker and Docker Compose installed
- Approximately 2GB of available memory for containers
- Bash shell for running test scripts

### Initial Setup

1. **Clone the repository and navigate to the project**:
   ```bash
   cd KafkaMirrorMakerImprovement
   ```

2. **Build the enhanced Kafka application**:
   ```bash
   cd Kafka/kafka
   ./gradlew build -x test
   cd ../..
   ```

3. **Build the custom MM2 Docker image**:
   ```bash
   docker build -t kafka-mirror-maker:latest ./docker/kafka/Dockerfile/
   ```

4. **Build the test producer Docker image**:
   ```bash
   docker build -t commit-log-producer ./Producer/Dockerfile/
   ```

5. **Start the containerized environment**:
   ```bash
   docker-compose up -d
   ```

   This will:
   - Start primary Kafka broker (port 9092)
   - Start standby Kafka broker (port 9094)
   - Start MirrorMaker 2 connector (mm2 container)

6. **Verify cluster status**:
   ```bash
   # Check if all services are running
   docker-compose ps
   
   # Verify primary-kafka is ready
   docker exec primary-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server primary-kafka:9092 --list"
   
   # Verify standby-kafka is ready
   docker exec standby-kafka sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server standby-kafka:9092 --list"
   ```

### Configuration Details

**MirrorMaker 2 Configuration** (`config/mm2.properties`):
```properties
clusters=primary,standby
primary.bootstrap.servers=primary-kafka:9092
standby.bootstrap.servers=standby-kafka:9092

# Replication direction: primary -> standby only
primary->standby.enabled=true
standby->primary.enabled=false

# Replicate all topics matching pattern
primary->standby.topics=.*

# Replication factors for single-node clusters
replication.factor=1
checkpoints.topic.replication.factor=1
heartbeats.topic.replication.factor=1
offset-syncs.topic.replication.factor=1

# Enable offset and group sync
sync.group.offsets.enabled=true
refresh.topics.enabled=true
refresh.groups.enabled=true
emit.heartbeats.enabled=true
```

---

## 4️⃣ Test Execution

### Running the Test Scenario Suite

Execute the comprehensive test suite that validates all enhancements:

```bash
chmod +x run_challenge.sh
./run_challenge.sh
```

### Test Scenarios

The test script (`run_challenge.sh`) executes three scenarios:

#### **Scenario 1: Normal Replication ✅**
**Purpose**: Verify baseline MM2 functionality works correctly

**Steps**:
1. Produces 20 messages to topic `commit-log` on primary cluster
2. Waits for topic to be replicated to standby as `primary.commit-log`
3. Consumes messages from standby to verify replication
4. Lists all topics on both clusters

**Expected Output**:
```
✅ Scenario 1 completed
Topic replicated: primary.commit-log visible on standby cluster
Consumer reads exactly 20 messages
```

**Interpret Results**: If messages are consumed successfully, the replication pipeline is functioning.

---

#### **Scenario 2: Log Truncation Detection 🔥**
**Purpose**: Verify fail-fast behavior when brokers delete log segments

**Steps**:
1. Sets aggressive retention policy (10 seconds) on `commit-log` topic
2. Produces 500 messages, waits for log segments to be deleted
3. Produces 500 more messages to force offset mismatch
4. Waits 20 seconds for truncation to occur
5. Captures MM2 logs and checks for custom truncation detection

**Expected Output**:
```
✅ PASS: Your truncation log is printed
LOG TRUNCATION DETECTED: topic=commit-log, partition=0, earliestAvailableOffset=XXX. Data loss has occurred.
```

**Interpret Results**:
- 🟢 **PASS**: Custom log message confirms fail-fast detection is working
- 🔴 **FAIL**: Indicates MM2 continued replicating despite log deletion (data inconsistency risk)

---

#### **Scenario 3: Topic Reset Detection & Recovery ♻️**
**Purpose**: Verify automatic recovery when topics are deleted and recreated

**Steps**:
1. Produces 500 initial messages to `commit-log`
2. Deletes the topic on primary cluster
3. Waits 10 seconds
4. Recreates the topic (simulating reset scenario)
5. Produces 500 new messages
6. Captures MM2 logs and checks for topic reset detection

**Expected Output**:
```
✅ PASS: Reset detection log printed
TOPIC RESET DETECTED. Topics affected: [commit-log]. Re-subscribing and seeking to beginning.
Recovery successful. MirrorMaker resumed replication.
```

**Interpret Results**:
- 🟢 **PASS**: Custom recovery logs confirm auto-recovery mechanism works
- 🔴 **FAIL**: MM2 didn't recover automatically (manual intervention required)

### Interpreting Test Results

| Scenario | Pass Criteria | Failure Impact |
|----------|---------------|-----------------|
| 1 | All 20 messages consumed | Replication broken, non-replicated data |
| 2 | "LOG TRUNCATION DETECTED" log found | Silent data loss undetected |
| 3 | "TOPIC RESET DETECTED" log found | Data consistency compromised, manual recovery needed |

---

## 5️⃣ Log Analysis

### Key Log Messages to Monitor

#### 1. **Log Truncation Detection** (Scenario 2)
```
[ERROR] LOG TRUNCATION DETECTED: topic=commit-log, partition=0, 
        earliestAvailableOffset=12345. Data loss has occurred.
```
- **Indicates**: Consumer offset became unreachable due to log deletion
- **Action Required**: Investigate why logs were deleted; verify replication consistency
- **Location**: MM2 container logs

#### 2. **Topic Reset Detection** (Scenario 3)
```
[WARN] TOPIC RESET DETECTED. Topics affected: [commit-log]. 
       Re-subscribing and seeking to beginning.
[INFO] Recovery successful. MirrorMaker resumed replication.
```
- **Indicates**: Topic was deleted and recreated; automatic recovery triggered
- **Action**: Monitor that replication resumes without data gaps
- **Location**: MM2 container logs

#### 3. **Normal Replication Logs**
```
[INFO] Herder started
[INFO] topic: mirror-start-marker, partition: 0
[INFO] Successfully synced group offset
```
- **Indicates**: MM2 is running normally and replicating topics
- **Action**: Baseline expected behavior
- **Location**: MM2 container logs

### Capturing Container Logs

**MM2 Logs**:
```bash
# View recent MM2 logs
docker logs mm2

# Follow MM2 logs in real-time
docker logs -f mm2

# Capture last 10 seconds of logs
docker logs mm2 --since 10s > /tmp/mm2.log
```

**Primary Kafka Logs**:
```bash
docker logs primary-kafka | tail -50
```

**Standby Kafka Logs**:
```bash
docker logs standby-kafka | tail -50
```

**Grep for Specific Events**:
```bash
# Search for truncation errors
docker logs mm2 | grep "LOG TRUNCATION DETECTED"

# Search for topic reset
docker logs mm2 | grep "TOPIC RESET DETECTED"

# Search for errors
docker logs mm2 | grep ERROR
```

---

## 6️⃣ Design Rationale

### Problem Statement
Apache Kafka MirrorMaker 2 (MM2) replicates topics between Kafka clusters. However, it wasn't designed to handle edge cases where:
1. **Log truncation occurs**: Consumer offsets become invalid due to aggressive retention policies
2. **Topics are reset**: Topics are deleted and recreated with new data

In both scenarios, MM2 would silently continue replication with inconsistent data, leading to undetected data loss.

### Solution Architecture

#### 1. **Fail-Fast Mechanism for Log Truncation**

**Implementation**: Enhanced `MirrorSourceTask.java` with exception handling

```java
catch (OffsetOutOfRangeException e) {
    log.error("LOG TRUNCATION DETECTED: topic={}, partition={}, "
        + "earliestAvailableOffset={}. Data loss has occurred.",
        topicPartition.topic(), topicPartition.partition(), earliestOffset);
    throw new KafkaException("Fail-fast due to log truncation", e);
}
```

**Why This Approach**:
- Immediately stops replication when data loss is detected
- Prevents replication of inconsistent state
- Forces operator to investigate and handle the issue
- Provides forensic information (earliest available offset) for debugging

**Data Flow**:
```
Primary Cluster → [Deleted Logs] → Consumer seeks failed
                                ↓
                    OffsetOutOfRangeException caught
                                ↓
                    Custom ERROR logged
                                ↓
                    Replication STOPS
```

#### 2. **Automatic Topic Reset Recovery**

**Implementation**: Exception handling for `UnknownTopicOrPartitionException`

```java
catch (UnknownTopicOrPartitionException e) {
    log.warn("TOPIC RESET DETECTED. Topics affected: {}. "
        + "Re-subscribing and seeking to beginning.", topics);
    
    consumer.unsubscribe();
    consumer.subscribe(topics);
    consumer.seekToBeginning(newAssignments);
    log.info("Recovery successful. MirrorMaker resumed replication.");
}
```

**Why This Approach**:
- Automatically recovers when topic is deleted and recreated
- No manual intervention required
- Resumes replication from the beginning of the new topic
- Logs all recovery actions for audit trail

**Data Flow**:
```
Topic Deleted → [Recreation] → Topic exists again
                              ↓
                    UnknownTopicOrPartitionException caught
                              ↓
                    Consumer unsubscribes
                              ↓
                    Consumer re-subscribes to new partition
                              ↓
                    Consumer seeks to beginning
                              ↓
                    Replication RESUMES with new data
```

### Integration Points

**Modified Files**:
- `Kafka/kafka/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java`
  - Line: `poll()` method
  - Changes: Added try-catch blocks for exception handling
  - Impact: Every message fetch operation is now protected

**No Breaking Changes**:
- All existing MM2 functionality preserved
- Enhanced logging only adds to log output
- Exception handling is additive (wraps existing logic)
- Compatible with existing MM2 configurations

### Performance Impact
- **Minimal**: Exception handling only activates on error conditions
- **Normal Operations**: No performance degradation
- **Edge Cases**: Immediate failure detection improves resource efficiency (stops replicating bad data)

### Monitoring & Alerting Strategy
```
Set alerts for these log patterns:
- ERROR: "LOG TRUNCATION DETECTED"  → CRITICAL: Immediate investigation
- WARN: "TOPIC RESET DETECTED"      → WARNING: Review recovery actions
- ERROR: "KafkaException"           → Check if caused by truncation
```

---

## 7️⃣ AI Usage Documentation

### Tools and Technologies Used
- **GitHub Copilot**: Used for code generation, error analysis, and understanding error patterns
- **ChatGPT**: Used for conceptual understanding, architectural design, and documentation

### Methodology

#### 1. **Project Setup and Debugging**
I used AI tools to understand and troubleshoot errors during the initial project setup, Kafka build process, and test simulation. This significantly accelerated the debugging cycle when encountering configuration and environment-specific issues.

#### 2. **Kafka Architecture Understanding**
AI assistance was crucial for understanding the internal workings and fundamentals of the Apache Kafka source code. Specifically, I needed to comprehend how the primary cluster, MirrorMaker 2, and secondary cluster interact through the unified codebase, which is fundamental to the implementation.

#### 3. **Design Decisions**
AI tools helped in making informed design decisions for:
- Overall project structure and organization
- Selection of appropriate JAR files to include in Docker images
- Configuration strategies for multi-cluster replication
- Testing strategy and test scenario design

#### 4. **Infrastructure and Testing**
AI assisted in designing:
- Test scripts (`run_challenge.sh`) for simulating normal replication and failure scenarios
- Configuration files (`mm2.properties`) for proper cluster communication
- Docker Compose orchestration files for container management
- Dockerfiles for building custom images with necessary modifications

#### 5. **Implementation of Failure Handling**
AI provided valuable guidance for:
- Understanding the current implementation of `MirrorSourceTask.java`
- Developing optimized solutions for handling failure scenarios in the `poll()` method
- Identifying the appropriate exception types (`OffsetOutOfRangeException`, `UnknownTopicOrPartitionException`)
- Implementing proper recovery mechanisms and error handling patterns

### Key Contributions from AI
- Accelerated the learning curve for Apache Kafka's internal architecture
- Provided code patterns for exception handling and error recovery
- Suggested optimization strategies for both normal operations and edge-case scenarios
- Helped design comprehensive test scenarios to validate the enhancements
- Assisted in documentation and explanation of complex architectural concepts
- Improved code quality through suggestions for better error handling practices

---

## 📝 Cleanup

Clean up Docker resources:

```bash
# Stop and remove all containers
docker-compose down

# Remove all dangling containers
docker container prune -f

# Remove images
docker rmi kafka-mirror-maker:latest commit-log-producer

# Clear KRaft logs (if using local volumes)
rm -rf /tmp/kraft-combined-logs
```
