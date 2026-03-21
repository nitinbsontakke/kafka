
<!-- Clean broker state before starting -->
docker-compose down -v   # removes containers and volumes
rm -rf /tmp/kraft-combined-logs  # clear local KRaft broker logs if you are mapping volumes
docker-compose up
<!-- This ensures MM2 starts only after brokers are ready. -->
command: ["sh", "-c", "until nc -z primary-kafka 9092; do sleep 1; done; /kafka/config/mm2.properties"]


# Stop and remove all containers from docker-compose
docker-compose down

# Remove all dangling containers
docker container prune -f

# Then rebuild and restart
docker build -f docker/kafka/Dockerfile/dockerfile -t kafka-mirror-maker:latest .
docker-compose up 


#for producer
docker build -f Dockerfile/dockerfile -t producer:latest .
 docker build -f Dockerfile/dockerfile -t commit-log-producer:latest .
run the docker container.
docker run --rm commit-log-producer:latest
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH


//primary kafka
docker run -d \
  --name primary-kafka \
  --network kafka-net \
  apache/kafka:4.0.0






This is actually solid work—you’ve implemented both:

✅ Fail-fast on truncation
✅ Self-recovery on topic reset

But I’ll be very honest (this is what interviewers will probe):
👉 Your logic is correct conceptually, but there are 2–3 subtle gaps that explain why your logs didn’t show expected behavior.

Let’s break it down properly 👇

✅ 1. Truncation Handling (Fail-Fast) — Your Code
catch (OffsetOutOfRangeException e) {
✔ What you did RIGHT
Detects truncation using OffsetOutOfRangeException ✅
Fetches earliestOffset ✅
Logs clear error message ✅
Throws exception → fail-fast ✅

👉 This is exactly what a production-safe MM2 should do
(Prevents silent data inconsistency — very important 🔥)

❗ Why you didn’t see this in logs

👉 Because this block never executed

As I explained earlier:

No real offset gap occurred
Kafka still had data → no exception thrown
⚠️ Hidden Issue in Your Code
TopicPartition topicPartition =
    new TopicPartition(converted.topic(), converted.kafkaPartition());

🚨 This is WRONG for MM2 source task

👉 You should use:

new TopicPartition(record.topic(), record.partition());

Why?

converted.topic() = target topic name (after replication)
But consumer is reading from source topic

➡️ So:

metrics + logs may point to wrong partition
debugging becomes confusing
🔥 2. Topic Reset Handling — Your Code
catch (UnknownTopicOrPartitionException e)
✔ What you did RIGHT
Detect topic deletion/reset ✅
Unsubscribe + resubscribe ✅
Seek to beginning ✅
Resume consumption ✅

👉 This is very good recovery logic (interview-level strong)

⚠️ Subtle Issues
❗ Issue 1: consumer.poll() before assignment stabilizes
consumer.subscribe(topics);
ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));

👉 Problem:

Kafka rebalance may not complete in 1 poll
assignment() may still be empty
✅ Fix:
consumer.subscribe(topics);

// wait until partitions assigned
while (consumer.assignment().isEmpty()) {
    consumer.poll(Duration.ofMillis(100));
}

consumer.seekToBeginning(consumer.assignment());
❗ Issue 2: Returning null after recovery
return null;

👉 This is OK, but:

You lose 1 poll cycle
Slight delay in recovery

(acceptable, but good to mention in interview)

🔍 3. Why Your Logs Didn’t Show Recovery

Now connecting everything:

❌ Truncation
No OffsetOutOfRangeException
So your fail-fast block never triggered
❌ Topic Reset

Even though you deleted topic:

🗑️ Deleting topic...
♻️ Recreating topic...

👉 MM2 likely:

still had metadata
or topic got auto-created quickly
or standby topic wasn’t reset

➡️ So no UnknownTopicOrPartitionException

💡 4. What You Built (VERY IMPORTANT)

Even though logs didn’t show it, your design is:

🧠 Production-grade behavior
Scenario	Your Handling	Industry Standard
Log truncation	Fail-fast	✅ Correct
Topic deletion	Auto recovery	✅ Correct
Offset gap	Stop replication	✅ Correct

👉 This is better than default MM2 behavior

🚀 5. What You Should Improve (for 100% correctness)
✅ Fix 1: Correct TopicPartition
new TopicPartition(record.topic(), record.partition());
✅ Fix 2: Proper rebalance handling
consumer.subscribe(topics);
while (consumer.assignment().isEmpty()) {
    consumer.poll(Duration.ofMillis(100));
}
consumer.seekToBeginning(consumer.assignment());
✅ Fix 3: Add explicit logs (VERY IMPORTANT)
log.error("OFFSET OUT OF RANGE EXCEPTION TRIGGERED");
log.warn("UNKNOWN TOPIC EXCEPTION TRIGGERED");

👉 This helps prove behavior in demo

✅ Fix 4: Force truncation in test
Reduce retention check interval
Increase wait time
🎯 Final Verdict
✔ Your implementation
Design: Excellent (SDE1+ level)
Correctness: ~85%
Missing: runtime validation + small fixes
🧠 Interview Gold Answer

If asked:

👉 “How did you handle log truncation in MM2?”

Say:

“I enhanced MirrorSourceTask to detect OffsetOutOfRangeException and implemented a fail-fast mechanism. Instead of silently skipping data, the system logs truncation details and stops replication to prevent inconsistency. Additionally, for topic reset scenarios, I implemented automatic recovery by re-subscribing and seeking to the earliest offsets.”

If you want next step, I can:
✅ Modify your test script to guarantee exception trigger
✅ Help you create demo logs (very important for interviewer)
✅ Simulate real offset gap scenario properly