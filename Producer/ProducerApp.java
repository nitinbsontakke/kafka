import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.HashMap;
import java.util.Map;

public class ProducerApp {

    private static final String TOPIC = "commit-log";
    private static final String BOOTSTRAP_SERVERS = "primary-kafka:9092";

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Reliability configs (important for your replication testing)
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();

        System.out.println("🚀 Starting Commit Log Producer...");

        for (int i = 1; i <= 1000; i++) {

            // Build JSON event
            Map<String, Object> event = new HashMap<>();
            event.put("event_id", UUID.randomUUID().toString());
            event.put("timestamp", Instant.now().getEpochSecond());
            event.put("op_type", "UPDATE");

            String key = "doc:" + UUID.randomUUID().toString().substring(0, 4);
            event.put("key", key);

            Map<String, String> value = new HashMap<>();
            value.put("status", "archived");
            event.put("value", value);

            String json = objectMapper.writeValueAsString(event);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, key, json);

            // Send async with callback
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println(
                        "✅ Produced event -> " +
                        "partition=" + metadata.partition() +
                        ", offset=" + metadata.offset() +
                        ", key=" + key +
                        ", payload=" + json
                    );
                } else {
                    System.err.println("❌ Failed to produce event: " + exception.getMessage());
                }
            });

            // Small delay (helps in observing logs + MM2 behavior)
            Thread.sleep(10);
        }

        producer.flush();
        producer.close();

        System.out.println("🎯 Finished producing 1000 events.");
    }
}