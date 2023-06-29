package pis.group2.Playground;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class upstashConsumerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "quiet-mammoth-10014-eu1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cXVpZXQtbWFtbW90aC0xMDAxNCR5Q9SExiuD84R2x6tDInE2vRF9AdFtb1mH8_o\" password=\"1f1cc05b6f0a4e47828093bff177a4dc\";");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "$GROUP_NAME");

        try(KafkaConsumer consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(Collections.singletonList("test-data"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Received message: key = " + record.key() + ", value = " + record.value());
                }
            }
        }
    }
}
