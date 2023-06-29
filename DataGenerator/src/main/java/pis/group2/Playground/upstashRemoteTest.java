package pis.group2.Playground;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class upstashRemoteTest {

    public static void main(String[] args) {
        String TOPIC_NAME = "test-data";

        Properties props = new Properties();
        props.put("bootstrap.servers", "quiet-mammoth-10014-eu1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cXVpZXQtbWFtbW90aC0xMDAxNCR5Q9SExiuD84R2x6tDInE2vRF9AdFtb1mH8_o\" password=\"1f1cc05b6f0a4e47828093bff177a4dc\";");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer producer = new KafkaProducer<String, String>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "gps","Hello");
            producer.send(record);
            producer.flush();
        }
    }
}
