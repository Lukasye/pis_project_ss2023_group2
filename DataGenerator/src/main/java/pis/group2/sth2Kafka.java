package pis.group2;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public abstract class sth2Kafka<T> {
    protected String Topic;
    protected String BOOTSTRAP_SERVERS;
    protected String DataPath;
    protected Properties properties;
    protected KafkaProducer<String, T> producer;

    public sth2Kafka(String topic, String BOOTSTRAP_SERVERS, String dataPath) {
        Topic = topic;
        DataPath = dataPath;
        this.BOOTSTRAP_SERVERS = BOOTSTRAP_SERVERS;
        initialize();
    }

    public void initialize(){
        properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(properties);
    }

    protected abstract void loadData();

    protected abstract void sendData();

}
