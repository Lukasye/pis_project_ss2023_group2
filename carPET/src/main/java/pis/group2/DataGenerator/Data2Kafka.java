package pis.group2.DataGenerator;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public abstract class Data2Kafka<T> {
    protected String Topic;
    protected String BOOTSTRAP_SERVERS;
    protected String DataPath;
    protected Properties properties;
    protected KafkaProducer<String, T> producer;

    public Data2Kafka(String topic, String BOOTSTRAP_SERVERS, String dataPath) {
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

    abstract void loadData();

    abstract void sendData();

}
