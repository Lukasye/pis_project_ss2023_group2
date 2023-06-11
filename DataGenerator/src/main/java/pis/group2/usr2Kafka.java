package pis.group2;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class usr2Kafka extends sth2Kafka<String> {
    private static final String TOPIC_NAME = "test-user-input";
    private String Command;

    public usr2Kafka(String BOOTSTRAP_SERVERS) throws IOException {
        super(TOPIC_NAME, BOOTSTRAP_SERVERS, "");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
    }

    @Override
    protected void loadData() {

    }

    @Override
    protected void sendData() {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "gps", Command);
        producer.send(record);
        producer.flush();
    }

    protected void sendCommand(String command){
        setCommand(command);
        sendData();
    }

    public String getCommand() {
        return Command;
    }

    public void setCommand(String command) {
        Command = command;
    }
}
