package pis.group2;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import pis.group2.sth2Kafka;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class data2Kafka extends sth2Kafka<String> {
    private static final String TOPIC_NAME = "test-data";
    private List<String[]> csvData;
    private Integer pointer = 0;
    private Integer size;
    private Integer currentBatchSize = 0;

    public data2Kafka(String BOOTSTRAP_SERVERS, String dataPath) {
        super(TOPIC_NAME, BOOTSTRAP_SERVERS, dataPath);
    }

    @Override
    protected void loadData() {
        csvData = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(DataPath))) {
            String[] line;
            while ((line = reader.readNext()) != null) {
                csvData.add(line);
            }
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }
        size = csvData.size();
    }

    @Override
    protected void sendData() {
        if (pointer >= size) return;

        String[] currentLine = csvData.get(pointer);
        pointer ++;
        byte[] bytes = dataProcessing(currentLine);
        currentBatchSize += bytes.length;

        if (currentBatchSize >= 1024) {
            producer.flush();
            currentBatchSize = 0;
        }
    }

    private byte[] dataProcessing(String[] line){
        String value = String.join(",", line);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, value);
        producer.send(record);

        return value.getBytes(StandardCharsets.UTF_8);
    }
}
