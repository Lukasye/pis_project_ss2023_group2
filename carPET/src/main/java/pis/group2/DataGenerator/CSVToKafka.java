package pis.group2.DataGenerator;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CSVToKafka {
    private static final String TOPIC_NAME = "test-data";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String FILE_PATH = "/Users/lukasye/Projects/pis_project_ss2023_group2/carPET/src/main/resources/PIS_data/gps_info.csv";

    int batchSize = 2;
    int delayMs = 1000;

    public static void main(String[] args) {
        // 读取CSV文件内容
        List<String[]> csvData = readCSVFile(FILE_PATH);

        // 将CSV数据写入Kafka
        writeToKafka(csvData);
    }

    private static List<String[]> readCSVFile(String filePath) {
        List<String[]> csvData = new ArrayList<>();

        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] line;
            while ((line = reader.readNext()) != null) {
                csvData.add(line);
            }
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }

        return csvData;
    }

    private static void writeToKafka(List<String[]> csvData) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("batch.size", "1024");
        props.put("linger.ms", "1000");


//        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
//            for (String[] line : csvData) {
//                String value = String.join(",", line);
//                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, value);
//                producer.send(record);
//            }
//        }



        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int currentBatchSize = 0;

            for (String[] line : csvData) {
                String value = String.join(",", line);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, value);
                producer.send(record);

                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                currentBatchSize += bytes.length;

                if (currentBatchSize >= 1024) {
                    producer.flush(); // 触发数据的发送
                    currentBatchSize = 0; // 重置当前批次大小
                    try {
                        Thread.sleep(1000); // 暂停 1 秒，控制发送速度
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            producer.flush(); // 发送剩余的数据
        }




    }
}

