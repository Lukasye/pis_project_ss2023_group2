package pis.group2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class img2Kafka extends sth2Kafka<byte[]> {
    private static final String TOPIC_NAME = "test-image";
    private Integer pointer = 0;
    private Integer size;
    private List<File> orderedFiles;

    public img2Kafka(String BOOTSTRAP_SERVERS, String dataPath) {
        super(TOPIC_NAME, BOOTSTRAP_SERVERS, dataPath);
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(properties);
    }

    @Override
    protected void loadData() {
        // Read PNG image files from the folder
        File folder = new File(DataPath);
        File[] imageFiles = folder.listFiles();

        orderedFiles  = new ArrayList<>();
        // Sort the image files based on file name for ordered processing
        if (imageFiles != null) {
            orderedFiles.addAll(Arrays.asList(imageFiles));
            orderedFiles.sort(Comparator.comparing(img2Kafka::getNumericOrder));
        }
        size = orderedFiles.size();
    }

    @Override
    protected void sendData() throws IOException {

        if (pointer >= size){
            System.out.println("Datasource already empty!");
            return;
        }
//        System.out.println(orderedFiles.get(pointer));
        BufferedImage image = ImageIO.read(orderedFiles.get(pointer));
        pointer++;

        // Convert image to byte array
        ByteArrayOutputStream by_img = new ByteArrayOutputStream();
        ImageIO.write(image, "png", by_img);
        byte[] imageData = by_img.toByteArray();

        // Publish image data to Kafka
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC_NAME, "image-key", imageData);
        producer.send(record);

    }

    private static int getNumericOrder(File file) {
        String fileName = file.getName();
        String numericPart = fileName.replaceAll("[^0-9]", "");

        if (numericPart.isEmpty()) {
            // Assign a large value for files without numeric order
            return Integer.MAX_VALUE;
        } else {
            return Integer.parseInt(numericPart);
        }
    }
}
