package pis.group2.algorithm;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;

import java.util.Properties;

public class testPipeline{
    public static void main(String[] args) throws Exception {
        PETPipeLine testPipeline = new PETPipeLine("D:\\Projects\\pis_project_ss2023_group2\\carPET\\config\\Pipeconfig.json") {
            @Override
            void buildPipeline() {
                env.setParallelism(1);

                FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<>(
                        "test-image", new SimpleStringSchema(), kafkaProperty);
                SingleOutputStreamOperator<byte[]> imageStream = env.addSource(kafkaSource).map(String::getBytes);

                SingleOutputStreamOperator<SensorReading> ImageSensorReading = imageStream.map(new PETUtils.addImageToReading());

                SingleOutputStreamOperator<SensorReading> outputStream = ImageSensorReading.map(
                        new PETUtils.applyPET<byte[]>(PETconfpath, "IMAGE"));

                outputStream.addSink(new PETUtils.saveDataAsImage(ImageOutputPath));

//                StreamingFileSink<byte[]> fileSink = StreamingFileSink
//                        .forBulkFormat(new Path(ImageOutputPath), createImageEncoder(FILEEXTENSION))
//                        .withBucketAssigner(new PETUtils.ImageBucketAssigner(ImageOutputPath, FILEEXTENSION))
//                        .build();
//
//                outputStream.addSink(fileSink);
            }
        };

        testPipeline.buildPipeline();
        testPipeline.execute();

    }


}
