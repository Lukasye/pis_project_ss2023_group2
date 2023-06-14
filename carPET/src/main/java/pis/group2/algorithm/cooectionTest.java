package pis.group2.algorithm;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;

import java.text.SimpleDateFormat;

public class cooectionTest {
    public static void main(String[] args) throws Exception {
        new PETPipeLine("D:\\Projects\\pis_project_ss2023_group2\\carPET\\config\\Pipeconfig.json") {
            @Override
            void buildPipeline() {
                // Read Image from kafka topic
                FlinkKafkaConsumer011<byte[]> kafkaSource = new FlinkKafkaConsumer011<>(
                        "test-image", new PETUtils.ReadByteAsStream(), kafkaPropertyImg);
                FlinkKafkaConsumer011<String> sensorDataConsumer = createStringConsumerForTopic("test-data",
                        BOOTSTRAPSERVER, GROUPID);
                DataStreamSource<byte[]> imageSource = env.addSource(kafkaSource);
                DataStreamSource<String> dataSource = env.addSource(sensorDataConsumer);

                ConnectedStreams<byte[], String> connectStream = imageSource.connect(dataSource);
                ConnectedStreams<byte[], String> stringConnectedStreams = connectStream.keyBy(new KeySelector<byte[], Object>() {
                    @Override
                    public Object getKey(byte[] bytes) throws Exception {
                        return new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
                    }
                }, new KeySelector<String, Object>() {
                    @Override
                    public Object getKey(String s) throws Exception {
                        return s;
                    }
                });

                SingleOutputStreamOperator<SensorReading> mergedStream = stringConnectedStreams.process(new PETUtils.ImageDataMerge());

                mergedStream.print();

            }
        };
    }
}
