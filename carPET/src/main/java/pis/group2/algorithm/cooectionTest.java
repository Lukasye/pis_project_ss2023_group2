package pis.group2.algorithm;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;
import scala.Tuple2;

public class cooectionTest {
    public static void main(String[] args) throws Exception {
        new PETPipeLine("D:\\Projects\\pis_project_ss2023_group2-main\\carPET\\config\\Pipeconfig.json") {
            @Override
            void buildPipeline() {
                env.setParallelism(1);
                // Read Image from kafka topic
                FlinkKafkaConsumer011<byte[]> kafkaSource = new FlinkKafkaConsumer011<>(
                        "test-image", new PETUtils.ReadByteAsStream(), kafkaPropertyImg);
                FlinkKafkaConsumer011<String> sensorDataConsumer = createStringConsumerForTopic("test-data",
                        BOOTSTRAPSERVER, GROUPID);
                FlinkKafkaConsumer011<String> userDataConsumer = createStringConsumerForTopic("test-user-input",
                        BOOTSTRAPSERVER, GROUPID);
                DataStreamSource<byte[]> imageSource = env.addSource(kafkaSource);
                DataStreamSource<String> dataSource = env.addSource(sensorDataConsumer);
                DataStreamSource<String> userSource = env.addSource(userDataConsumer);

                // Merge two Stream
                ConnectedStreams<byte[], String> connectedDataStream = imageSource.connect(dataSource);
                SingleOutputStreamOperator<SensorReading> SensorReadingStream = connectedDataStream.flatMap(new PETUtils.assembleSensorReading());

                // Duplicate filter
                SingleOutputStreamOperator<SensorReading> filteredStream = SensorReadingStream.filter(new PETUtils.duplicateCheck());

                // Evaluation
                SingleOutputStreamOperator<SensorReading> evaluatedStream = filteredStream.connect(userSource).flatMap(new PETUtils.evaluateSensorReading());

                //Apply PET
                SingleOutputStreamOperator<SensorReading> resultStream = evaluatedStream.map(new PETUtils.applyPET<Double>(PETconfpath, "SPEED"))
                        .map(new PETUtils.applyPET<Tuple2<Double, Double>>(PETconfpath, "LOCATION"))
                        .map(new PETUtils.applyPET<byte[]>(PETconfpath, "IMAGE"));

                // Sink
                resultStream.addSink(new PETUtils.showInGUI(GUI));

            }
        };
    }
}
