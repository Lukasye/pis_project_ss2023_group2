package pis.group2.algorithm;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import pis.group2.PETPipeLine.PETPipeLine;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;

public class testPipeline{
    public static void main(String[] args) throws Exception {
        /**
         * Class to test the basic function of the PET Loder
         * Work as an example file
         */

        new PETPipeLine("D:\\Projects\\pis_project_ss2023_group2\\carPET\\config\\Pipeconfig.json") {
            @Override
            public void buildPipeline() {
                env.setParallelism(1);

                // Read Image from kafka topic
                FlinkKafkaConsumer011<byte[]> kafkaSource = new FlinkKafkaConsumer011<>(
                        "test-image", new PETUtils.ReadByteAsStream(), kafkaPropertyImg);
                FlinkKafkaConsumer011<String> sensorDataConsumer = createStringConsumerForTopic("test-data",
                        BOOTSTRAPSERVER, GROUPID);
                DataStreamSource<byte[]> imageSource = env.addSource(kafkaSource);
                DataStreamSource<String> inputStream = env.addSource(sensorDataConsumer);
                // Data transformation
                SingleOutputStreamOperator<SensorReading> sensorStream = inputStream.map(new PETUtils.toSensorReading());
                SingleOutputStreamOperator<SensorReading> ImageSensorReading = imageSource.map(new PETUtils.addImageToReading());

                SingleOutputStreamOperator<SensorReading> dataEvaluatedStream = sensorStream.map(new PETUtils.evaluationData());
                SingleOutputStreamOperator<SensorReading> evaluatedStream = ImageSensorReading.map(new PETUtils.evaluationData());


                SingleOutputStreamOperator<SensorReading> dataOutputStream = dataEvaluatedStream
                        .map(new PETUtils.applyPET(PETconfpath, "SPEED"))
                        .map(new PETUtils.applyPET(PETconfpath, "LOCATION"));
                SingleOutputStreamOperator<SensorReading> outputStream = evaluatedStream.map(
                        new PETUtils.applyPET<byte[]>(PETconfpath, "IMAGE"));
                // Data sink

//                outputStream.addSink(new PETUtils.saveDataAsImage(ImageOutputPath, FILEEXTENSION));
                dataOutputStream.addSink(new PETUtils.sendDataToGUI(this.GUI));
                outputStream.addSink(new PETUtils.showInGUI(this.GUI));
            }
        };
    }
}
