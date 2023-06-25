package pis.group2.algorithm;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;

public class demoPipeLine {
    public static void main(String[] args) throws Exception {

        PETPipeLine petPipeLine = new PETPipeLine("D:\\Projects\\pis_project_ss2023_group2\\carPET\\config\\Pipeconfig.json") {
            @Override
            void buildPipeline() {
                FlinkKafkaConsumer011<String> sensorDataConsumer = createStringConsumerForTopic("test-data",
                BOOTSTRAPSERVER, GROUPID);
                DataStreamSource<String> inputStream = env.addSource(sensorDataConsumer);

                SingleOutputStreamOperator<SensorReading> sensorStream = inputStream.map(new PETUtils.toSensorReading());
                SingleOutputStreamOperator<SensorReading> evaluatedStream = sensorStream.map(new PETUtils.evaluationData());
                SingleOutputStreamOperator<SensorReading> outputStream = evaluatedStream
                        .map(new PETUtils.applyPET(PETconfpath, "SPEED"))
                        .map(new PETUtils.applyPET(PETconfpath, "LOCATION"));
                outputStream.print();
            }
        };

        petPipeLine.buildPipeline();
        petPipeLine.execute();
    }
}
