package pis.group2.algorithm;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;

public class demoPipeLine {
    public static void main(String[] args) throws Exception {
        PETPipeLine petPipeLine = new PETPipeLine("config/Pipeconfig.json") {
            @Override
            void buildPipeline() {
                env.setParallelism(1);
                String FilePath = "src/main/resources/PIS_data/gps_info.csv";
                DataStreamSource<String> inputStream = env.readTextFile(FilePath);
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
