package pis.group2.algorithm;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;

import java.util.Properties;

public class testPipeline extends PETPipeLine{

    public testPipeline(String confPath) throws Exception {
        super(confPath);
    }

    @Override
    void buildPipeline() {

//        FlinkKafkaConsumer011<String> sensorDataConsumer = createStringConsumerForTopic("test-data",
//                "localhost:9092", "test-consumer-group");
//        DataStreamSource<String> inputStream = env.addSource(sensorDataConsumer);

        String FilePath = "src/main/resources/PIS_data/gps_info.csv";
        DataStreamSource<String> inputStream = env.readTextFile(FilePath);

        SingleOutputStreamOperator<SensorReading> sensorStream = inputStream.map(new PETUtils.toSensorReading());

        SingleOutputStreamOperator<SensorReading> evaluatedStream = sensorStream.map(new PETUtils.evaluationData());

        SingleOutputStreamOperator<SensorReading> speedStream = evaluatedStream.map(
                new PETUtils.applyPET(PETconfpath, "SPEED"));

        SingleOutputStreamOperator<SensorReading> locationStream = speedStream.map(new PETUtils.applyPET(PETconfpath, "LOCATION"));
        locationStream.print();
    }
    public static void main(String[] args) throws Exception {
        testPipeline testPipeline = new testPipeline("config/Pipeconfig.json");
        testPipeline.buildPipeline();
        testPipeline.execute();

    }


}
