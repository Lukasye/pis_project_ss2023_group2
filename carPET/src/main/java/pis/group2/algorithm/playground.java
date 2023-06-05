package pis.group2.algorithm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pis.group2.beams.SensorReading;
import privacyBA.processing.algorithm.speed.SpeedAnonymizer;

import java.time.Duration;


public class playground {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // initialize speed anonymization
        int gamma = 10;

        SpeedAnonymizer sa = new SpeedAnonymizer(4, 8, gamma, Duration.ofSeconds(1), 2.5);

        String FilePath = "src/main/resources/PIS_data/gps_info.csv";
        DataStreamSource<String> inputStream = env.readTextFile(FilePath);

        SingleOutputStreamOperator<SensorReading> sensorStream = inputStream.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(new Double(fields[0]),
                        new Double(fields[1]),
                        new Double(fields[2]),
                        new Double(fields[3]),
                        new Double(fields[12]),
                        new Double(fields[13]),
                        new Double(fields[9]));
            }
        });

        SingleOutputStreamOperator<Float> averageVelocityStream = sensorStream.map(new AverageMap());

        SingleOutputStreamOperator<Double> resultStream = averageVelocityStream.map(new MapFunction<Float, Double>() {
            @Override
            public Double map(Float aFloat) throws Exception {
                return sa.process(aFloat);
            }
        });

        averageVelocityStream.print("Average Speed");
        resultStream.print("PETStream");

        env.execute();
    }

    public static class AverageMap implements MapFunction<SensorReading, Float>{
        private float  sumVelocity = 0.0F;
        private int sumNumber = 0;

        public Float map(SensorReading sensorReading) throws Exception {
            sumVelocity += sensorReading.getVel();
            sumNumber ++;
            return sumVelocity / sumNumber;
        }
    }
}
