package pis.group2.algorithm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import pis.group2.PETPipeLine.PETPipeLine;
import pis.group2.PETPipeLine.PETProcessor;
import pis.group2.beams.generalSensorReading;

import java.util.ArrayList;
import java.util.Arrays;


public class Variation3 {
    public static void main(String[] args) throws Exception {
        if (args.length != 1){
            System.out.println("Wrong Number of arguments!" + args.length + " Arguments can not be resolved!");
            return;
        }
        String path = args[0];
        new PETPipeLine(path) {
            @Override
            public void buildPipeline() throws Exception {
                env.setParallelism(1);

                String inputPath = "/Users/lukasye/Projects/pis_project_ss2023_group2/carPET/src/main/resources/PIS_data/gps_info_mini.csv";
                DataStreamSource<String> dataStream = env.readTextFile(inputPath);
//                HashMap<String, DataStream<SingleReading<?>>> stringDataStreamHashMap = SplitStringDataSource(dataStream, names);

                PETProcessor speedProcessor = new PETProcessor(this.PETconfpath, "SPEED") {

                    @Override
                    public DataStream<Tuple2<Integer, String>> evaluation() {
                        return StreamLoader.getRawStream().map(new MapFunction<String, Tuple2<Integer, String>>() {
                            private int counter = 0;
                            @Override
                            public Tuple2<Integer, String> map(String s) throws Exception {
                                counter ++;
                                return new Tuple2<>(counter > 5 ? 1 : 0, s);
                            }
                        });
                    }
                };

                PETProcessor locationProcessor = new PETProcessor(this.PETconfpath, "LOCATION") {
                    @Override
                    public DataStream<Tuple2<Integer, String>> evaluation() {
                        return StreamLoader.getRawStream().map(new MapFunction<String, Tuple2<Integer, String>>() {
                            private int counter = 0;
                            @Override
                            public Tuple2<Integer, String> map(String s) throws Exception {
                                counter ++;
                                return new Tuple2<>(counter > 5 ? 1 : 0, s);
                            }
                        });
                    }
                };
                SingleOutputStreamOperator<generalSensorReading> speedResult = speedProcessor.run(dataStream);
                SingleOutputStreamOperator<generalSensorReading> locationResult = locationProcessor.run(dataStream);
                speedResult.print("Speed");
                locationResult.print("Location");
            }
        };
    }
}
