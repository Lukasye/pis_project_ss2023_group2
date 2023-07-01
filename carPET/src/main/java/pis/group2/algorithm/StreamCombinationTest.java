package pis.group2.algorithm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import pis.group2.PETPipeLine.PETPipeLine;
import pis.group2.PETPipeLine.PETProcessor;

import java.util.ArrayList;
import java.util.Arrays;

import static pis.group2.PETLoader.StreamLoader.generateStreams;
import static pis.group2.PETLoader.StreamLoader.mergeStream;


public class StreamCombinationTest {
    public static void main(String[] args) throws Exception {
        if (args.length != 1){
            System.out.println("Wrong Number of arguments!" + args.length + " Arguments can not be resolved!");
            return;
        }
        String path = args[0];
        new PETPipeLine(path) {
            @Override
            public void buildPipeline() {
                env.setParallelism(1);

                ArrayList<Integer> integers = new ArrayList<>();
                integers.add(1);
                integers.add(2);
                integers.add(3);

                String inputPath = "/Users/lukasye/Projects/pis_project_ss2023_group2/carPET/src/main/resources/PIS_data/gps_info_mini.csv";
                DataStreamSource<String> dataStream = env.readTextFile(inputPath);
                ArrayList<DataStream<Tuple2<Object, Class>>> dataSources = generateStreams(dataStream);

                PETProcessor petProcessor = new PETProcessor("LOCATION") {
                    @Override
                    public void processLogic() {
                        DataStream<Tuple2<Object, Class>> tuple2DataStream = loadStream(integers);
                        tuple2DataStream.print();

                    }
                };
                petProcessor.setStreams(dataSources);
                petProcessor.processLogic();


                // Try to combine them
//                DataStream<Tuple2<Object, Class>> stringDataStream = mergeStream(dataSources, integers);
//                DataStream<String> stringDataStream = mergeStream(dataSources, integers);

//                stringDataStream.print("Union");
            }
        };
    }
}
