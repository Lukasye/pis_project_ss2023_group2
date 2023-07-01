package pis.group2.algorithm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import pis.group2.PETPipeLine.PETPipeLine;
import pis.group2.PETPipeLine.PETProcessor;
import pis.group2.beams.SingleReading;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


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

                ArrayList<String> names = new ArrayList<>();
                names.add("Longitude");
                names.add("Latitude");
                names.add("Altitude");
                names.add("Speed");

                ArrayList<String> selectedNames = new ArrayList<>();
                selectedNames.add("Longitude");
                selectedNames.add("Altitude");
                selectedNames.add("Speed");


                String inputPath = "D:\\Projects\\pis_project_ss2023_group2\\carPET\\src\\main\\resources\\PIS_data\\gps_info_mini.csv";
                DataStreamSource<String> dataStream = env.readTextFile(inputPath);
                HashMap<String, DataStream<SingleReading<?>>> stringDataStreamHashMap = SplitStringDataSource(dataStream, names);

                PETProcessor petProcessor = new PETProcessor("LOCATION") {
                    @Override
                    public void processLogic() {
                        DataStream<SingleReading<?>> tuple2DataStream = loadStream(selectedNames);
                        tuple2DataStream.print("result");
//                        testPrint(selectedNames);

                    }
                };
                petProcessor.setStream(stringDataStreamHashMap);
                petProcessor.processLogic();


                // Try to combine them
//                DataStream<Tuple2<Object, Class>> stringDataStream = mergeStream(dataSources, integers);
//                DataStream<String> stringDataStream = mergeStream(dataSources, integers);

//                stringDataStream.print("Union");
            }
        };
    }
}
