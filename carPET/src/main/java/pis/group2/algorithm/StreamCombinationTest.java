package pis.group2.algorithm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import pis.group2.PETPipeLine.PETPipeLine;
import pis.group2.PETPipeLine.PETProcessor;
import pis.group2.beams.SingleReading;

import java.util.ArrayList;
import java.util.Arrays;
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
            public void buildPipeline() throws Exception {
                env.setParallelism(1);

                String inputPath = "/Users/lukasye/Projects/pis_project_ss2023_group2/carPET/src/main/resources/PIS_data/gps_info_mini.csv";
                DataStreamSource<String> dataStream = env.readTextFile(inputPath);
//                HashMap<String, DataStream<SingleReading<?>>> stringDataStreamHashMap = SplitStringDataSource(dataStream, names);

                PETProcessor petProcessor = new PETProcessor(this.PETconfpath, "SPEED") {
                    @Override
                    public DataStream<Tuple3<Integer, ArrayList<Integer>, String>> evaluation() {
                        return StreamLoader.getRawStream().map(new MapFunction<String, Tuple3<Integer, ArrayList<Integer>, String>>() {
                            private int counter = 0;
                            @Override
                            public Tuple3<Integer, ArrayList<Integer>, String> map(String s) throws Exception {
                                counter ++;
                                ArrayList<Integer> list = new ArrayList<>(Arrays.asList(0, counter > 5 ? 2 : 1));
                                return new Tuple3<>(counter > 5 ? 2 : 0, list, s);
                            }
                        });
                    }
                };
                petProcessor.run(dataStream);
            }
        };
    }

    public static class MyMapFunction implements MapFunction<SingleReading<?>, SingleReading<?>> {
        private int counter = 0;
        private final transient PETProcessor processor;
        private final ArrayList<String> changedNames;

        public MyMapFunction(PETProcessor petProcessor) {
            this.processor = petProcessor;
            changedNames = new ArrayList<>();
                changedNames.add("LON");
            changedNames.add("Alt");
            changedNames.add("LAT");
        }

        @Override
        public SingleReading<?> map(SingleReading<?> singleReading) throws Exception {
            System.out.println(processor);
            counter++;
            if (counter > 10) {
                processor.loadStream(changedNames);
            }
            return singleReading;
        }
    }
}
