package pis.group2.algorithm;

import org.apache.flink.api.common.functions.MapFunction;
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

import static pis.group2.PETLoader.StreamLoader.SplitStringDataSource;


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

                ArrayList<String> names = new ArrayList<>();
                names.add("LON");
                names.add("LAT");
                names.add("ALT");
                names.add("VEL");

                String inputPath = "D:\\Projects\\pis_project_ss2023_group2\\carPET\\src\\main\\resources\\PIS_data\\gps_info_mini.csv";
                DataStreamSource<String> dataStream = env.readTextFile(inputPath);
//                HashMap<String, DataStream<SingleReading<?>>> stringDataStreamHashMap = SplitStringDataSource(dataStream, names);

                PETProcessor petProcessor = new PETProcessor(this.PETconfpath, "LOCATION") {

                    @Override
                    public DataStream<Tuple2<Integer, ArrayList<Integer>>> evaluation() {
                        return this.StreamLoader.getRawStream().map(new MapFunction<String, Tuple2<Integer, ArrayList<Integer>>>() {
                            private int counter = 0;
                            @Override
                            public Tuple2<Integer, ArrayList<Integer>> map(String s) throws Exception {
                                counter ++;
                                if (counter < 4){
                                    return new Tuple2<>(0, new ArrayList<>(Arrays.asList(0, 2, 3)));
                                }else {
                                    return new Tuple2<>(1, new ArrayList<>(Arrays.asList(0, 1, 3)));
                                }
                            }
                        });
                    }

                    @Override
                    public void applyPET() {

                    }
                };
                petProcessor.run(dataStream, names);
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
