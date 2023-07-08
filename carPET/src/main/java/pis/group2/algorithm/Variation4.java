package pis.group2.algorithm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import pis.group2.PETPipeLine.PETPipeLine;
import java.util.ArrayList;
import java.util.HashMap;

import static pis.group2.PETLoader.StreamLoader.SplitStringDataSource;

public class Variation4 {
    public static void main(String[] args) throws Exception {
        if (args.length != 1){
            System.out.println("Wrong Number of arguments!" + args.length + " Arguments can not be resolved!");
            return;
        }
        String path = args[0];
        new PETPipeLine(path) {
            @Override
            public void buildPipeline() throws Exception {
                // initialisation settings
                env.setParallelism(1);

                ArrayList<String> names = new ArrayList<>();
                names.add("Longitude");
                names.add("Latitude");
                names.add("Altitude");
                names.add("Speed");

                String inputPath = "/Users/lukasye/Projects/pis_project_ss2023_group2/carPET/src/main/resources/PIS_data/gps_info_mini.csv";
                DataStream<String> dataStream = env.readTextFile(inputPath);
                HashMap<String, DataStream<Tuple2<String, Double>>> stringDataStreamHashMap = SplitStringDataSource(dataStream, names);
                DataStream<Tuple2<String, Double>> fooStream = stringDataStreamHashMap.get("Speed");
                DataStream<Tuple2<String, Double>> ControlStream = stringDataStreamHashMap.get("Speed");

//                for (String foo : names){
//                    InputStream = InputStream.union(stringDataStreamHashMap.get(foo));
//                }
//                ControlStream.flatMap(new fooMap(fooStream, stringDataStreamHashMap)).name("Hallo");
                ControlStream.process(new FooProcessFunction(stringDataStreamHashMap));

                fooStream.print("ASDFASDF");

            }
        };
    }

    public static class fooMap implements FlatMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {
        private transient DataStream<Tuple2<String, Double>> fooStream;
        private int counter = 0;
        public transient HashMap<String, DataStream<Tuple2<String, Double>>> stringDataStreamHashMap;

        public fooMap(DataStream<Tuple2<String, Double>> fooStream, HashMap<String, DataStream<Tuple2<String, Double>>> stringDataStreamHashMap) {
            this.fooStream = fooStream;
            this.stringDataStreamHashMap = stringDataStreamHashMap;
        }

        @Override
        public void flatMap(Tuple2<String, Double> stringDoubleTuple2, Collector<Tuple2<String, Double>> collector) throws Exception {
            counter++;
            if (counter == 5) {
                fooStream = fooStream.union(stringDataStreamHashMap.get("Longitude"));
            }
        }
    }


    public static class FooProcessFunction extends ProcessFunction<Tuple2<String, Double>, Tuple2<String, Double>> {
        private final transient HashMap<String, DataStream<Tuple2<String, Double>>> stringDataStreamHashMap;
        private transient DataStream<Tuple2<String, Double>> fooStream;
        private int counter = 0;

        public FooProcessFunction(HashMap<String, DataStream<Tuple2<String, Double>>> stringDataStreamHashMap) {
            this.stringDataStreamHashMap = stringDataStreamHashMap;
        }

        @Override
        public void processElement(Tuple2<String, Double> value, Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
            counter++;
            if (counter == 5) {
                fooStream = fooStream.union(stringDataStreamHashMap.get("Longitude"));
            }

            // Process the element and emit it
            out.collect(value);
        }
    }
}
