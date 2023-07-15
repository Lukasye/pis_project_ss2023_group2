package pis.group2.algorithm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import pis.group2.PETPipeLine.PETPipeLine;
import pis.group2.PETPipeLine.PETProcessor;
import pis.group2.beams.generalSensorReading;
import pis.group2.utils.PETUtils;



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
                this.initKafka();
                env.setParallelism(1);

                // Mini Sample test
//                String inputPath = "D:\\Projects\\pis_project_ss2023_group2\\carPET\\src\\main\\resources\\PIS_data\\gps_info_mini.csv";
//                DataStreamSource<String> dataStream = env.readTextFile(inputPath);

//                HashMap<String, DataStream<SingleReading<?>>> stringDataStreamHashMap = SplitStringDataSource(dataStream, names);


                PETProcessor speedProcessor = new PETProcessor(this.PETconfpath, "SPEED") {

                    @Override
                    public DataStream<Tuple2<Integer, String>> evaluation() {
                        return StreamLoader.getRawStream().map(new MapFunction<String, Tuple2<Integer, String>>() {
                            private int counter = 0;
                            @Override
                            public Tuple2<Integer, String> map(String s) throws Exception {
                                counter ++;
                                return new Tuple2<>(DummyPolicySelector(counter), s);
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
                                return new Tuple2<>(DummyPolicySelector(counter), s);
                            }
                        });
                    }
                };
                SingleOutputStreamOperator<generalSensorReading> speedResult = speedProcessor.run(dataSource);
                SingleOutputStreamOperator<generalSensorReading> locationResult = locationProcessor.run(dataSource);
//                speedResult.print("Speed");
//                locationResult.print("Location");
                speedResult.addSink(new PETUtils.DataWrapperToCSV.generalSensorReadingToCSV(
                        "D:\\Projects\\pis_project_ss2023_group2\\carPET\\src\\main\\resources\\result\\variation3_speed.csv", ","));
                locationResult.addSink(new PETUtils.DataWrapperToCSV.generalSensorReadingToCSV(
                        "D:\\Projects\\pis_project_ss2023_group2\\carPET\\src\\main\\resources\\result\\variation3_location.csv", ","));
                speedResult.print();
                locationResult.print();
            }
        };
    }

    public static int DummyPolicySelector(int Counter){
        if (Counter < 50){
            return 0;
        } else {
            return (Counter % 2 )== 0? 1: 0;
        }
    }
}
