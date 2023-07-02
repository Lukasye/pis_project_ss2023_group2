package pis.group2.PETPipeLine;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import pis.group2.PETLoader.PETLoader;
import pis.group2.PETLoader.StreamLoader;
import pis.group2.beams.SingleReading;
import pis.group2.beams.generalSensorReading;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public abstract class PETProcessor implements Serializable {
    private ArrayList<Integer> StreamIndex;
    protected final StreamLoader StreamLoader;
    private final PETLoader<?> PETLoader;
    private final String TYPE;

    public PETProcessor(String confPath, String TYPE) throws Exception {
        StreamLoader = new StreamLoader();
        this.TYPE = TYPE;
        this.PETLoader = new PETLoader<>(confPath, TYPE, 0);
    }

    public void setStream(HashMap<String, DataStream<SingleReading<?>>> input){
        StreamLoader.setStreamMap(input);
    }

    public DataStream<SingleReading<?>> loadStream(ArrayList<String> names){
        return StreamLoader.combineStreams(names);
    }

    public void run(DataStream<String> input, ArrayList<String> NameList){
        this.setRawStream(input, NameList);
        this.processLogic();
    }

    private void setRawStream(DataStream<String> input, ArrayList<String> NameList){
        StreamLoader.setRawStream(input, NameList);
    }

    public void testPrint(){
        StreamLoader.testPrint();
    }

    public void testPrint(ArrayList<String> names){
        StreamLoader.testPrint(names);
    }

    public void processLogic() {
        DataStream<Tuple2<Integer, ArrayList<Integer>>> evaluation = evaluation();
        SingleOutputStreamOperator<Tuple2<Integer, ArrayList<Integer>>> filter = evaluation.filter(new FilterFunction<Tuple2<Integer, ArrayList<Integer>>>() {
            private ArrayList<Integer> latest;
            @Override
            public boolean filter(Tuple2<Integer, ArrayList<Integer>> integerArrayListTuple2) throws Exception {
                ArrayList<Integer> strings = integerArrayListTuple2.f1;
                if (latest == null){
                    latest = strings;
                    return true;
                }
                if (!latest.equals(strings)){
                    latest = strings;
                    return true;
                }
                return false;
            }
        });

        SingleOutputStreamOperator<generalSensorReading> dataSource = filter.connect(StreamLoader.getRawStream()).map(new selectData());
        dataSource.print();
        applyPET();
    }

    public abstract DataStream<Tuple2<Integer, ArrayList<Integer>>> evaluation();
    public abstract void applyPET();

    public static class selectData extends RichCoMapFunction<Tuple2<Integer, ArrayList<Integer>>, String, generalSensorReading>{
        private ListState<Integer> dataTitle;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("dataTitle", Integer.class);
            dataTitle = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public generalSensorReading map1(Tuple2<Integer, ArrayList<Integer>> integerArrayListTuple2) throws Exception {
            dataTitle.update(integerArrayListTuple2.f1);
            return null;
        }

        @Override
        public generalSensorReading map2(String s) throws Exception {
            String[] split = s.split(",");
            Iterable<Integer> integers = dataTitle.get();
            generalSensorReading generalSensorReading = new generalSensorReading();
            for (Integer index: integers){
                generalSensorReading.addData(index, Double.valueOf(split[index]));
            }
            return generalSensorReading;
        }
    }

}
