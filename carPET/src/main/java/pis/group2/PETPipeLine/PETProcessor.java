package pis.group2.PETPipeLine;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
import pis.group2.utils.PETUtils;

import java.io.Serializable;
import java.util.*;

public abstract class PETProcessor implements Serializable {
    private ArrayList<Integer> StreamIndex;
    protected final StreamLoader StreamLoader;
    private final PETLoader<?> PETLoader;
    private final String PETConfPath;
    private final String TYPE;

    public PETProcessor(String confPath, String TYPE) throws Exception {
        StreamLoader = new StreamLoader();
        this.TYPE = TYPE;
        this.PETLoader = new PETLoader<>(confPath, TYPE, 0);
        this.PETConfPath = confPath;
    }

    public void setStream(HashMap<String, DataStream<SingleReading<?>>> input){
        StreamLoader.setStreamMap(input);
    }

    public DataStream<SingleReading<?>> loadStream(ArrayList<String> names){
        return StreamLoader.combineStreams(names);
    }

    public void run(DataStream<String> input){
        this.setRawStream(input);
        this.processLogic();
    }

    private void setRawStream(DataStream<String> input){
        StreamLoader.setRawStream(input);
    }

    public void testPrint(){
        StreamLoader.testPrint();
    }

    public void testPrint(ArrayList<String> names){
        StreamLoader.testPrint(names);
    }

    public void processLogic() {
//        SingleOutputStreamOperator<Tuple3<Integer, ArrayList<Integer>, String>> evaluatedRawStream = this.StreamLoader.getRawStream().map(new MapFunction<String, Tuple3<Integer, ArrayList<Integer>, String>>() {
//            @Override
//            public Tuple3<Integer, ArrayList<Integer>, String> map(String s) throws Exception {
//                Integer index = evaluateRawData(s);
//                // Only for testing!!!!!
//                ArrayList<Integer> tmp = new ArrayList<>(Arrays.asList(0, index == 1 ? 2 : 1, 3));
//                return new Tuple3<>(index, tmp, s);
////                return new Tuple3<>(index, PETLoader.getComponents(), s);
//            }
//        });
        DataStream<Tuple3<Integer, ArrayList<Integer>, String>> evaluatedRawStream = evaluation();

        SingleOutputStreamOperator<generalSensorReading> dataSource = evaluatedRawStream.map(new selectData());
        SingleOutputStreamOperator<generalSensorReading> filteredDataSource = dataSource.filter(new FilterFunction<generalSensorReading>() {
            @Override
            public boolean filter(generalSensorReading generalSensorReading) throws Exception {
                return !(generalSensorReading == null);
            }
        });
//        filteredDataSource.print();
        SingleOutputStreamOperator<generalSensorReading> resultStream = filteredDataSource.map(new PETUtils.applyPETForGeneralSensorReading<>(PETConfPath, TYPE));
    }

    public abstract DataStream<Tuple3<Integer, ArrayList<Integer>, String>> evaluation();
    public Integer evaluateRawData(String s) throws NotImplementedException {
        throw new NotImplementedException("");
    }


//    public abstract void applyPET();

    public static class selectData extends RichMapFunction<Tuple3<Integer, ArrayList<Integer>, String>, generalSensorReading> {
//        private ListState<Integer> dataTitle;
        private ArrayList<Integer> foobar;
        private Integer policy;

        public selectData() {
            this.foobar = new ArrayList<>();
            policy = 0;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
//            ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("dataTitle", Integer.class);
//            dataTitle = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public generalSensorReading map(Tuple3<Integer, ArrayList<Integer>, String> integerArrayListTuple3) throws Exception {
//            dataTitle.update(integerArrayListTuple2.f1);
            foobar = integerArrayListTuple3.f1;
            policy = integerArrayListTuple3.f0;

            String[] split = integerArrayListTuple3.f2.split(",");
            if (foobar.size() == 0){
                return null;
            }
            generalSensorReading generalSensorReading = new generalSensorReading();
            generalSensorReading.setPolicy(integerArrayListTuple3.f0);
            for (Integer index: foobar){
                generalSensorReading.addData(index, Double.valueOf(split[index]));
            }
            return generalSensorReading;
        }
    }
}
