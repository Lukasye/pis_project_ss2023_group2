package pis.group2.PETPipeLine;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import pis.group2.PETLoader.StreamLoader;
import pis.group2.beams.SingleReading;
import pis.group2.beams.generalSensorReading;
import pis.group2.utils.PETUtils;
import org.json.simple.parser.JSONParser;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;

public abstract class PETProcessor implements Serializable {
    /**
     * Class to wrap PETLoader and contain a whole process for the single processing
     * of one topic, need to be instanciate and realize the function to determine
     * the evaluation condition
     */
    private ArrayList<Integer> StreamIndex;
    protected final StreamLoader StreamLoader;
//    private final PETLoader<?> PETLoader;
    private final String PETConfPath;
    private final String TYPE;
    private JSONObject PETLibrary;
    private Integer size;
    private HashMap<String, ArrayList<Integer>> PETComponent;

    public PETProcessor(String confPath, String TYPE) throws Exception {
        StreamLoader = new StreamLoader();
        this.TYPE = TYPE;
//        this.PETLoader = new PETLoader<>(confPath, TYPE, 0);
        this.PETConfPath = confPath;
        this.loadConf();
    }

    public PETProcessor(pis.group2.PETLoader.StreamLoader streamLoader, String PETConfPath, String TYPE) {
        StreamLoader = streamLoader;
        this.PETConfPath = PETConfPath;
        this.TYPE = TYPE;
    }

    private void loadConf() throws FileNotFoundException {
        PETComponent = new HashMap<>();
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader(PETConfPath));
            // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
            JSONObject jsonObject = (JSONObject) obj;
            PETLibrary = (JSONObject) jsonObject.get(TYPE);
            size = PETLibrary.size();
            for (Object index: PETLibrary.keySet()){
                ArrayList<Integer> integers = new ArrayList<>();
                JSONObject tmp = (JSONObject) PETLibrary.get(index);
                JSONArray element = (JSONArray) tmp.get("Component");
                for (int i = 0; i < element.size(); i++) {
                    String string = element.get(i).toString();
                    integers.add(Integer.valueOf(string));
                }
                PETComponent.put((String) index, integers);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setStream(HashMap<String, DataStream<SingleReading<?>>> input){
        StreamLoader.setStreamMap(input);
    }

    public DataStream<SingleReading<?>> loadStream(ArrayList<String> names){
        return StreamLoader.combineStreams(names);
    }

    public SingleOutputStreamOperator<generalSensorReading> run(DataStream<String> input){
        this.setRawStream(input);
        return this.processLogic();
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

    public SingleOutputStreamOperator<generalSensorReading> processLogic() {

        DataStream<Tuple2<Integer, String>> evaluatedRawStream = evaluation();
        SingleOutputStreamOperator<Tuple3<Integer, ArrayList<Integer>, String>> evaluatedComponentStream = evaluatedRawStream.map(new addComponents(PETComponent));

        SingleOutputStreamOperator<generalSensorReading> dataSource = evaluatedComponentStream.map(new selectData());
        SingleOutputStreamOperator<generalSensorReading> filteredDataSource = dataSource.filter(new FilterFunction<generalSensorReading>() {
            @Override
            public boolean filter(generalSensorReading generalSensorReading) throws Exception {
                if (generalSensorReading == null) return false;
                generalSensorReading.recordTimer();
                return true;
            }
        });
//        filteredDataSource.print();
        SingleOutputStreamOperator<generalSensorReading> resultStream = filteredDataSource.map(new PETUtils.applyPETForGeneralSensorReading<>(PETConfPath, TYPE));
//        resultStream.print(this.TYPE);
        return resultStream;
    }

//    public abstract DataStream<Tuple3<Integer, ArrayList<Integer>, String>> evaluation();
    public abstract DataStream<Tuple2<Integer, String>> evaluation();
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
            generalSensorReading.recordTimer();
            generalSensorReading.setPolicy(integerArrayListTuple3.f0);
            for (Integer index: foobar){
                generalSensorReading.addData(index, Double.valueOf(split[index]));
            }
            return generalSensorReading;
        }
    }

    public static class addComponents implements MapFunction<Tuple2<Integer, String>, Tuple3<Integer, ArrayList<Integer>, String>>{
        private HashMap<String, ArrayList<Integer>> component;

        public addComponents(HashMap<String, ArrayList<Integer>> component) {
            this.component= component;
        }

        @Override
        public Tuple3<Integer, ArrayList<Integer>, String> map(Tuple2<Integer, String> integerStringTuple2) throws Exception {
            String f0 = String.valueOf(integerStringTuple2.f0);
            ArrayList<Integer> integers = component.get(f0);
            return new Tuple3<>(integerStringTuple2.f0, integers, integerStringTuple2.f1);
        }
    }

    public static void main(String[] args) throws Exception {
        PETProcessor petProcessor = new PETProcessor("/Users/lukasye/Projects/pis_project_ss2023_group2/carPET/config/PETconfig.json", "SPEED") {

            @Override
            public DataStream<Tuple2<Integer, String>> evaluation() {
                return null;
            }
        };
    }


}
