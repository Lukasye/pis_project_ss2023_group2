package pis.group2.algorithm;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import pis.group2.PETLoader.PETLoader;
import org.json.simple.parser.JSONParser;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;

import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;

public class testPipeline {
    private final String confPath;
    private String PETconfpath;
    private ArrayList<String> PETType;
    private HashMap<String, PETLoader<Object>> PETLoaders;
    private StreamExecutionEnvironment env;

    public testPipeline(String confPath) throws Exception {
        this.confPath = confPath;
        loadConfig();
        loadPET();
    }

    public void loadConfig() throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(confPath));
        // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
        JSONObject jsonObject = (JSONObject) obj;
        PETconfpath = (String) jsonObject.get("PET-CONF");
        PETType = (ArrayList<String>) jsonObject.get("PET-TYPE");
    }

    public void loadPET() throws Exception {
        PETLoaders = new HashMap<String, PETLoader<Object>>();
        for (String type: PETType){
            PETLoader<Object> objectPETLoader = new PETLoader<>(PETconfpath, type, 0);
            objectPETLoader.instantiate();
            PETLoaders.put(type, objectPETLoader);
        }
    }

    public void buildPipeline(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Test Data
        String FilePath = "src/main/resources/PIS_data/gps_info.csv";
        DataStreamSource<String> inputStream = env.readTextFile(FilePath);

        SingleOutputStreamOperator<SensorReading> sensorStream = inputStream.map(new PETUtils.toSensorReading());

        SingleOutputStreamOperator<SensorReading> speedStream = sensorStream.map(new PETUtils.applyPET(PETLoaders.get("SPEED")));

        sensorStream.print();
    }

    public void execute() throws Exception {
        env.execute();
    }

    public void test() throws InvocationTargetException, IllegalAccessException {
        System.out.println(PETLoaders);
        PETLoader<Object> objectPETLoader = PETLoaders.get("SPEED");
        System.out.println(objectPETLoader.getHome());
        System.out.println(objectPETLoader.getType());
        ArrayList<Object> invoke = objectPETLoader.invoke(20.3);
        System.out.println(invoke.get(0));
    }


    public static void main(String[] args) throws Exception {
        testPipeline testPipeline = new testPipeline("config/Pipeconfig.json");
        testPipeline.test();
        testPipeline.buildPipeline();
        testPipeline.execute();
    }
}
