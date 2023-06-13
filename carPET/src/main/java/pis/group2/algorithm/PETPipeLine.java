package pis.group2.algorithm;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import pis.group2.GUI.SinkGUI;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public abstract class PETPipeLine {
    protected final String confPath;
    protected String PETconfpath;
    protected String ImageOutputPath;
    protected String BOOTSTRAPSERVER;
    protected String GROUPID;
    protected String FILEEXTENSION;
    protected Properties kafkaProperty = new Properties();
    protected Properties kafkaPropertyImg = new Properties();
    protected ArrayList<String> PETType;
    protected StreamExecutionEnvironment env;
    protected SinkGUI GUI;

    /**
     * create the Pipeline and initialisation, read the configurations
     * @param confPath: The path of the file "Pipeconfig.json"
     * @throws Exception
     */
    public PETPipeLine(String confPath) throws Exception {
        this.confPath = confPath;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.GUI = new SinkGUI();
        loadConfig();
        this.buildPipeline();
        this.execute();
    }

    public void loadConfig() throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(confPath));
        // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
        JSONObject jsonObject = (JSONObject) obj;
        PETconfpath = (String) jsonObject.get("PET-CONF");
        PETType = (ArrayList<String>) jsonObject.get("PET-TYPE");
        ImageOutputPath = (String) jsonObject.get("IMAGE-OUTPUT-PATH");
        BOOTSTRAPSERVER = (String) jsonObject.get("BOOTSTRAP-SERVER");
        GROUPID = (String) jsonObject.get("GROUP-ID");
        kafkaProperty.setProperty("bootstrap.servers", BOOTSTRAPSERVER);
        kafkaProperty.setProperty("group.id", GROUPID);
        kafkaPropertyImg.setProperty("bootstrap.servers", BOOTSTRAPSERVER);
        kafkaPropertyImg.setProperty("group.id", GROUPID);
        kafkaPropertyImg.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        FILEEXTENSION = (String) jsonObject.get("FILE-EXTENSION");
    }

    /**
     * Implement the Streaming process with source and sink
     */
    abstract void buildPipeline();


    /**
     * Don't forget to run this!
     * @throws Exception
     */
    public void execute() throws Exception {
        env.execute();
    }


    public static FlinkKafkaConsumer011<String> createStringConsumerForTopic(
            String topic, String kafkaAddress, String kafkaGroup ) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id",kafkaGroup);
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(
                topic, new SimpleStringSchema(), props);

        return consumer;
    }


}
