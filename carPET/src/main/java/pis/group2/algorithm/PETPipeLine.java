package pis.group2.algorithm;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

public abstract class PETPipeLine {
    protected final String confPath;
    protected String PETconfpath;
    protected ArrayList<String> PETType;
    protected StreamExecutionEnvironment env;

    public PETPipeLine(String confPath) throws Exception {
        this.confPath = confPath;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
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
        // TODO: implementation
        System.out.printf("loadPET");
    }

    abstract void buildPipeline();

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
