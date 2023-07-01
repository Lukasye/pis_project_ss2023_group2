package pis.group2.PETPipeLine;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import pis.group2.GUI.SinkGUI;
import pis.group2.Jedis.DataFetcher;
import pis.group2.beams.SingleReading;
import pis.group2.utils.PETUtils;
import pis.group2.utils.streamSchneider;

import javax.sql.DataSource;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public abstract class PETPipeLine {
    protected final String confPath;
    protected String PETconfpath;
    protected String ImageOutputPath;
    protected String BOOTSTRAPSERVER;
    protected String GROUPID;
    protected String FILEEXTENSION;
    protected String IMAGETOPIC;
    protected String GPSTOPIC;
    protected String USERTOPIC;
    protected Properties kafkaProperty = new Properties();
    protected Properties kafkaPropertyImg = new Properties();
    protected ArrayList<String> PETType;
    protected StreamExecutionEnvironment env;
    protected SinkGUI GUI;
    protected SingleOutputStreamOperator<byte[]> imageSource;
    protected SingleOutputStreamOperator<String> dataSource;
    protected SingleOutputStreamOperator<String> userSource;
    protected Tuple3<String, String, String> RedisConfig;

    /**
     * create the Pipeline and initialisation, read the configurations
     * @param confPath: The path of the file "Pipeconfig.json"
     * @throws Exception
     */
    public PETPipeLine(String confPath) throws Exception {
        this.confPath = confPath;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.GUI = new SinkGUI(1);
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
        IMAGETOPIC = (String) jsonObject.get("KAFKA-IMAGETOPIC");
        GPSTOPIC = (String) jsonObject.get("KAFKA-GPSTOPIC");
        USERTOPIC = (String) jsonObject.get("KAFKA-USERTOPIC");
        String redisSer = (String) jsonObject.get("REDIS-SERVER");
        String redisPass = (String) jsonObject.get("REDIS-PASS");
        String awsJedis = (String) jsonObject.get("AWS-JEDIS");
        RedisConfig = new Tuple3<>(redisSer, redisPass, awsJedis);
    }

    /**
     * Implement the Streaming process with source and sink
     */
    public abstract void buildPipeline();


    /**
     * Don't forget to run this!
     * @throws Exception
     */
    public void execute() throws Exception {
        env.execute();
    }

    public void initKafka(){
        FlinkKafkaConsumer011<byte[]> kafkaSource = new FlinkKafkaConsumer011<>(
                IMAGETOPIC, new PETUtils.ReadByteAsStream(), kafkaPropertyImg);
        FlinkKafkaConsumer011<String> sensorDataConsumer = createStringConsumerForTopic(GPSTOPIC,
                BOOTSTRAPSERVER, GROUPID);
        FlinkKafkaConsumer011<String> userDataConsumer = createStringConsumerForTopic(USERTOPIC,
                BOOTSTRAPSERVER, GROUPID);
        imageSource = env.addSource(kafkaSource);
        dataSource = env.addSource(sensorDataConsumer);
        userSource = env.addSource(userDataConsumer);
    }

    public void initKafkaOnline(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "quiet-mammoth-10014-eu1-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cXVpZXQtbWFtbW90aC0xMDAxNCR5Q9SExiuD84R2x6tDInE2vRF9AdFtb1mH8_o\" password=\"1f1cc05b6f0a4e47828093bff177a4dc\";");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "$GROUP_NAME");
        FlinkKafkaConsumer011<String> sensorDataConsumer = new FlinkKafkaConsumer011<>(GPSTOPIC, new SimpleStringSchema(), props);
        dataSource = env.addSource(sensorDataConsumer);
        FlinkKafkaConsumer011<String> userConsumer = new FlinkKafkaConsumer011<>(USERTOPIC, new SimpleStringSchema(), props);
        imageSource = null;
        userSource = env.addSource(userConsumer);
    }

    public void initKafka(Long timeout){
        FlinkKafkaConsumer011<byte[]> kafkaSource = new FlinkKafkaConsumer011<>(
                IMAGETOPIC, new PETUtils.ReadByteAsStream(), kafkaPropertyImg);
        FlinkKafkaConsumer011<String> sensorDataConsumer = createStringConsumerForTopic(GPSTOPIC,
                BOOTSTRAPSERVER, GROUPID);
        FlinkKafkaConsumer011<String> userDataConsumer = createStringConsumerForTopic(USERTOPIC,
                BOOTSTRAPSERVER, GROUPID);
        imageSource = env.addSource(kafkaSource).setBufferTimeout(timeout);
        dataSource = env.addSource(sensorDataConsumer).setBufferTimeout(timeout);
        userSource = env.addSource(userDataConsumer).setBufferTimeout(timeout);
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

    public static HashMap<String, DataStream<SingleReading<?>>> SplitStringDataSource(DataStream<String> input, ArrayList<String> NameList){
        HashMap<String, DataStream<SingleReading<?>>> stringDataStreamHashMap = new HashMap<>();
        for (int i = 0; i < NameList.size(); i++) {
            int finalI = i;
            String name = NameList.get(i);
            stringDataStreamHashMap.put(name, input.map(new MapFunction<String, SingleReading<?>>() {
                @Override
                public SingleReading<?> map(String s) throws Exception {
                    String[] split = s.split(",");
                    return new SingleReading<>(split[finalI], name) ;
                }
            }));
        }
        return stringDataStreamHashMap;
    }

}
