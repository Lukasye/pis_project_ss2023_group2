package pis.group2.utils;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import pis.group2.GUI.SinkGUI;
import pis.group2.Jedis.DataFetcher;
import pis.group2.Jedis.JedisTest;
import pis.group2.PETLoader.PETLoader;
import pis.group2.beams.ImageWrapper;
import pis.group2.beams.JedisWrapper;
import pis.group2.beams.SensorReading;
import pis.group2.beams.dataWrapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisFactory;
import redis.clients.jedis.JedisPool;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.ArrayList;
import java.util.Map;

public class PETUtils implements Serializable {


    /**
     * Determine the way flink accept kafka data as image byte array.
     */
    // Kafka deserializer function
    public static class ReadByteAsStream extends AbstractDeserializationSchema<byte[]>{

        @Override
        public byte[] deserialize(byte[] bytes) throws IOException {
            return bytes;
        }
    }

    // Transformation Functions

    /**
     * Map a gps raw stream direct into a SensorReading POJO without Image.
     */
    public static class toSensorReading implements MapFunction<String, SensorReading>{

        @Override
        public SensorReading map(String s) throws Exception {
            String[] fields = s.split(",");
            return new SensorReading(new Double(fields[0]),
                    new Double(fields[1]),
                    new Double(fields[2]),
                    new Double(fields[3]),
                    new Double(fields[12]),
                    new Double(fields[13]),
                    new Double(fields[9]));
        }
    }

    /**
     * Map a byte array into a ImageWrapper POJO without gps. used in Variation two.
     */
    public static class toImageWrapper implements MapFunction<byte[], ImageWrapper>{

        @Override
        public ImageWrapper map(byte[] bytes) throws Exception {
            return new ImageWrapper(bytes);
        }
    }

    public static class addImageToReading implements MapFunction<byte[], SensorReading>{

        @Override
        public SensorReading map(byte[] bytes) throws Exception {
            SensorReading tmp = new SensorReading();
            tmp.setImg(bytes);
            return tmp;
        }
    }

    /**
     * Methode to merge two Stream together in Variation 1, use the timestamp to extract the newest gps information
     * and the image, assemble them to create a SensorReading POJO.
     */
    public static class assembleSensorReading implements CoFlatMapFunction<byte[], String, SensorReading> {
        private String Data;
        private byte[] Image;

        @Override
        public void flatMap1(byte[] bytes, Collector<SensorReading> collector) throws Exception {
            this.Image = bytes;
            if (this.Data != null) {
                collector.collect(createSensorReadingFromRawInput(this.Data, bytes));
            }
        }

        @Override
        public void flatMap2(String s, Collector<SensorReading> collector) throws Exception {
            this.Data = s;
            if (this.Image != null) {
                collector.collect(createSensorReadingFromRawInput(s, this.Image));
            }

        }
    }

    /**
     * Outdated version for the previous function.
     */
    public static class ImageDataMerge extends KeyedCoProcessFunction<String, byte[], String, SensorReading> {
        private ValueState<byte[]> latestImage;
        private ValueState<String> latestData;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            latestImage = this.getRuntimeContext().getState(new ValueStateDescriptor<>("latestImage", byte[].class));
            latestData = this.getRuntimeContext().getState(new ValueStateDescriptor<>("latestData", String.class));
        }

        @Override
        public void processElement1(byte[] bytes, KeyedCoProcessFunction<String, byte[], String, SensorReading>.Context context, Collector<SensorReading> collector) throws Exception {
            latestImage.update(bytes);
            String value = latestData.value();
            if (value != null){
                collector.collect(createSensorReadingFromRawInput(value, bytes));
            }
        }

        @Override
        public void processElement2(String s, KeyedCoProcessFunction<String, byte[], String, SensorReading>.Context context, Collector<SensorReading> collector) throws Exception {
            latestData.update(s);
            byte[] img = latestImage.value();
            if (img != null){
                collector.collect(createSensorReadingFromRawInput(s, img));
            }

        }
    }

    public static class evaluationData implements MapFunction<SensorReading, SensorReading>{

        private int count = 0;
        private final Tuple2<Double, Double> UserHome = new Tuple2<>(48.98561, 8.39571);
        private final Double thredhold = 0.00135;

        @Override
        public SensorReading map(SensorReading sensorReading) throws Exception {
            count ++;
            // Location strategy, determine whether the car is near at home
            Double distance = MathUtils.calculateDistance(UserHome, sensorReading.getPosition());
            int locationPET = (distance < thredhold)? 1 : 0;
            sensorReading.setPETPolicy("LOCATION", locationPET);

            // Dummy speed evaluation
            if (count > 50){
                sensorReading.setPETPolicy("SPEED", 1);
            }

            // TODO: Image PET
            sensorReading.setPETPolicy("IMAGE", 1);

            return sensorReading;
        }
    }


    /**
     * Duplicate filter for Outcome from connected stream in variation 1, for the concurrence problem cause by
     * connect stream. The message might be duplicated. Use the Timestamp to determine whether this is the actual
     * information.
     */
    public static class duplicateCheck implements FilterFunction<SensorReading>{
        private Double currentTimeStamp = 0.0;

        @Override
        public boolean filter(SensorReading sensorReading) throws Exception {
            Double timestamp = sensorReading.getTimestamp();
            if (currentTimeStamp < timestamp) {
                currentTimeStamp = timestamp;
                return true;
            } else {
                return false;
            }
        }
    }

    public static class retrieveDataPolicy extends RichMapFunction<SensorReading, SensorReading>{
        private final Tuple3<String, String, String> RedisConfig;
//        private transient JedisPool jedisPool;
        private transient Jedis jedis;
        private Integer UserSpeedPolicy;
        private Integer UserLocationPolicy;
        private Integer UserCameraPolicy;
        private Integer SpeedSituation;
        private final Tuple2<Double, Double> UserHome = new Tuple2<>(48.98561, 8.39571);
        private final Double thredhold = 0.00135;

        public retrieveDataPolicy(Tuple3<String, String, String> dataFetcher) {
            super();
            this.RedisConfig = dataFetcher;
        }

        public void getPolicy(){
//            try (Jedis jedis = this.jedisPool.getResource()) {
                UserSpeedPolicy = Integer.valueOf(jedis.get("SpeedPET"));
                UserLocationPolicy = Integer.valueOf(jedis.get("LocationPET"));
                UserCameraPolicy = Integer.valueOf(jedis.get("CameraPET"));
                SpeedSituation = Integer.valueOf(jedis.get("SpeedSituation"));
//            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
//            this.jedisPool = DataFetcher.jedisPoolFactory(this.RedisConfig);
            jedis = new Jedis(this.RedisConfig.f2);
            this.getPolicy();
        }

        @Override
        public void close() throws Exception {
            super.close();
            this.jedis.close();
        }

        @Override
        public SensorReading map(SensorReading sensorReading) throws Exception {
//            try(Jedis jedis = this.jedisPool.getResource()) {
                // check the dirty bit, if the data is already modified, update the policy
                if (Integer.parseInt(jedis.get("dirty")) == 1){
                    this.getPolicy();
                    jedis.set("dirty", "0");
                }
                Double distance = MathUtils.calculateDistance(UserHome, sensorReading.getPosition());
                int locationPET = (distance < thredhold)? 1 : 0;
                sensorReading.setPETPolicy("LOCATION", locationPET);
                sensorReading.setPETPolicy("SPEED", SpeedSituation == 1? UserSpeedPolicy: 0);
                sensorReading.setPETPolicy("IMAGE", 0);
                return sensorReading;
//            }
        }
    }

    public static class retrieveImagePolicy extends RichMapFunction<ImageWrapper, ImageWrapper>{
        private final Tuple3<String, String, String> RedisConfig;
        //        private transient JedisPool jedisPool;
        private transient Jedis jedis;
        private Integer UserSpeedPolicy;
        private Integer UserLocationPolicy;
        private Integer UserCameraPolicy;
        private Integer SpeedSituation;
        private Integer CameraSituation;

        public retrieveImagePolicy(Tuple3<String, String, String> dataFetcher) {
            super();
            this.RedisConfig = dataFetcher;
        }

        public void getPolicy(){
//            try (Jedis jedis = this.jedisPool.getResource()) {
            UserSpeedPolicy = Integer.valueOf(jedis.get("SpeedPET"));
            UserLocationPolicy = Integer.valueOf(jedis.get("LocationPET"));
            UserCameraPolicy = Integer.valueOf(jedis.get("CameraPET"));
            SpeedSituation = Integer.valueOf(jedis.get("SpeedSituation"));
            CameraSituation = Integer.valueOf(jedis.get("CameraSituation"));
//            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
//            this.jedisPool = DataFetcher.jedisPoolFactory(this.RedisConfig);
            jedis = new Jedis(this.RedisConfig.f2);
            this.getPolicy();
        }

        @Override
        public void close() throws Exception {
            super.close();
            this.jedis.close();
        }

        @Override
        public ImageWrapper map(ImageWrapper sensorReading) throws Exception {
//            try(Jedis jedis = this.jedisPool.getResource()) {
            // check the dirty bit, if the data is already modified, update the policy
            if (Integer.parseInt(jedis.get("dirty")) == 1){
                this.getPolicy();
                jedis.set("dirty", "0");
            }
            sensorReading.setPETPolicy("LOCATION", 0);
            sensorReading.setPETPolicy("SPEED", SpeedSituation == 1? UserSpeedPolicy: 0);
            sensorReading.setPETPolicy("IMAGE", CameraSituation == 1? UserCameraPolicy: 0);
            return sensorReading;
//            }
        }
    }

    /**
     * Mapfunction for PET method processing, direct operate on datatype SensorReading and return the
     * same type of data.
     * @param <T> input data type
     */
    public static class applyPETForImage<T> extends RichMapFunction<ImageWrapper, ImageWrapper> {
        private PETLoader<T> PETLoader;
        private Integer id;
        private String confPath;
        private final String Type="IMAGE";

        /**
         * Constructor method
         * @param confPath: The configuration file, normally will be given in the conf file.
         */
        public applyPETForImage(String confPath) {
            this.confPath = confPath;
            this.id = 0;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // Load and initialise the PET method
            PETLoader = new  PETLoader<T>(confPath, Type, id);
            PETLoader.initialize();
//            ClassLoader userCodeClassLoader = getRuntimeContext().getUserCodeClassLoader();
////            userCodeClassLoader.loadClass()
        }


        public void reloadPET() throws Exception {
//            PETLoader = new  PETLoader<T>(confPath, Type, id);
            PETLoader.reloadPET(id);
            PETLoader.instantiate();
        }

        /**
         * Depends on the PET TYPE, process the data with the PETLoader
         * @param sensorReading: input from the stream
         * @return: output of the modified stream
         * @throws Exception
         */
        @Override
        public ImageWrapper map(ImageWrapper sensorReading) throws Exception {
//            String type = PET.getType();
            if (id != sensorReading.getPETPolicy().get(Type)) {
                id = sensorReading.getPETPolicy().get(Type);
                reloadPET();
            }
            byte[] invoke_img = (byte[]) PETLoader.invoke((T) sensorReading.getImage()).get(0);
            sensorReading.setImage(invoke_img);
            return sensorReading;
        }
    }


    /**
     * Mapfunction for PET method processing, direct operate on datatype SensorReading and return the
     * same type of data.
     * @param <T> input data type
     */
    public static class applyPET<T> extends RichMapFunction<SensorReading, SensorReading> {
        private PETLoader<T> PETLoader;
        private Integer id;
        private String confPath;
        private String Type;

        /**
         * Constructor method
         * @param confPath: The configuration file, normally will be given in the conf file.
         * @param Type: The PET data type ("IMAGE", "LOCATION", "SPEED")
         */
        public applyPET(String confPath, String Type) {
            this.confPath = confPath;
            this.Type = Type;
            this.id = 0;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // Load and initialise the PET method
            PETLoader = new  PETLoader<T>(confPath, Type, id);
            PETLoader.initialize();
//            ClassLoader userCodeClassLoader = getRuntimeContext().getUserCodeClassLoader();
////            userCodeClassLoader.loadClass()
        }


        public void reloadPET() throws Exception {
//            PETLoader = new  PETLoader<T>(confPath, Type, id);
            PETLoader.reloadPET(id);
            PETLoader.instantiate();
        }

        /**
         * Depends on the PET TYPE, process the data with the PETLoader
         * @param sensorReading: input from the stream
         * @return: output of the modified stream
         * @throws Exception
         */
        @Override
        public SensorReading map(SensorReading sensorReading) throws Exception {
//            String type = PET.getType();
            if (id != sensorReading.getPETPolicy().get(Type)) {
                id = sensorReading.getPETPolicy().get(Type);
                reloadPET();
            }
            switch (Type) {
                case "SPEED":
                    Double invoke_speed = (Double) PETLoader.invoke((T) sensorReading.getVel()).get(0);
                    sensorReading.setVel(invoke_speed);
                    break;
                case "LOCATION":
                    ArrayList<Tuple2<Double, Double>> invoke_pos = (ArrayList<Tuple2<Double, Double>>) PETLoader.invoke((T) sensorReading.getPosition());
                    sensorReading.setLocation(invoke_pos);
                    break;
                case "IMAGE":
                    byte[] invoke_img = (byte[]) PETLoader.invoke((T) sensorReading.getImg()).get(0);
                    sensorReading.setImg(invoke_img);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + Type);
            }
            return sensorReading;
        }
    }

    /**
     * Evaluation methode for Variation 1, Accept the SensorReading stream and the User Input stream, use coflatmap
     * to determine whether the situation is changed or not.
     */
    public static class evaluateSensorReading implements CoFlatMapFunction<SensorReading, String, SensorReading>{
        private Integer UserSpeedPolicy = 0;
        private Integer UserLocationPolicy = 0;
        private Integer UserCameraPolicy = 0;
        private Boolean SpeedSituation = false;
        private Boolean CameraSituation = false;
        private final Tuple2<Double, Double> UserHome = new Tuple2<>(48.98561, 8.39571);
        private final Double threshold = 0.00135;

        @Override
        public void flatMap1(SensorReading sensorReading, Collector<SensorReading> collector) throws Exception {
            // Location strategy, determine whether the car is near at home
            Double distance = MathUtils.calculateDistance(UserHome, sensorReading.getPosition());
            if (distance < threshold) sensorReading.setPETPolicy("LOCATION", UserLocationPolicy);
            if (CameraSituation) sensorReading.setPETPolicy("IMAGE", UserCameraPolicy);
            if (SpeedSituation) sensorReading.setPETPolicy("SPEED", UserSpeedPolicy);
            collector.collect(sensorReading);
        }

        @Override
        public void flatMap2(String s, Collector<SensorReading> collector) throws Exception {
            System.out.println("Current policy: \nLocation: " + UserLocationPolicy + " Camera: " + UserCameraPolicy + " Speed: " + UserSpeedPolicy);
            if (s.startsWith("change")){
                // change the user-specified PET policy
                String[] fields = s.split(",");
                UserLocationPolicy = Integer.valueOf(fields[1]);
                UserCameraPolicy = Integer.valueOf(fields[2]);
                UserSpeedPolicy = Integer.valueOf(fields[3]);
                System.out.println("Change policy setting!");
            } else if (s.startsWith("situation")) {
                String[] fields = s.split(",");
                switch (fields[1]){
                    case "speed":
                        SpeedSituation = !SpeedSituation;
                        System.out.println("Switch Speed environment!" + SpeedSituation);
                        break;
                    case "camera":
                        CameraSituation = !CameraSituation;
                        System.out.println("Switch camera environment!" + CameraSituation);
                        break;
                    default:
                        System.out.println("Not Valid 'situation' input!");
                }
            }else {
                System.out.println("Not valid 'user config input!");
            }
        }
    }

    public static class evaluation extends RichCoMapFunction<SensorReading, String, SensorReading> {
        private ValueState<Integer> UserSpeedPolicy;
        private ValueState<Integer> UserLocationPolicy;
        private ValueState<Integer> UserCameraPolicy;
        private ValueState<Boolean> SpeedSituation;
        private ValueState<Boolean> CameraSituation;
        private final Tuple2<Double, Double> UserHome = new Tuple2<>(48.98561, 8.39571);
        private final Double threshold = 0.00135;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            UserSpeedPolicy = this.getRuntimeContext().getState(new ValueStateDescriptor<>("UserSpeedPolicy", Integer.class));
            UserLocationPolicy = this.getRuntimeContext().getState(new ValueStateDescriptor<>("UserLocationPolicy", Integer.class));
            UserCameraPolicy = this.getRuntimeContext().getState(new ValueStateDescriptor<>("UserCameraPolicy", Integer.class));
            SpeedSituation = this.getRuntimeContext().getState(new ValueStateDescriptor<>("SpeedSituation", Boolean.class));
            CameraSituation = this.getRuntimeContext().getState(new ValueStateDescriptor<>("CameraSituation", Boolean.class));
            // Initialise default value
            UserCameraPolicy.update(0);
            UserLocationPolicy.update(0);
            UserSpeedPolicy.update(0);
            SpeedSituation.update(false);
            CameraSituation.update(false);
        }

        @Override
        public SensorReading map1(SensorReading sensorReading) throws Exception {
            // Location strategy, determine whether the car is near at home
            Double distance = MathUtils.calculateDistance(UserHome, sensorReading.getPosition());
            if (distance < threshold) sensorReading.setPETPolicy("LOCATION", UserLocationPolicy.value());
            if (CameraSituation.value()) sensorReading.setPETPolicy("IMAGE", UserCameraPolicy.value());
            if (SpeedSituation.value()) sensorReading.setPETPolicy("SPEED", UserSpeedPolicy.value());
            return sensorReading;
        }

        @Override
        public SensorReading map2(String s) throws Exception {
            if (s.startsWith("change")){
                // change the user-specified PET policy
                String[] fields = s.split(",");
                UserLocationPolicy.update(Integer.valueOf(fields[1]));
                UserCameraPolicy.update(Integer.valueOf(fields[2]));
                UserSpeedPolicy.update(Integer.valueOf(fields[3]));
                System.out.println("Change policy setting!");
            } else if (s.startsWith("situation")) {
                String[] fields = s.split(",");
                switch (fields[1]){
                    case "speed":
                        SpeedSituation.update(!SpeedSituation.value());
                        System.out.println("Switch Speed environment!");
                        break;
                    case "camera":
                        CameraSituation.update(!CameraSituation.value());
                        System.out.println("Switch camera environment!");
                    default:
                        System.out.println("Not Valid 'situation' input!");
                }
            }else {
                System.out.println("Not valid 'user config input!");
            }
            return null;
        }
    }

    public static class dataEvaluationRedis extends RichMapFunction<SensorReading, SensorReading>{
        private final Tuple3<String, String, String> RedisConfig;
        private transient Jedis jedis;
//        private transient JedisPool jedisPool;


        public dataEvaluationRedis(Tuple3<String, String, String> dataFetcher) {
            super();
            this.RedisConfig = dataFetcher;
        }
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
//            this.jedisPool = DataFetcher.jedisPoolFactory(this.RedisConfig);
            jedis = new Jedis(this.RedisConfig.f2);
        }

        @Override
        public void close() throws Exception {
            super.close();
            this.jedis.close();
//            this.jedisPool.destroy();
        }

        @Override
        public SensorReading map(SensorReading sensorReading) throws Exception {
            return null;
        }
    }
    // Sink Functions
    public static class sendDataToGUI implements SinkFunction<SensorReading>{
        private final SinkGUI GUI;

        public sendDataToGUI(SinkGUI GUI) {
            this.GUI = GUI;
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            SinkFunction.super.invoke(value, context);
            this.GUI.addGeneralInfo(String.valueOf(value.getTimestamp()));
            this.GUI.addLocationInfo(String.valueOf(value.getPositionAsString()));
            this.GUI.addSpeedInfo(String.valueOf(value.getVel()));
//            this.GUI.foolRefresh();
        }
    }

    public static void saveImage(String OutputPath, Integer counter, String DataType, SensorReading value){
        try (ByteArrayInputStream bais = new ByteArrayInputStream(value.getImg())) {
            System.out.println(bais);
            BufferedImage image = ImageIO.read(bais);
            String filePath = OutputPath + counter + "." + DataType;

            ImageIO.write(image, DataType, new File(filePath));

        } catch (IOException e) {
            System.out.println("Error writing image file: " + e.getMessage());
        }
    }

    public static class saveDataAsImage implements SinkFunction<ImageWrapper>{
        private final String OutputPath;
        private final String DataType;
        private Integer counter = 0;

        public saveDataAsImage(String path, String Datatype){
            this.DataType = Datatype;
            this.OutputPath = path;
        }

        @Override
        public void invoke(ImageWrapper value, Context context) throws Exception {
//            try(FileOutputStream fos = new FileOutputStream(
//                    OutputPath + counter + ".png")){
//                counter ++;
//                System.out.println(counter);
//                fos.write(value.getImg());
//            }

//            System.out.println(value.getImage());
            try (ByteArrayInputStream bais = new ByteArrayInputStream(value.getImage())) {
                System.out.println(bais);
                BufferedImage image = ImageIO.read(bais);
                counter ++;
                String filePath = OutputPath + counter + "." + DataType;
                System.out.println(filePath);

                ImageIO.write(image, DataType, new File(filePath));

            } catch (IOException e) {
                System.out.println("Error writing image file: " + e.getMessage());
            }
        }
    }

    public static class showInGUI implements SinkFunction<SensorReading>{
        private final SinkGUI GUI;

        public showInGUI(SinkGUI gui){
            this.GUI = gui;
        }
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            SwingUtilities.invokeAndWait(()->{
                this.GUI.addImageFromByteArray(value.getImg());
                this.GUI.addGeneralInfo(String.valueOf(value.getTimestamp()));
                this.GUI.addLocationInfo(String.valueOf(value.getPosition()));
                this.GUI.addSpeedInfo(String.valueOf(value.getVel()));
                this.GUI.foolRefresh();
            });
        }
    }

    /**
     * Save the gps data information from SensorReading object with the name of the timestamp
     */
    public static class saveDataAsText implements SinkFunction<SensorReading>{
        private final String OutputPath;

        public saveDataAsText(String path){
            this.OutputPath = path;
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            try (PrintWriter out = new PrintWriter(OutputPath + value.getTimestamp())) {
                out.println(value);
            }
        }
    }


    /**
     * Update the new Policy and situation changes to the redis server
     */
    public static class changeUserPolicyInRedis extends RichSinkFunction<String> {
        private final Tuple3<String, String, String> RedisConfig;
//        private transient JedisPool jedisPool;
        private transient Jedis jedis;
        private Integer UserSpeedPolicy;
        private Integer UserLocationPolicy;
        private Integer UserCameraPolicy;
        private Integer SpeedSituation;
        private Integer CameraSituation;

        public changeUserPolicyInRedis(Tuple3<String, String, String> dataFetcher) {
            super();
            this.RedisConfig = dataFetcher;
        }

        public void getPolicy(){
//            try (Jedis jedis = this.jedisPool.getResource()) {
                UserSpeedPolicy = Integer.valueOf(jedis.get("SpeedPET"));
                UserLocationPolicy = Integer.valueOf(jedis.get("LocationPET"));
                UserCameraPolicy = Integer.valueOf(jedis.get("CameraPET"));
                SpeedSituation = Integer.valueOf(jedis.get("SpeedSituation"));
                CameraSituation = Integer.valueOf(jedis.get("CameraSituation"));
//            }
        }

        public void setPolicy(){
//            try (Jedis jedis = this.jedisPool.getResource()) {
                jedis.set("SpeedPet", String.valueOf(UserSpeedPolicy));
                jedis.set("LocationPET", String.valueOf(UserLocationPolicy));
                jedis.set("CameraPET", String.valueOf(UserCameraPolicy));
                jedis.set("SpeedSituation", String.valueOf(SpeedSituation));
                jedis.set("CameraSituation", String.valueOf(CameraSituation));
                jedis.set("dirty", "1");
//            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
//            this.jedisPool = DataFetcher.jedisPoolFactory(this.RedisConfig);
            jedis = new Jedis(this.RedisConfig.f2);
            this.getPolicy();
        }

        @Override
        public void close() throws Exception {
            super.close();
            this.jedis.close();
//            this.jedisPool.destroy();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            super.invoke(value, context);
            if (value.startsWith("change")){
                // change the user-specified PET policy
                String[] fields = value.split(",");
                UserLocationPolicy = Integer.valueOf(fields[1]);
                UserCameraPolicy = Integer.valueOf(fields[2]);
                UserSpeedPolicy = Integer.valueOf(fields[3]);
                this.setPolicy();
                System.out.println("Change policy setting!");
                System.out.println("Current policy: " + UserLocationPolicy + UserSpeedPolicy + UserCameraPolicy);
            } else if (value.startsWith("situation")) {
                String[] fields = value.split(",");
                switch (fields[1]) {
                    case "speed":
                        SpeedSituation = SpeedSituation == 1? 0: 1;
                        this.setPolicy();
                        System.out.println("Switch Speed environment!" + SpeedSituation);
                        break;
                    case "camera":
                        CameraSituation = CameraSituation == 1? 0: 1;
                        this.setPolicy();
                        System.out.println("Switch camera environment!" + CameraSituation);
                        break;
                    default:
                        System.out.println("Not Valid 'situation' input!");
                }
            }
            else {
                System.out.println("Not valid 'user config input!");
            }
        }
    }

    public static class changeUserPolicyInRedisV2 implements RedisMapper<Tuple2<String, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return null;
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> stringIntegerTuple2) {
            return stringIntegerTuple2.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> stringIntegerTuple2) {
            return String.valueOf(stringIntegerTuple2.f1);
        }
    }
    // Helper Functions

    /**
     * Helper function to convert raw input data source into POJO
     * @param input The input stream from the gps information in raw String csv format
     * @param Image Byte array format of input image
     * @return SensorReading POJO
     */
    public static SensorReading createSensorReadingFromRawInput(String input, byte[] Image){
        String[] fields = input.split(",");
        return new SensorReading(new Double(fields[0]),
                new Double(fields[1]),
                new Double(fields[2]),
                new Double(fields[3]),
                new Double(fields[12]),
                new Double(fields[13]),
                new Double(fields[9]),
                Image);
    }

}
