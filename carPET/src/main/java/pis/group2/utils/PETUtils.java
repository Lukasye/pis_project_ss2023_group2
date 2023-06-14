package pis.group2.utils;

import org.apache.commons.math3.analysis.function.Sin;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import pis.group2.GUI.SinkGUI;
import pis.group2.PETLoader.PETLoader;
import pis.group2.beams.SensorReading;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import pis.group2.beams.SerializableMethod;

import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.image.BufferedImage;
import java.io.*;

import java.lang.reflect.Method;
import java.util.ArrayList;

public class PETUtils implements Serializable {

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

    public static class addImageToReading implements MapFunction<byte[], SensorReading>{

        @Override
        public SensorReading map(byte[] bytes) throws Exception {
            SensorReading tmp = new SensorReading();
            tmp.setImg(bytes);
            return tmp;
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
            } else if (s.startsWith("situation")) {
                String[] fields = s.split(",");
                switch (fields[1]){
                    case "speed":
                        SpeedSituation.update(!SpeedSituation.value());
                        break;
                    case "camera":
                        CameraSituation.update(!CameraSituation.value());
                    default:
                        System.out.println("Not Valid 'situation' input!");
                }
            }else {
                System.out.println("Not valid 'user config input!");
            }
            return null;
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

    public static class saveDataAsImage implements SinkFunction<SensorReading>{
        private final String OutputPath;
        private final String DataType;
        private Integer counter = 0;

        public saveDataAsImage(String path, String Datatype){
            this.DataType = Datatype;
            this.OutputPath = path;
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
//            try(FileOutputStream fos = new FileOutputStream(
//                    OutputPath + counter + ".png")){
//                counter ++;
//                System.out.println(counter);
//                fos.write(value.getImg());
//            }

            System.out.println(value.getImg());
            try (ByteArrayInputStream bais = new ByteArrayInputStream(value.getImg())) {
                System.out.println(bais);
                BufferedImage image = ImageIO.read(bais);
                counter ++;
                String filePath = OutputPath + counter + "." + DataType;

                ImageIO.write(image, DataType, new File(filePath));

            } catch (IOException e) {
                System.out.println("Error writing image file: " + e.getMessage());
            }
        }
    }


    public static class ReadByteAsStream extends AbstractDeserializationSchema<byte[]>{

        @Override
        public byte[] deserialize(byte[] bytes) throws IOException {
            return bytes;
        }
    }

    public static class showInGUI implements SinkFunction<SensorReading>{
        private final SinkGUI GUI;

        public showInGUI(SinkGUI gui){
            this.GUI = gui;
        }
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            if (value.getPosition() == null){

                SwingUtilities.invokeAndWait(()->{
                    GUI.addImageFromByteArray(value.getImg());
                });

            }
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


    public static class sendDataToGUI implements SinkFunction<SensorReading>{
        private final SinkGUI GUI;

        public sendDataToGUI(SinkGUI GUI) {
            this.GUI = GUI;
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            SinkFunction.super.invoke(value, context);
            this.GUI.addGeneralInfo(String.valueOf(value.getTimestamp()));
            this.GUI.addLocationInfo(String.valueOf(value.getPosition()));
            this.GUI.addSpeedInfo(String.valueOf(value.getVel()));
//            this.GUI.foolRefresh();
        }
    }

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
