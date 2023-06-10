package pis.group2.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import pis.group2.PETLoader.PETLoader;
import pis.group2.beams.SensorReading;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import pis.group2.beams.SerializableMethod;

import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import javax.imageio.ImageIO;
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
        @Override
        public SensorReading map(SensorReading sensorReading) throws Exception {
            count ++;
            sensorReading.setPETPolicy("LOCATION", 1);
            if (count > 20){
                sensorReading.setPETPolicy("SPEED", 1);
            }
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
        private Integer counter = 0;

        public saveDataAsImage(String path){
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

                String filePath = OutputPath + counter + ".png";

                ImageIO.write(image, "png", new File(filePath));

            } catch (IOException e) {
                System.out.println("Error writing image file: " + e.getMessage());
            }
        }
    }



}
