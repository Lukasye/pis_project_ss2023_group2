package pis.group2.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import pis.group2.PETLoader.PETLoader;
import pis.group2.beams.SensorReading;
import pis.group2.beams.SerializableMethod;

import java.io.Serializable;
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


    public static class applyPET<T> extends RichMapFunction<SensorReading, SensorReading> {
        private PETLoader<T> PETLoader;
        private Integer id;
        private String confPath;
        private String Type;

        public applyPET(String confPath, String Type, Integer id) {
            this.confPath = confPath;
            this.Type = Type;
            this.id = id;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            reloadPET();
//            ClassLoader userCodeClassLoader = getRuntimeContext().getUserCodeClassLoader();
////            userCodeClassLoader.loadClass()
        }

        public void resetPET(String Type, Integer id) throws Exception {
            this.Type = Type;
            this.id = id;
            reloadPET();
        }

        public void reloadPET() throws Exception {
            PETLoader = new  PETLoader<T>(confPath, Type, id);
            PETLoader.instantiate();
        }

        @Override
        public SensorReading map(SensorReading sensorReading) throws Exception {
//            String type = PET.getType();
            Double vel = sensorReading.getVel();
            ArrayList<Object> invoke = (ArrayList<Object>) PETLoader.invoke((T) vel);
            sensorReading.setVel((Double) invoke.get(0));
            return sensorReading;
        }
    }
}
