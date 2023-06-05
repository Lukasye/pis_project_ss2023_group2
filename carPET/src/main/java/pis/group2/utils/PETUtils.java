package pis.group2.utils;

import org.apache.flink.api.common.functions.MapFunction;
import pis.group2.PETLoader.PETLoader;
import pis.group2.beams.SensorReading;

import java.io.Serializable;
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


    public static class applyPET implements MapFunction<SensorReading, SensorReading>{
        private PETLoader<Object> PET;
        public applyPET(PETLoader<Object> PETLoader) {
            PET = PETLoader;
        }

        @Override
        public SensorReading map(SensorReading sensorReading) throws Exception {
            String type = PET.getType();
            Double vel = sensorReading.getVel();
            ArrayList<Object> invoke = PET.invoke(vel);
            sensorReading.setVel((Double) invoke.get(0));
            return sensorReading;
        }
    }
}
