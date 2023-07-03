package pis.group2.beams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class generalSensorReading implements Serializable {
    private final HashMap<Integer, Double> data;
    private Integer Policy;

    public generalSensorReading() {
        data = new HashMap<>();
    }

    public void addData(Integer key, Double value){
        if (value == null || key == null) {System.out.println("Null value can't be accepted as addData parameter!"); return;}
        data.put(key, value);
    }

    public Double getData(Integer key){
        return data.get(key);
    }

    public Integer getPolicy() {
        return Policy;
    }

    public void setPolicy(Integer policy) {
        Policy = policy;
    }

    @Override
    public String toString() {
        return "generalSensorReading{" +
                "data=" + data +
                ", Policy=" + Policy +
                '}';
    }
}
