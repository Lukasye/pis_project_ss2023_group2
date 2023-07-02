package pis.group2.beams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class generalSensorReading implements Serializable {
    private HashMap<Integer, Double> data;
    private Integer Policy;

    public generalSensorReading() {

    }

    public void addData(Integer key, Double value){
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
