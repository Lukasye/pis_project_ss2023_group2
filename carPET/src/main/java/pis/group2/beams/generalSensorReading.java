package pis.group2.beams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class generalSensorReading implements Serializable {
    private final HashMap<Integer, Double> data;
    private Integer Policy;
    private Object result;
    protected ArrayList<Long> TimerRecord;

    public generalSensorReading() {
        data = new HashMap<>();
        TimerRecord = new ArrayList<>();
    }

    public void recordTimer(){
        TimerRecord.add(System.nanoTime());
    }

    public ArrayList<Long> getTimerRecord() {
        return TimerRecord;
    }

    public void addData(Integer key, Double value){
        if (value == null || key == null) {System.out.println("Null value can't be accepted as addData parameter!"); return;}
        data.put(key, value);
    }

//    public void setData(Integer key, Double value){
//        data.put()
//    }

    public Double getData(Integer key){
        return data.get(key);
    }

    public Integer getPolicy() {
        return Policy;
    }

    public void setPolicy(Integer policy) {
        Policy = policy;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "generalSensorReading{" +
                "data=" + data +
                ", Policy=" + Policy +
                ", result=" + result +
                '}';
    }
}
