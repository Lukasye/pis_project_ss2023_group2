package pis.group2.beams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;

public class dataWrapper {
    protected HashMap<String, Integer> PETPolicy;
    protected ArrayList<Long> TimerRecord;

    public dataWrapper() {
        PETPolicy = new HashMap<>();
        PETPolicy.put("SPEED", 0);
        PETPolicy.put("IMAGE", 0);
        PETPolicy.put("LOCATION", 0);
        TimerRecord = new ArrayList<>();
    }

    public void recordTimer(){
        TimerRecord.add(System.currentTimeMillis());
    }

    public ArrayList<Long> getTimerRecord() {
        return TimerRecord;
    }

    public HashMap<String, Integer> getPETPolicy() {
        return PETPolicy;
    }

    public void setPETPolicy(String key, Integer value) {
        this.PETPolicy.put(key, value);
    }
}
