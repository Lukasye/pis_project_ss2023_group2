package pis.group2.beams;

import java.util.ArrayList;
import java.util.HashMap;

public class dataWrapper {
    /**
     * Superclass for all the data POJO
     */
    protected HashMap<String, Integer> PETPolicy;
    protected ArrayList<Long> TimerRecord;
    protected byte[] Image;

    public dataWrapper() {
        PETPolicy = new HashMap<>();
        PETPolicy.put("SPEED", 0);
        PETPolicy.put("IMAGE", 0);
        PETPolicy.put("LOCATION", 0);
        TimerRecord = new ArrayList<>();
    }

    public void recordTimer(){
        TimerRecord.add(System.nanoTime());
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

    public byte[] getImage() {
        return Image;
    }

    public void setImage(byte[] image) {
        Image = image;
    }
}
