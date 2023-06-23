package pis.group2.beams;

import java.util.HashMap;

public class dataWrapper {
    protected HashMap<String, Integer> PETPolicy;

    public dataWrapper() {
        PETPolicy = new HashMap<>();
        PETPolicy.put("SPEED", 0);
        PETPolicy.put("IMAGE", 0);
        PETPolicy.put("LOCATION", 0);
    }

    public HashMap<String, Integer> getPETPolicy() {
        return PETPolicy;
    }

    public void setPETPolicy(String key, Integer value) {
        this.PETPolicy.put(key, value);
    }
}
