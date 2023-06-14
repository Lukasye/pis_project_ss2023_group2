package pis.group2.beams;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class SensorReading {
    private HashMap<String, Integer> PETPolicy;
    private ArrayList<Tuple2<Double, Double>> location;
    private Double timestamp;
    private Double altitude;
    private Double acc_x;
    private Double acc_y;
    private Double vel;

    private byte[] img;

    public SensorReading() {
        PETPolicy = new HashMap<>();
        PETPolicy.put("SPEED", 0);
        PETPolicy.put("IMAGE", 0);
        PETPolicy.put("LOCATION", 0);
    }

    public SensorReading(Double timestamp, Double latitude, Double longitude, Double altitude, Double acc_x, Double acc_y, Double vel) {
        PETPolicy = new HashMap<>();
        PETPolicy.put("SPEED", 0);
        PETPolicy.put("IMAGE", 0);
        PETPolicy.put("LOCATION", 0);
        this.timestamp = timestamp;
//        this.latitude = latitude;
//        this.longitude = longitude;
        this.location = new ArrayList<>(Arrays.asList(new Tuple2<>(latitude, longitude)));
        this.altitude = altitude;
        this.acc_x = acc_x;
        this.acc_y = acc_y;
        this.vel = vel;
    }

    public SensorReading(Double timestamp,
                         Double latitude,
                         Double longitude,
                         Double altitude,
                         Double acc_x,
                         Double acc_y,
                         Double vel,
                         byte[] Image) {
        PETPolicy = new HashMap<>();
        PETPolicy.put("SPEED", 0);
        PETPolicy.put("IMAGE", 0);
        PETPolicy.put("LOCATION", 0);
        this.timestamp = timestamp;
//        this.latitude = latitude;
//        this.longitude = longitude;
        this.location = new ArrayList<>(Arrays.asList(new Tuple2<>(latitude, longitude)));
        this.altitude = altitude;
        this.acc_x = acc_x;
        this.acc_y = acc_y;
        this.vel = vel;
        this.img = Image;
    }

    public HashMap<String, Integer> getPETPolicy() {
        return PETPolicy;
    }

    public void setPETPolicy(String key, Integer value) {
        this.PETPolicy.put(key, value);
    }

    public Tuple2<Double, Double> getPosition(){
        if (location == null){
            return null;
        }
        return location.get(0);
    }

    public String getPositionAsString(){
        if (location == null){
            return null;
        }
        String tmp = "";
        for (Tuple2<Double, Double> element : this.location) {
            tmp = tmp + element + '\n';
        }
        return tmp;
    }


    public void setLocation(ArrayList<Tuple2<Double, Double>> location) {
        this.location = location;
    }

    public double getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(double timestamp) {
        this.timestamp = timestamp;
    }

    public Double getLatitude() {
        return getPosition().f0;
    }

    public void setLatitude(Double latitude) {
        location.get(0).f0 = latitude;
    }

    public Double getLongitude() {
        return getPosition().f1;
    }

    public void setLongitude(Double longitude) {
        location.get(0).f1 = longitude;
    }

    public Double getAltitude() {
        return altitude;
    }

    public void setAltitude(Double altitude) {
        this.altitude = altitude;
    }

    public Double getAcc_x() {
        return acc_x;
    }

    public void setAcc_x(Double acc_x) {
        this.acc_x = acc_x;
    }

    public Double getAcc_y() {
        return acc_y;
    }

    public void setAcc_y(Double acc_y) {
        this.acc_y = acc_y;
    }

    public Double getVel() {
        return vel;
    }

    public void setVel(Double vel) {
        this.vel = vel;
    }

    public byte[] getImg() {
        return img;
    }

    public void setImg(byte[] img) {
        this.img = img;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "PETPolicy=" + PETPolicy + "\n"+
                ", location=" + location +
                ", timestamp=" + timestamp +
                ", vel=" + vel +
                ", Image=" + (this.img != null) +
                '}';
    }

    public static Double calculateVelocity(){

        return 0.0D;
    }
}
