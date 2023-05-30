package pis.group2.beams;

public class SensorReading {
    private double timestamp;
    private float latitude;
    private float longitude;
    private float altitude;
    private float acc_x;
    private float acc_y;
    private float vel;

    private byte[] img;

    public SensorReading() {
    }

    public SensorReading(double timestamp, float latitude, float longitude, float altitude, float acc_x, float acc_y, float vel) {
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
        this.acc_x = acc_x;
        this.acc_y = acc_y;
        this.vel = vel;
    }

    public double getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(double timestamp) {
        this.timestamp = timestamp;
    }

    public float getLatitude() {
        return latitude;
    }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }

    public float getAltitude() {
        return altitude;
    }

    public void setAltitude(float altitude) {
        this.altitude = altitude;
    }

    public float getAcc_x() {
        return acc_x;
    }

    public void setAcc_x(float acc_x) {
        this.acc_x = acc_x;
    }

    public float getAcc_y() {
        return acc_y;
    }

    public void setAcc_y(float acc_y) {
        this.acc_y = acc_y;
    }

    public float getVel() {
        return vel;
    }

    public void setVel(float vel) {
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
                "timestamp=" + timestamp +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", altitude=" + altitude +
                ", acc_x=" + acc_x +
                ", acc_y=" + acc_y +
                ", vel=" + vel +
                '}';
    }

    public static float calculateVelocity(){

        return 0.0F;
    }
}
