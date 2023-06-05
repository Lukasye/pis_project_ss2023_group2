package pis.group2.beams;

public class SensorReading {
    private Double timestamp;
    private Double latitude;
    private Double longitude;
    private Double altitude;
    private Double acc_x;
    private Double acc_y;
    private Double vel;

    private byte[] img;

    public SensorReading() {
    }

    public SensorReading(Double timestamp, Double latitude, Double longitude, Double altitude, Double acc_x, Double acc_y, Double vel) {
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

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
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
                "timestamp=" + timestamp +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", altitude=" + altitude +
                ", acc_x=" + acc_x +
                ", acc_y=" + acc_y +
                ", vel=" + vel +
                '}';
    }

    public static Double calculateVelocity(){

        return 0.0D;
    }
}
