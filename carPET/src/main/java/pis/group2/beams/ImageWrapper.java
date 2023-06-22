package pis.group2.beams;

public class ImageWrapper {
    private byte[] Image;
    private Double Timestamp;

    public ImageWrapper() {
    }

    public ImageWrapper(byte[] image) {
        Image = image;
    }

    public ImageWrapper(byte[] image, Double timestamp) {
        Image = image;
        Timestamp = timestamp;
    }

    public byte[] getImage() {
        return Image;
    }

    public void setImage(byte[] image) {
        Image = image;
    }

    public Double getTimestamp() {
        return Timestamp;
    }

    public void setTimestamp(Double timestamp) {
        Timestamp = timestamp;
    }
}
