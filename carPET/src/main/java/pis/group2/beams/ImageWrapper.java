package pis.group2.beams;

import java.util.Arrays;

public class ImageWrapper extends dataWrapper{
    private byte[] Image;
    private Double Timestamp;

    public ImageWrapper() {
        super();
    }

    public ImageWrapper(byte[] image) {
        super();
        Image = image;
    }

    public ImageWrapper(byte[] image, Double timestamp) {
        super();
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


    @Override
    public String toString() {
        return "ImageWrapper{" +
                "Image=" + (this.Image != null) +
                ", Timestamp=" + Timestamp +
                ", PETPolicy=" + PETPolicy +
                '}';
    }
}
