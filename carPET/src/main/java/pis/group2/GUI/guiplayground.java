package pis.group2.GUI;

import pis.group2.PETLoader.PETLoader;
import privacyBA.evaluation.data.DataType;

import javax.imageio.ImageIO;
import javax.swing.*;
import javax.xml.crypto.Data;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class guiplayground {
    public static void main(String[] args) throws IOException {
        SinkGUI sinkGUI = new SinkGUI();
//        sinkGUI.addImage("D:\\Projects\\pis_project_ss2023_group2\\carPET\\src\\main\\resources\\result\\ImgOutput\\0.jpg");


        InputStream imgStream = PETLoader.class.getClassLoader().getResourceAsStream("testImage/byteString");
        assert imgStream != null;
        byte[] testfileContent = imgStream.readAllBytes();

        sinkGUI.addImageFromByteArray(testfileContent);

    }
}
