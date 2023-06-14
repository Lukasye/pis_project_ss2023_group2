package pis.group2.GUI;

import javax.swing.*;
import javax.swing.border.Border;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class SinkGUI extends JFrame implements ActionListener {
    private final JLabel Camera;
    private final JTextArea SpeedInfo;
    private final JTextArea LocationInfo;
    private final JTextArea GeneralInfo;

    public SinkGUI(){
        // Initialize
        this.setTitle("FlinkDataSink");
        this.setResizable(false);
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.setSize(1400, 620);
        this.setLayout(null);
        this.getContentPane().setBackground(new Color(0Xffffff));

        //create component
        Camera = new JLabel();
        Camera.setText("Camera");
        Camera.setHorizontalTextPosition(JLabel.CENTER);
        Camera.setVerticalTextPosition(JLabel.BOTTOM);
        Camera.setForeground(new Color(0x000000));
        Camera.setFont(new Font("Arial", Font.PLAIN, 20));
        Camera.setBounds(20, 0, 1200, 600);


        int FontSize = 25;
        String FontType = "Arial";
        Color background = new Color(0xbbbbbb);
        GeneralInfo = new JTextArea();
        GeneralInfo.setBackground(background);
        GeneralInfo.setBounds(1070, 30, 260, 200);
        GeneralInfo.setFont(new Font(FontType, Font.PLAIN, FontSize));
        GeneralInfo.setEditable(false);

        LocationInfo = new JTextArea();
        LocationInfo.setBackground(background);
        LocationInfo.setBounds(1070, 240, 260, 150);
        LocationInfo.setFont(new Font(FontType, Font.PLAIN, FontSize));
        LocationInfo.setEditable(false);

        SpeedInfo = new JTextArea();
        SpeedInfo.setBackground(background);
        SpeedInfo.setBounds(1070, 400, 260, 150);
        SpeedInfo.setFont(new Font(FontType, Font.PLAIN, FontSize));

        SpeedInfo.setEditable(false);

        this.add(Camera);
        this.add(GeneralInfo);
        this.add(LocationInfo);
        this.add(SpeedInfo);

//        this.setVisible(true);
    }

    public void addImage(String path){
        ImageIcon Image = new ImageIcon(path);
        Camera.setIcon(Image);
    }

    public void addImageFromByteArray(byte[] bytes){
        ImageIcon Image = new ImageIcon(bytes);
        Camera.setIcon(Image);
        foolRefresh();
    }

    public void foolRefresh(){
        this.setVisible(false);
        this.setVisible(true);
    }

    public void addGeneralInfo(String content){
        GeneralInfo.setText("General Info:\n" + content);
    }

    public void addLocationInfo(String content){
        LocationInfo.setText("Location:\n" + content);
    }

    public void addSpeedInfo(String content){
        SpeedInfo.setText("Speed:\n" + content);
    }
    @Override
    public void actionPerformed(ActionEvent e) {
    }

    public static void main(String[] args) {
        SinkGUI sinkGUI = new SinkGUI();
        sinkGUI.addImage("src/main/resources/gui/test0.jpg");
        sinkGUI.addGeneralInfo("asdfasdfasd");
        sinkGUI.addSpeedInfo("12312");
        sinkGUI.addLocationInfo("112312123");
        sinkGUI.setVisible(true);
    }

}
