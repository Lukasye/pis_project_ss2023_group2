package pis.group2;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

public class ControlPanel extends JFrame {

    private JComboBox<Integer> menu1;
    private JComboBox<Integer> menu2;
    private JButton resetButton;
    private JButton runButton;
    private JButton writeButton;
    private JTextArea displayArea;
    private JTextArea editArea;
    private JCheckBox LocationSituation;
    private JCheckBox SpeedSituation;
    private JCheckBox CameraSituation;
    private UserInterface ui;

    public ControlPanel(String path) throws IOException {
        ui = new UserInterface(path);
        setTitle("Control Panel");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(400, 450);
        setLayout(new FlowLayout());

        // Create menus
        Integer[] numRun = {1, 5, 10, 20, 50, 100};
        menu1 = new JComboBox<>(numRun);
        Integer[] delayTime = {0, 1, 2, 3, 4, 5};
        menu2 = new JComboBox<>(delayTime);

        // Create buttons
        resetButton = new JButton("Reset");
        runButton = new JButton("Run");
        writeButton = new JButton("Write");

        // Create text display area
        displayArea = new JTextArea(10, 30);
        displayArea.setEditable(false); // Set as non-editable

        // Create text edit area
        editArea = new JTextArea(5, 30);

        // Create switches
        LocationSituation = new JCheckBox("Location");
        SpeedSituation = new JCheckBox("Speed");
        CameraSituation = new JCheckBox("Camera");

        // Add components to the frame
        add(menu1);
        add(menu2);
        add(resetButton);
        add(runButton);
        add(new JScrollPane(displayArea));
        add(new JScrollPane(editArea));
        add(LocationSituation);
        add(SpeedSituation);
        add(CameraSituation);
        add(writeButton);

        // Add event listeners
        resetButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ui.resetPointer();
                displayArea.append("Pointer reset to 1.\n");
            }
        });

        runButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                int num = (int) menu1.getSelectedItem();
                int delay = (int) menu2.getSelectedItem();
                try {
                    for (int i = 0; i < num; i++) {
                        ui.sendSingleData((double) delay);
                        displayArea.append(" (" + (i+1) + "/" +  num + ") GPS and IMG data published!\n");
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });

        LocationSituation.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                boolean selected = LocationSituation.isSelected();
//                ui.sendCommand("write situation,location");
                displayArea.append("Location: " + (selected ? "Activate" : "Deactivate") + "\n");
            }
        });

        SpeedSituation.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                boolean selected = SpeedSituation.isSelected();
                ui.sendCommand("situation,speed,"+ (selected? "1": "0"));
                displayArea.append("Speed: " + (selected ? "Activate" : "Deactivate") + "\n");
            }
        });

        CameraSituation.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                boolean selected = CameraSituation.isSelected();
                ui.sendCommand("situation,camera,"+ (selected? "1": "0"));
                displayArea.append("Camera: " + (selected ? "Activate" : "Deactivate") + "\n");
            }
        });

        writeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String text = editArea.getText();
                ui.sendCommand(text);
                displayArea.append("Write: " + text + "\n");
            }
        });
    }

    public static void main(String[] args) {
        if (args.length != 1){
            System.out.println("Wrong number of arguments!");
            return;
        }
        String path = args[0];
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                ControlPanel controlPanel = null;
                try {
                    controlPanel = new ControlPanel(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                controlPanel.setVisible(true);
            }
        });
    }
}