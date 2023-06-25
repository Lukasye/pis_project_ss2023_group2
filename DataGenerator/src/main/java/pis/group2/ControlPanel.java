package pis.group2;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

public class ControlPanel extends JFrame {

    private JComboBox<Integer> menu1;
    private JComboBox<Integer> menu2;
    private JButton button1;
    private JButton button2;
    private JTextArea displayArea;
    private JTextArea editArea;
    private UserInterface ui;

    public ControlPanel() throws IOException {
        ui = new UserInterface("src/main/resources/Dataconfig.json");
        setTitle("Control Panel");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(400, 400);
        setLayout(new FlowLayout());

        // Create menus
        Integer[] numRun = {1, 5, 10, 20, 50, 100};
        menu1 = new JComboBox<>(numRun);
        Integer[] delayTime = {0, 1, 2, 3, 4, 5};
        menu2 = new JComboBox<>(delayTime);

        // Create buttons
        button1 = new JButton("Run");
        button2 = new JButton("Reset");

        // Create text display area
        displayArea = new JTextArea(10, 30);
        displayArea.setEditable(false); // Set as non-editable

        // Create text edit area
        editArea = new JTextArea(5, 30);

        // Add components to the frame
        add(menu1);
        add(menu2);
        add(button1);
        add(button2);
        add(new JScrollPane(displayArea));
        add(new JScrollPane(editArea));

        // Add event listeners
        button1.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                int selectedValue1 = (int) menu1.getSelectedItem();
                int selectedValue2 = (int) menu2.getSelectedItem();
//                int result = selectedValue1 + selectedValue2;
//                displayArea.append("Button 1 clicked. Sum: " + result + "\n");
            }
        });

        button2.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                int num = (int) menu1.getSelectedItem();
                int delay = (int) menu2.getSelectedItem();
                try {
                    for (int i = 0; i < num; i++) {
                        ui.sendSingleData((double) delay);
                        displayArea.append(" (" + i + "/" +  num + ") GPS and IMG data published!\n");
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                ControlPanel controlPanel = null;
                try {
                    controlPanel = new ControlPanel();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                controlPanel.setVisible(true);
            }
        });
    }
}