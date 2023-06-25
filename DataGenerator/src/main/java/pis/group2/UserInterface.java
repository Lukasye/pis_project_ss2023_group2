package pis.group2;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class UserInterface {
    private final String ConfPath;
    private String BOOTSTRAPSERVER;
    private String GPSData;
    private String DataType;
    private String ImgData;
    private final data2Kafka data2Kafka;
    private final usr2Kafka usr2Kafka;
    private final img2Kafka img2Kafka;
    public UserInterface(String ConfPath) throws IOException {
        this.ConfPath = ConfPath;
        loadConfig();
        data2Kafka = new data2Kafka(BOOTSTRAPSERVER, GPSData, DataType);
        img2Kafka = new img2Kafka(BOOTSTRAPSERVER, ImgData);
        usr2Kafka = new usr2Kafka(BOOTSTRAPSERVER);
    }

    private void loadConfig() {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader(ConfPath));
            JSONObject jsonObject = (JSONObject) obj;
            GPSData = (String) jsonObject.get("GPS-DATA");
            ImgData = (String) jsonObject.get("IMG-DATA");
            BOOTSTRAPSERVER = (String) jsonObject.get("BOOTSTRAPSERVER");
            DataType = (String) jsonObject.get("DATA-TYPE");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start() throws IOException, InterruptedException {
        Scanner scanner = new Scanner(System.in);
        String userInput;
        System.out.println("* * * * PET data generator * * * *");
        while (true){
            userInput = scanner.nextLine();
            // Deal with empty input
            if (userInput == null) continue;
            // quit program
            if (userInput.equalsIgnoreCase("exit")) {
                break;  // Exit the loop
            }
            // actual logic
            logic(userInput);
        }
    }

    public void logic(String userInput) throws IOException, InterruptedException {
        int counter;
        double timeout;
        String command;
        String[] elements = userInput.split(" ");
        switch (elements[0]){
            case "run":
                if (elements.length != 2) {
                    System.out.println("Invalid 'run' argument!");
                    break;
                }
                counter = Integer.parseInt(elements[1]);
                sendData(counter, 0.0);
                break;
            case "delayrun":
                if (elements.length != 3){
                    System.out.println("Invalid 'delayrun' argument!");
                }
                counter =Integer.parseInt(elements[1]);
                timeout = Double.parseDouble(elements[2]);
                sendData(counter, timeout);
                break;
            case "write":
                if (elements.length != 2) {
                    System.out.println("Invalid 'write' argument!");
                    break;
                }
                command = elements[1];
                usr2Kafka.sendCommand(command);
                System.out.println("New usr meg published!");
                break;
            case "script":
                if (elements.length != 2) {
                    System.out.println("Invalid 'write' argument!");
                    break;
                }
                command = elements[1];
                runScript("src/main/resources/scripts/" + command);
            case "reset":
                data2Kafka.reset();
                img2Kafka.reset();
            default:
                System.out.println("Not a valid command!");
        }
    }

    public void sendData(Integer counter, Double timeout) throws IOException, InterruptedException {
        for (int i = 0; i < counter; i++){
            data2Kafka.sendData();
            img2Kafka.sendData();
            TimeUnit.MILLISECONDS.sleep((long) (timeout * 100));
            System.out.println(" (" + i + "/" +  counter + ") GPS and IMG data published!");
        }
    }

    public void sendSingleData(Double timeout) throws IOException, InterruptedException {
        data2Kafka.sendData();
        img2Kafka.sendData();
        TimeUnit.MILLISECONDS.sleep((long) (timeout * 100));
    }

    public void runScript(String path) throws IOException {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader("src/main/resources/script"));
            String line = reader.readLine();
            while (line != null) {
                if (line.startsWith("#")){
                    line = reader.readLine();
                    continue;
                }
                System.out.println("Executing command: " + line);
                logic(line);
                // read next line
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        UserInterface ui = new UserInterface("src/main/resources/Dataconfig.json");
        ui.start();
    }
}
