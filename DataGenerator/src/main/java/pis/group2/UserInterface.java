package pis.group2;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.Scanner;

public class UserInterface {
    private final String ConfPath;
    private String BOOTSTRAPSERVER;
    private String GPSData;
    private String ImgData;
    private sth2Kafka<String> data2Kafka;
    private sth2Kafka<String> usr2Kafka;
    private sth2Kafka<Byte[]> img2Kafka;
    public UserInterface(String ConfPath){
        this.ConfPath = ConfPath;
        loadConfig();
        // TODO: initialize also the img2Kafka and usr2Kafka
        data2Kafka = new data2Kafka(BOOTSTRAPSERVER, ConfPath);
    }

    private void loadConfig() {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader(ConfPath));
            JSONObject jsonObject = (JSONObject) obj;
            GPSData = (String) jsonObject.get("GPS-DATA");
            ImgData = (String) jsonObject.get("IMG-DATA");
            BOOTSTRAPSERVER = (String) jsonObject.get("BOOTSTRAPSERVER");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void start(){
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

    public void logic(String userInput){
        int counter = 0;
        String[] elements = userInput.split(" ");
        switch (elements[0]){
            case "run":
                data2Kafka.sendData();
                break;
            case "delayrun":
                // TODO: implement
                break;
            case "write":
                // TODO: implement
                break;
            default:
                System.out.println("Not a valid command!");
        }
    }

    public static void main(String[] args) {
        UserInterface ui = new UserInterface("src/main/resources/Dataconfig.json");
        ui.start();
    }
}
