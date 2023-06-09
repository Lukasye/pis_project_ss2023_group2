package pis.group2;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class usr2Kafka extends sth2Kafka<String> {
    private static final String TOPIC_NAME = "test-user-input";
    private List<String[]> csvData;

    public usr2Kafka(String BOOTSTRAP_SERVERS, String dataPath) {
        super(TOPIC_NAME, BOOTSTRAP_SERVERS, dataPath);
    }

    @Override
    protected void loadData() {

    }

    @Override
    protected void sendData() {

    }
}
