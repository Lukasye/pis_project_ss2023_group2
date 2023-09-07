package pis.group2.utils;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

public class CsvOutputFormat<T> implements OutputFormat<T> {
    /**
     * Class to define the output of the timestamp data into the local filse system
     */
    private final String filePath;

    private transient java.io.FileWriter writer;

    CsvOutputFormat(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void configure(Configuration configuration) {
        // No additional configuration required
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        writer = new java.io.FileWriter(filePath, true); // Append to the file if it already exists
    }

    @Override
    public void writeRecord(T record) throws IOException {
        writer.append(record.toString()).append('\n');
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    public void flush() throws IOException {
        if (writer != null){
            writer.flush();
        }
    }
}