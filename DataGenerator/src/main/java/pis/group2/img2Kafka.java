package pis.group2;

public class img2Kafka extends sth2Kafka<String> {
    private static final String TOPIC_NAME = "test-img";

    public img2Kafka(String BOOTSTRAP_SERVERS, String dataPath) {
        super(TOPIC_NAME, BOOTSTRAP_SERVERS, dataPath);
    }

    @Override
    protected void loadData() {

    }

    @Override
    protected void sendData() {

    }
}
