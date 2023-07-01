package pis.group2.PETPipeLine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import pis.group2.PETLoader.StreamLoader;

import java.util.ArrayList;

public abstract class PETProcessor {
    private ArrayList<Integer> StreamIndex;
    private final StreamLoader StreamLoader;
    private Integer counter;
    private final String TYPE;

    public PETProcessor(String TYPE) {
        StreamLoader = new StreamLoader();
        this.TYPE = TYPE;
        counter = 0;
    }

    public void addStreams(DataStream... inputs){
        for (DataStream input: inputs){
            StreamLoader.addStream(input);
            counter ++;
        }
    }

    public void setStreams(ArrayList<DataStream<Tuple2<Object, Class>>> input){
        StreamLoader.setStreamList(input);
    }

    public DataStream<Tuple2<Object, Class>> loadStream(ArrayList<Integer> index){
        return StreamLoader.combineStreams(index);
    }

    public abstract void processLogic();
}
