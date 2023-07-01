package pis.group2.PETPipeLine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import pis.group2.PETLoader.StreamLoader;
import pis.group2.beams.SingleReading;

import java.util.ArrayList;
import java.util.HashMap;

public abstract class PETProcessor {
    private ArrayList<Integer> StreamIndex;
    private final StreamLoader StreamLoader;
    private final String TYPE;

    public PETProcessor(String TYPE) {
        StreamLoader = new StreamLoader();
        this.TYPE = TYPE;
    }

    public void setStream(HashMap<String, DataStream<SingleReading<?>>> input){
        StreamLoader.setStreamMap(input);
    }

    public DataStream<SingleReading<?>> loadStream(ArrayList<String> names){
        return StreamLoader.combineStreams(names);
    }

    public void testPrint(){
        StreamLoader.testPrint();
    }

    public void testPrint(ArrayList<String> names){
        StreamLoader.testPrint(names);
    }

    public abstract void processLogic();
}
