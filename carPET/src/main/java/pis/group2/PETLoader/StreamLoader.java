package pis.group2.PETLoader;

import com.twitter.chill.java.ArraysAsListSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.SingleThreadEventExecutor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import pis.group2.beams.SingleReading;
import pis.group2.utils.streamSchneider;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class StreamLoader implements Serializable {
    private ArrayList<Integer> indexList;
    private HashMap<String, DataStream<SingleReading<?>>> streamMap;
    private DataStream<String> RawStream;

    public StreamLoader() {
    }

    public void addStream(String Name, DataStream<SingleReading<?>> newStream){
        streamMap.put(Name, newStream);
    }

    public HashMap<String, DataStream<SingleReading<?>>> getStreamMap() {
        return streamMap;
    }

    public DataStream<String> getRawStream() {
        return RawStream;
    }

    public void setRawStream(DataStream<String> rawStream) {
        RawStream = rawStream;
//        setStreamMap(SplitStringDataSource(rawStream, name));
    }

    public void setStreamMap(HashMap<String, DataStream<SingleReading<?>>> streamMap) {
        this.streamMap = streamMap;
    }

    public DataStream<SingleReading<?>> combineStreams(ArrayList<String> names) {
        if (names.size() == 0){
            System.out.println("No index specified!");
            return null;
        }
        DataStream<SingleReading<?>> singleReadingDataStream = streamMap.get(names.get(0));
        for (int i = 1; i < names.size(); i++) {
            singleReadingDataStream = singleReadingDataStream.union(streamMap.get(names.get(i)));
        }
        return singleReadingDataStream;
    }

    public void testPrint(){
        for (DataStream<SingleReading<?>> stream: streamMap.values()){
            stream.print();
        }
    }
    public void testPrint(ArrayList<String> names){
        for (String name: names){
            streamMap.get(name).print();
        }
    }

    public static HashMap<String, DataStream<SingleReading<?>>> SplitStringDataSource(DataStream<String> input, ArrayList<String> NameList){
        HashMap<String, DataStream<SingleReading<?>>> stringDataStreamHashMap = new HashMap<>();
        for (int i = 0; i < NameList.size(); i++) {
            int finalI = i;
            String name = NameList.get(i);
            stringDataStreamHashMap.put(name, input.map(new MapFunction<String, SingleReading<?>>() {
                @Override
                public SingleReading<?> map(String s) throws Exception {
                    String[] split = s.split(",");
                    return new SingleReading<>(split[finalI], name) ;
                }
            }));
        }
        return stringDataStreamHashMap;
    }
}
