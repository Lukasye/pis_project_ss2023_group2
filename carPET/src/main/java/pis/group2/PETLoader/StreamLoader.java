package pis.group2.PETLoader;

import com.twitter.chill.java.ArraysAsListSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.SingleThreadEventExecutor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import pis.group2.utils.streamSchneider;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class StreamLoader implements Serializable {
    private ArrayList<Integer> indexList;
    private ArrayList<DataStream<Tuple2<Object, Class>>> streamList;

    public StreamLoader() {
    }

    public <T> void addStream(DataStream<T> newStream){
        streamList.add(newStream.map(new streamSchneider.convert2Tuple<>()));
    }
    
    public void removeStream(Integer index){
        streamList.remove(index);
    }

    public DataStream<Tuple2<Object, Class>> combineStreams(ArrayList<Integer> index) {
        return mergeStream(streamList, index);
    }

    public static DataStream<Tuple2<Object, Class>> mergeStream(ArrayList<DataStream<Tuple2<Object, Class>>> streamArray, ArrayList<Integer> index){
        if (index.size() == 0){
            System.out.println("No index specified!");
            return null;
        }
        DataStream<Tuple2<Object, Class>> outputStream = streamArray.get(index.get(0));
        for (int i = 1; i < index.size(); i++) {
            outputStream = outputStream.union(streamArray.get(index.get(i)));
        }
        return outputStream;
    }

    public static ArrayList<DataStream<Tuple2<Object, Class>>> generateStreams(SingleOutputStreamOperator<String> dataStream){
        ArrayList<DataStream<Tuple2<Object, Class>>> outputStream = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            int finalI = i;
            outputStream.add(dataStream.map(new MapFunction<String, String>() {
                @Override
                public String map(String s) throws Exception {
                    String[] split = s.split(",");
                    return split[finalI];
                }
            }).map(new streamSchneider.convert2Tuple<>()));
        }
        return outputStream;
    }

    public ArrayList<DataStream<Tuple2<Object, Class>>> getStreamList() {
        return streamList;
    }

    public void setStreamList(ArrayList<DataStream<Tuple2<Object, Class>>> streamList) {
        this.streamList = streamList;
    }
}
