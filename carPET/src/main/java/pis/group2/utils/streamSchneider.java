package pis.group2.utils;

import javassist.bytecode.stackmap.TypeData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import pis.group2.beams.SingleReading;

import java.io.Serializable;


public class streamSchneider<T> implements Serializable {
    /**
     * Preparation for the dynamic stream reload
     * @param <T> data type of the stream
     */

    public static class convert2Tuple<T> implements MapFunction<T, Tuple2<Object, Class>>{

        @Override
        public Tuple2<Object, Class> map(T input) throws Exception {
            return new Tuple2<>(input, input.getClass());
        }
    }

    public static class convert2SingleReading<T> implements MapFunction<T, SingleReading<T>>{
        private final String Name;

        public convert2SingleReading(String name) {
            Name = name;
        }

        @Override
        public SingleReading<T> map(T t) throws Exception {
            return new SingleReading<>(t, this.Name);
        }
    }
}
