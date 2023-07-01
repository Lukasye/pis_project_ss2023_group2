package pis.group2.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;


public class streamSchneider<T> implements Serializable {
    public static class convert2Tuple<T> implements MapFunction<T, Tuple2<Object, Class>>{

        @Override
        public Tuple2<Object, Class> map(T input) throws Exception {
            return new Tuple2<>(input, input.getClass());
        }
    }
}
