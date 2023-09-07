package pis.group2.algorithm;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;

public class foobar {
    /**
     * Class for testing new function that learned in Flink
     * @param args: path to configuration files
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStream<Integer> stream2 = env.fromElements(6, 7, 8, 9, 10);

        SingleOutputStreamOperator<Integer> mappedStream = stream1.map(new UnionTriggeredMapper(stream2));

        mappedStream.print();

        env.execute("Dynamic Union Example");
    }

    public static class UnionTriggeredMapper implements MapFunction<Integer, Integer> {
        private transient DataStream<Integer> secondStream;
        private boolean triggerActivated = false;

        public UnionTriggeredMapper(DataStream<Integer> secondStream) {
            this.secondStream = secondStream;
        }

        @Override
        public Integer map(Integer value) throws Exception {
            if (!triggerActivated) {
                // Process the input value without union
                if (value == 3) {
                    // Trigger the union when a specific condition is met
                    triggerActivated = true;
                    return null; // Discard the element
                }
                return value;
            } else {
                // Union the input value with the second stream
//                secondStream = secondStream.union(stream1);
                return value;
            }
        }
    }
}