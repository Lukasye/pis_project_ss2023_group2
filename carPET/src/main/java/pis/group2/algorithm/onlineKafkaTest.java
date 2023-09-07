package pis.group2.algorithm;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import pis.group2.PETPipeLine.PETPipeLine;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;

public class onlineKafkaTest {
    /**
     * Test whether it is feasible to move the kafka serer online
     * @param args: path to configuration files
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1){
            System.out.println("Wrong Number of arguments!" + args.length + " Arguments can not be resolved!");
            return;
        }
        String path = args[0];
        new PETPipeLine(path) {
            @Override
            public void buildPipeline() {
                env.setParallelism(1);
                initKafkaOnline();

                SingleOutputStreamOperator<SensorReading> map = dataSource.map(new PETUtils.toSensorReading());
                map.print();

            }
        };
    }
}
