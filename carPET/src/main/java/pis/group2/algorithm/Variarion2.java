package pis.group2.algorithm;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import pis.group2.Jedis.DataFetcher;
import pis.group2.beams.ImageWrapper;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;
import redis.clients.jedis.Jedis;

import java.nio.channels.Pipe;

public class Variarion2 {
    public static void main(String[] args) throws Exception {
        new PETPipeLine("D:\\Projects\\pis_project_ss2023_group2\\carPET\\config\\Pipeconfig.json") {
            @Override
            void buildPipeline() {
                env.setParallelism(1);
                // Initialize data source
                initKafka();
                // Initialize JedisPool
                // Convert to POJO
                SingleOutputStreamOperator<ImageWrapper> ImageWrapperStream = imageSource.map(new PETUtils.toImageWrapper());
                SingleOutputStreamOperator<SensorReading> SensorReadingStream = dataSource.map(new PETUtils.toSensorReading());
                // Determine Evaluation
                // As the userSource only provide configurations, it goes directly into the sink
                userSource.addSink(new PETUtils.changeUserPolicyInRedis(RedisConfig));
                SingleOutputStreamOperator<SensorReading> evaluatedSensorReadingStream = SensorReadingStream.map(new PETUtils.retrieveDataPolicy(RedisConfig));

                // Apply PET
                SingleOutputStreamOperator<SensorReading> dataResultStream = evaluatedSensorReadingStream.map(new PETUtils.applyPET<>(PETconfpath, "LOCATION"));

                // Sink
                dataResultStream.print();

            }
        };
    }
}
