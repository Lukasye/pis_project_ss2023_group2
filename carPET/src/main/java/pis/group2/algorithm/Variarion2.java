package pis.group2.algorithm;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import pis.group2.PETPipeLine.PETPipeLine;
import pis.group2.beams.ImageWrapper;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;

public class Variarion2 {
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
                // Initialize data source
                initKafka();
                // Initialize JedisPool
                // Convert to POJO
                SingleOutputStreamOperator<ImageWrapper> ImageWrapperStream = imageSource.map(new PETUtils.toImageWrapper());
                SingleOutputStreamOperator<SensorReading> SensorReadingStream = dataSource.map(new PETUtils.toSensorReading());
                // Determine Evaluation
                // As the userSource only provide configurations, it goes directly into the sink
                userSource.addSink(new PETUtils.changeUserPolicyInRedis(RedisConfig));
                SingleOutputStreamOperator<SensorReading> evaluatedSensorReadingStream =
                        SensorReadingStream.map(new PETUtils.retrieveDataPolicy(RedisConfig));
                SingleOutputStreamOperator<ImageWrapper> evaluatedImageStream =
                        ImageWrapperStream.map(new PETUtils.retrieveImagePolicy(RedisConfig));

                // Apply PET
                SingleOutputStreamOperator<SensorReading> dataResultStream = evaluatedSensorReadingStream
                        .map(new PETUtils.applyPET<>(PETconfpath, "LOCATION"))
                        .map(new PETUtils.applyPET<>(PETconfpath, "SPEED"));
                SingleOutputStreamOperator<ImageWrapper> imageResultStream = evaluatedImageStream
                        .map(new PETUtils.applyPETForImage<>(PETconfpath));

                // Sink
                dataResultStream.print("Data");
                imageResultStream.addSink(new PETUtils.saveDataAsImage<ImageWrapper>(ImageOutputPath, "jpg"));

            }
        };
    }
}
