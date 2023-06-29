package pis.group2.algorithm;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;
import scala.Tuple2;

public class Variation1 {
    public static void main(String[] args) throws Exception {
        if (args.length != 1){
            System.out.println("Wrong Number of arguments!" + args.length + " Arguments can not be resolved!");
            return;
        }
        String path = args[0];
        new PETPipeLine(path) {
            @Override
            void buildPipeline() {
                env.setParallelism(1);
                // Read Image from kafka topic
                initKafka(1000L);

                // Merge two Stream
                ConnectedStreams<byte[], String> connectedDataStream = imageSource.connect(dataSource);
                SingleOutputStreamOperator<SensorReading> SensorReadingStream =
                        connectedDataStream.flatMap(new PETUtils.assembleSensorReading());

                // Duplicate filter
                SingleOutputStreamOperator<SensorReading> filteredStream =
                        SensorReadingStream.filter(new PETUtils.duplicateCheck());

                // Evaluation
                SingleOutputStreamOperator<SensorReading> evaluatedStream =
                        filteredStream.connect(userSource).flatMap(new PETUtils.evaluateSensorReading());

                //Apply PET
                SingleOutputStreamOperator<SensorReading> resultStream = evaluatedStream.map(new PETUtils.applyPET<Double>(PETconfpath, "SPEED"))
                        .map(new PETUtils.applyPET<Tuple2<Double, Double>>(PETconfpath, "LOCATION"))
                        .map(new PETUtils.applyPET<byte[]>(PETconfpath, "IMAGE"));

                // Sink
//                resultStream.addSink(new PETUtils.showInGUI(GUI));
                resultStream.print("Result");
                resultStream.addSink(new PETUtils.SensorReadingToCSV(
                        "D:\\Projects\\pis_project_ss2023_group2\\carPET\\src\\main\\resources\\result\\timerecord.csv",
                        ","));
                resultStream.addSink(new PETUtils.saveDataAsImage<>(ImageOutputPath, FILEEXTENSION));

            }
        };
    }
}
