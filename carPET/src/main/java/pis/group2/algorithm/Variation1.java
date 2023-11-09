package pis.group2.algorithm;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import pis.group2.PETPipeLine.PETPipeLine;
import pis.group2.beams.SensorReading;
import pis.group2.utils.PETUtils;
import scala.Tuple2;

public class Variation1 {
    /**
     * Variation one, which merge the streams together before going through the PET processing
     * additional duplicate filter needed
     * @param args: path for the configuration files
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Loading of essential parameters
        if (args.length != 1){
            System.out.println("Wrong Number of arguments!" + args.length + " Arguments can not be resolved!");
            return;
        }
        String path = args[0];
        new PETPipeLine(path) {
            @Override
            public void buildPipeline() {
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
//                resultStream.print("Result");
                resultStream.addSink(new PETUtils.SensorReadingToCSV(
                        "/Users/lukasye/Desktop/result/variation1.csv",
                        ","));
//                resultStream.print();
//                resultStream.addSink(new PETUtils.saveDataAsImage<>(ImageOutputPath, FILEEXTENSION));

            }
        };
    }
}
