package pis.group2.algorithm;

import java.nio.channels.Pipe;

public class Variarion2 {
    public static void main(String[] args) throws Exception {
        new PETPipeLine("D:\\Projects\\pis_project_ss2023_group2\\carPET\\config\\Pipeconfig.json") {
            @Override
            void buildPipeline() {
                env.setParallelism(1);
                // Initialize data source
                initKafka();
                // Convert to POJO

            }
        };
    }
}
