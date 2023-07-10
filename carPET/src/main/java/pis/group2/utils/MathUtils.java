package pis.group2.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import static java.lang.Math.sqrt;

public class MathUtils {
    public static Double calculateDistance(Tuple2<Double, Double> p1, Tuple2<Double, Double> p2){
        Double x1 = p1.f0;
        Double y1 = p1.f1;
        Double x2 = p2.f0;
        Double y2 = p2.f1;
        return sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
    }


}
