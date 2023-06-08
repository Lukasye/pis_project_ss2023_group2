package pis.group2;

import com.sun.jdi.PathSearchingVirtualMachine;
import pis.group2.PETLoader.PETLoader;

import java.util.ArrayList;

public class PETLoaderTest {
    public void speedTest() throws Exception {
        System.out.println("Speed PET Testing...");
        PETLoader<Double> pl = new PETLoader<Double>("config/PETconfig.json", "SPEED", 0);
        System.out.println("Totally " + pl.getSize() + "PETs");
        pl.instantiate();
        ArrayList<Double> result = pl.invoke(20.3);
        System.out.println(result);
    }
}
