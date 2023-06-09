package pis.group2.PETLoader;

import java.util.ArrayList;

/**
 * All the PET method that are going to install in the external library must
 * implement this interface. The input data type for the constructor might cause
 * a problem. In this case wrapper class like "java.lang.Double" is preferred to
 * primitive types.
 * @param <T> The Input datatype
 */
public interface PETProcess<T> {
    /**
     * Apply the PET
     * @param input: The data in the Stream
     * @return: An ArrayList of the initial input type of variable, as the output may related to the PET Method
     */
    public ArrayList<T> process(T input);

}
