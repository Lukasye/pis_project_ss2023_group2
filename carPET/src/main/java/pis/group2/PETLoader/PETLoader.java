package pis.group2.PETLoader;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public class PETLoader<T> {
    private String Home;
    private String ConfPath;
    private String FileName;
    private int size;
    private ArrayList<Class> classes;
    private Object CurrentPolicy;
    private Method process;

    public PETLoader(String confPath, String Type, String id) throws Exception {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader(confPath));
            // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
            JSONObject jsonObject = (JSONObject) obj;
            Home = (String) jsonObject.get("HOMEDIR");
            JSONObject typeMethode = (JSONObject) jsonObject.get(Type);
            typeMethode = (JSONObject) typeMethode.get(id);
            FileName = "lib/" + (String) typeMethode.get("FileName");
        } catch (Exception e) {
            e.printStackTrace();
        }
        ConfPath = confPath;
        System.out.println(FileName);
        classes = this.loadJarFile(FileName);
        int count = 0;
        for (Class c : classes){
            if ("pis.group2.SpeedAnonymizer".equals(c.getName())) break;
            count ++;
        }
        Class asd = classes.get(count);
        System.out.println(asd.getName());

    }

    public void instantiate(int ID) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class tmp_class = classes.get(0);
        CurrentPolicy = tmp_class.getConstructor(null).newInstance();
        process = tmp_class.getDeclaredMethod("action");
        process.setAccessible(true);
    }

    public void readConfig(){
        // TODO: To be implemented
    }

    public ArrayList<T> invoke(T input) throws InvocationTargetException, IllegalAccessException {
        return (ArrayList<T>) process.invoke(CurrentPolicy, input);
    }

    public String getHome() {
        return Home;
    }

    public void setHome(String home) {
        Home = home;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    // Returns an arraylist of class names in a JarInputStream
    private ArrayList<String> getClassNamesFromJar(JarInputStream jarFile) throws Exception {
        ArrayList<String> classNames = new ArrayList<>();
        try {
            //JarInputStream jarFile = new JarInputStream(jarFileStream);
            JarEntry jar;

            //Iterate through the contents of the jar file
            while (true) {
                jar = jarFile.getNextJarEntry();
                if (jar == null) {
                    break;
                }
                //Pick file that has the extension of .class
                if ((jar.getName().endsWith(".class"))) {
                    String className = jar.getName().replaceAll("/", "\\.");
                    String myClass = className.substring(0, className.lastIndexOf('.'));
                    classNames.add(myClass);
                }
            }
        } catch (Exception e) {
            throw new Exception("Error while getting class names from jar", e);
        }
        return classNames;
    }

    // Returns an arraylist of class names in a JarInputStream
    // Calls the above function by converting the jar path to a stream
    private ArrayList<String> getClassNamesFromJar(String jarPath) throws Exception {
        return getClassNamesFromJar(new JarInputStream(new FileInputStream(jarPath)));
    }

    // get an arraylist of all the loaded classes in a jar file
    private ArrayList<Class> loadJarFile(String filePath) throws Exception {

        ArrayList<Class> availableClasses = new ArrayList<>();

        ArrayList<String> classNames = getClassNamesFromJar(filePath);
        File f = new File(filePath);

        URLClassLoader classLoader = new URLClassLoader(new URL[]{f.toURI().toURL()});
        for (String className : classNames) {
            try {
                Class cc = classLoader.loadClass(className);
                availableClasses.add(cc);
            } catch (ClassNotFoundException e) {
                System.out.println("Class " + className + " was not found!");
            }
        }
        return availableClasses;
    }

    public static void main(String[] args) throws Exception {
        PETLoader<Double> pl = new PETLoader<Double>("config/config.json", "SPEED", "0");
    }
}
