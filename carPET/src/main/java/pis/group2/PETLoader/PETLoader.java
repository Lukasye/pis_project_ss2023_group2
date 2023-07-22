package pis.group2.PETLoader;

import org.apache.flink.api.java.tuple.Tuple2;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import pis.group2.beams.SerializableMethod;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public class PETLoader<T> implements Serializable{
    private String Home; // directory of the root
    private String Type;
    private Integer id;
    private String confPath;
    private String ConfPath; // directory of the Configuration file, input
    private String FileName; // The name of the package
    private String FunctionName; // The name of the PET methode
    private int size; // How many PET are there for this kind of data type
    private ArrayList<Class> classes;
    private JSONObject PETLibrary;

    private Class[] ClassList;
    private Class[] FunctionParameter;
    private ArrayList<Object> Default;
    private Object CurrentPolicy;
    private Method process;
    private Class PetClass;
    private SerializableMethod<T> PETMethod;
    private ArrayList<Integer> components;

    /**
     * @param confPath: The path of the file "petconfig.json"
     * @param Type: The type of the Input data for PET ("IMAGE", "SPEED", "LOCATION")
     * @param id: The ID of the PET that you want to apply.
     * @throws Exception
     */
    public PETLoader(String confPath, String Type, Integer id) throws Exception {
        this.confPath = confPath;
        this.Type = Type;
        this.id = id;
        initialize();
    }

    /**
     * Read the configuration file and load the information necessary for the instantiation.
     * @throws Exception
     */
    public void initialize() throws Exception {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader(confPath));
            // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
            JSONObject jsonObject = (JSONObject) obj;
            Home = (String) jsonObject.get("HOMEDIR");
            PETLibrary = (JSONObject) jsonObject.get(Type);
            size = PETLibrary.size();
            quickLoad();
        } catch (Exception e) {
            e.printStackTrace();
        }
        ConfPath = confPath;
        locateClass();
    }

    public void reloadPET(Integer newID) throws Exception {
        if (newID < 0 ){
            throw new IllegalArgumentException();
        } else if (newID >= size) {
            System.out.println("PET ID out of bound! Reloading PET Library");
            id = newID;
            initialize();
        }else {
            System.out.println(Type + " Policy switched to " + newID);
            id = newID;
            quickLoad();
            locateClass();
        }
        // Uncomment the following code to test the redeployment latency
//        id = newID;
//        initialize();
    }

    public void locateClass() throws Exception {
        classes = this.loadJarFile(FileName);
        int count = 0;
        for (Class c : classes){
            if (FunctionName.equals(c.getName())) break;
            count ++;
        }
        PetClass = classes.get(count);
    }

    /**
     * Instantiate the class, convert the datatype and pass the constructor, and reflect the PET Method,
     * finally wrap with a SerializableMethod.
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public void instantiate() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Object[] objects = new Object[ClassList.length];
        for (int i = 0; i < ClassList.length; i++) {
            Class aClass = ClassList[i];
            Object o = Default.get(i);
            if (aClass.isInstance(o)) {
                Object cast = aClass.cast(o);
                objects[i] = cast;
            } else {
                Constructor declaredConstructor = aClass.getConstructor(String.class);
                Object cast = declaredConstructor.newInstance(o.toString());
                objects[i] = cast;
            }
        }
        CurrentPolicy = PetClass.getConstructor(ClassList).newInstance(objects);
//        CurrentPolicy = PetClass.getConstructor(ClassList).newInstance(Default.toArray(new Object[Default.size()]));
        process = PetClass.getMethod("process", FunctionParameter);
        process.setAccessible(true);
        PETMethod = new SerializableMethod<T>(process, CurrentPolicy);
    }

    public void quickLoad() throws NoSuchFieldException, ClassNotFoundException, IllegalAccessException {
        JSONObject typeMethode = (JSONObject) PETLibrary.get(id.toString());
        FileName = "lib/" + (String) typeMethode.get("FileName");
        FunctionName = (String) typeMethode.get("FunctionName");
        ClassList = parseClassString((ArrayList<String>) typeMethode.get("ConstructorParameter"));
        FunctionParameter = parseClassString((ArrayList<String>) typeMethode.get("FunctionParameter"));
        Default = (ArrayList<Object>) typeMethode.get("Default");
        JSONArray component = (JSONArray) typeMethode.get("Component");
        components = new ArrayList<>();
        for (int i = 0; i < component.size(); i++) {
            Integer element = ((Long) component.get(i)).intValue();
            components.add(element);
        }
    }

    public ArrayList<Integer> getComponents(){
//        ArrayList<Integer> integers = new ArrayList<>();
//        for (String s: this.components){
//            integers.add(Integer.valueOf(s));
//        }
        return this.components;
    }

    public static Class[] parseClassString(ArrayList<String> InputList) throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        int length = InputList.size();
        Class[] TmpList = new Class[length];
        for (int i = 0; i < length; i ++) {
            Class TmpClass = (Class.forName(InputList.get(i)));
            TmpList[i] = TmpClass;
        }
        return TmpList;
    }

    public ArrayList<T> invoke(T input) throws InvocationTargetException, IllegalAccessException {
//        return (ArrayList<T>) process.invoke(CurrentPolicy, input);
        return PETMethod.invoke(input);
    }

    public void setId(Integer id) throws Exception {
        this.id = id;
        initialize();
    }

    public SerializableMethod<T> getPETMethod() {
        return PETMethod;
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

    public String getType() {
        return Type;
    }

    public void setType(String type) {
        Type = type;
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
        // For speed pet
        System.out.println("************ Speed PET Testing ************");
        PETLoader<Double> pl = new PETLoader<Double>("config/PETconfig.json", "SPEED", 0);
        System.out.println("Totally " + pl.getSize() + " PETs");
        System.out.println("Input: " + 20.3);
        for (int i = 0; i < pl.getSize(); i++){
            pl.setId(i);
            pl.instantiate();
            ArrayList<Double> result = pl.invoke(20.3);
            System.out.println("PET " + i + ": " + result);
        }


        // For image pet
        System.out.println("\n************ Image PET Testing ************");
        PETLoader<byte[]> pl_img = new PETLoader<>("config/PETconfig.json", "IMAGE", 0);
        System.out.println("Totally " + pl_img.getSize() + " PETs");

        InputStream imgStream = PETLoader.class.getClassLoader().getResourceAsStream("testImage/byteString");
        assert imgStream != null;
        byte[] testfileContent = imgStream.readAllBytes();
        System.out.println("Input: " + testfileContent);

        for (int i = 0; i < pl_img.getSize(); i++){
            pl_img.setId(i);
            pl_img.instantiate();
            ArrayList<byte[]> result_img = pl_img.invoke(testfileContent);
            System.out.println("PET " + i + ": " + result_img);

            OutputStream out = new FileOutputStream("src/main/resources/result/test"+i+".jpg");
            out.write(result_img.get(0));
            out.flush();
            out.close();
        }

        //For location pet
        System.out.println("\n************ Location PET Testing ************");
        PETLoader<Tuple2<Double, Double>> pl_loc = new PETLoader<>("config/PETconfig.json", "LOCATION", 0);
        System.out.println("Totally " + pl_loc.getSize() + " PETs");
        System.out.println("Input: " + new Tuple2<>(48.985771846331,8.3941997039792));
        for (int i = 0; i < pl_loc.getSize(); i++){
            pl_loc.setId(i);
            pl_loc.instantiate();
            ArrayList<Tuple2<Double, Double>> invoke = pl_loc.invoke(new Tuple2<>(48.985771846331,8.3941997039792));
            System.out.println("PET " + i + ": " + invoke);
        }
        System.out.println("\nTest Ended!");
    }
}
