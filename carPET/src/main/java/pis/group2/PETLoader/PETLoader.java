package pis.group2.PETLoader;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
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
    private String ConfPath; // directory of the Configuration file, input
    private String FileName; // The name of the package
    private String FunctionName; // The name of the PET methode
    private int size; // How many PET are there for this kind of data type
    private ArrayList<Class> classes;

    private Class[] ClassList;
    private Class[] FunctionParameter;
    private ArrayList<Object> Default;
    private Object CurrentPolicy;
    private Method process;
    private Class PetClass;

    public PETLoader(String confPath, String Type, Integer id) throws Exception {
        JSONParser parser = new JSONParser();
        this.Type = Type;
        try {
            Object obj = parser.parse(new FileReader(confPath));
            // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
            JSONObject jsonObject = (JSONObject) obj;
            Home = (String) jsonObject.get("HOMEDIR");
            JSONObject typeMethode = (JSONObject) jsonObject.get(Type);
            size = typeMethode.size();
            typeMethode = (JSONObject) typeMethode.get(id.toString());
            FileName = "lib/" + (String) typeMethode.get("FileName");
            FunctionName = (String) typeMethode.get("FunctionName");
            ClassList = parseClassString((ArrayList<String>) typeMethode.get("ConstructorParameter"));
            FunctionParameter = parseClassString((ArrayList<String>) typeMethode.get("FunctionParameter"));
            Default = (ArrayList<Object>) typeMethode.get("Default");
        } catch (Exception e) {
            e.printStackTrace();
        }
        ConfPath = confPath;
        classes = this.loadJarFile(FileName);
        int count = 0;
        for (Class c : classes){
            if (FunctionName.equals(c.getName())) break;
            count ++;
        }
        PetClass = classes.get(count);
    }

    public void instantiate() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        CurrentPolicy = PetClass.getConstructor(ClassList).newInstance(Default.toArray(new Object[Default.size()]));
        process = PetClass.getMethod("process", FunctionParameter);
        process.setAccessible(true);
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
        PETLoader<Double> pl = new PETLoader<Double>("config/PETconfig.json", "SPEED", 0);
        pl.instantiate();
        ArrayList<Double> result = pl.invoke(20.3);
        System.out.println(result);
        // For image pet
//        PETLoader<byte[]> pl_img = new PETLoader<>("config/PETconfig.json", "IMAGE", 0);
//        pl_img.instantiate();
//
//        InputStream imgStream = PETLoader.class.getClassLoader().getResourceAsStream("testImage/byteString");
//        assert imgStream != null;
//        byte[] testfileContent = imgStream.readAllBytes();
//
//        ArrayList<byte[]> result = pl_img.invoke(testfileContent);
//
//		OutputStream out = new FileOutputStream("src/main/resources/result/test.jpg");
//		out.write(result.get(0));
//		out.flush();
//		out.close();
    }
}
