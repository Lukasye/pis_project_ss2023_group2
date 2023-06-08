# pis_project_ss2023_group2
Practical Information System Project

## Introduction
The target is to avaluate and build a streaming process to handle the driving data for a car. It needs to be dynamically changeeable and adapt user input. In this project we'll use Flink and NiFi to implement the task and find out which one is better in sense of performance and implementation/ deplotment effort. The main focus of the project should be:
1. Whether the framework is capable to load new PET policy from external sources.
2. The performance when PET is de/activated, changed or even introduced in runnung environment.<br>
<img src="./img/concept.png"  width="90%">
   

## Data
The data that we used in this project is part of the [KITTI dataset](https://www.cvlibs.net/datasets/kitti/). We took a single trace of the KITTI and applied a little reformulation to better fit out use case. The data we used contains the GPS, IMU and image information. For simplicity we only use one channel from the RGB camera data.<br>
<img src="./data/SITTI930/traj.png"  width="60%">
## Dynamic JAR package loading
One of the main challenge in this task is the dynamic loading of JAR package. (Since we are constructing Flink pipeline using JAVA) It will be extremly convinient if we can directly add or remove JAR package from an external library where the Streaming process can load new Policy from. So we've constructed a new `PETLoader` to read the configuration file and reflect the object and class in JAR package to the StreamMapFunctions.<br>
<img src="./img/PETLoader.png"  width="60%"><br>
The PET will read the configuration file `PETconfig.json` with the following format:
```json
  "LOCATION": {
    "0": {
      "FileName": "LocationPET01-1.0-SNAPSHOT.jar",
      "Description": "",
      "FunctionName":"pis.group2.LocationAnonymizer",
      "ConstructorParameter": ["java.lang.Integer",
        "java.lang.Integer",
        "java.lang.Double"],
      "Default": [3, 15, 10.0],
      "FunctionParameter": ["org.apache.flink.api.java.tuple.Tuple2"]
    }
```
To use the PETLoader class, you only need to create the object and call the `instantiate()` method, and then through the `invoke()` function you can easily get an Arraylist of returns.
```java
PETLoader<Tuple2<Double, Double>> pl_loc = new PETLoader<>("config/PETconfig.json", "LOCATION", 1);
        pl_loc.instantiate();
        ArrayList<Tuple2<Double, Double>> invoke = pl_loc.invoke(new Tuple2<>(48.985771846331,8.3941997039792));
        System.out.println(invoke);
```
## PET

## Evaluation