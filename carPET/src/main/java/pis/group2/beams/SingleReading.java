package pis.group2.beams;

import java.io.Serializable;

public class SingleReading<T> implements Serializable {
    private T data;
    private String Name;
    private Class dataClass;

    public SingleReading(T data, String Name) {
        this.Name = Name;
        this.data = data;
        this.dataClass = data.getClass();
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public Class getDataClass() {
        return dataClass;
    }

    public void setDataClass(Class dataClass) {
        this.dataClass = dataClass;
    }

    @Override
    public String toString() {
        return "SingleReading{" +
                "data=" + data +
                ", Name='" + Name + '\'' +
                ", dataClass=" + dataClass +
                '}';
    }
}
