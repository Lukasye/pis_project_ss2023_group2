package pis.group2.beams;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

public class SerializableMethod<T> implements Serializable{
        private static final long serialVersionUID = 6631604036553063657L;
        private Method method;
        private Object object;

        public SerializableMethod(Method method, Object object)
        {
            this.method = method;
            this.object = object;
        }

        public Method getMethod()
        {
            return method;
        }

        public ArrayList<T> invoke(T input) throws InvocationTargetException, IllegalAccessException {
            return (ArrayList<T>) method.invoke(object, input);
//            return (ArrayList<T>) method.invoke(input);
        }

        private void writeObject(ObjectOutputStream out) throws IOException
        {
            out.writeObject(method.getDeclaringClass());
            out.writeUTF(method.getName());
            out.writeObject(method.getParameterTypes());
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
        {
            Class<?> declaringClass = (Class<?>)in.readObject();
            String methodName = in.readUTF();
            Class<?>[] parameterTypes = (Class<?>[])in.readObject();
            try
            {
                method = declaringClass.getMethod(methodName, parameterTypes);
            }
            catch (Exception e)
            {
                throw new IOException(String.format("Error occurred resolving deserialized method '%s.%s'", declaringClass.getSimpleName(), methodName), e);
            }
        }
    }
