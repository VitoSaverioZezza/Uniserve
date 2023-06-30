package edu.stanford.futuredata.uniserve.utilities;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class Utilities {
    public static String null_name = "__null__unready__";
    private static final Logger logger = LoggerFactory.getLogger(Utilities.class);

    public static Pair<String, Integer> parseConnectString(String connectString) {
        String[] hostPort = connectString.split(":");
        String host = hostPort[0];
        Integer port = Integer.parseInt(hostPort[1]);
        return new Pair<>(host, port);
    }

    public static ByteString objectToByteString(Serializable obj) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(obj);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Serialization Failed {} {}", obj, e);
            assert(false);
        }
        return ByteString.copyFrom(bos.toByteArray());
    }

    public static Object byteStringToObject(ByteString b){
        ByteArrayInputStream bis = new ByteArrayInputStream(b.toByteArray());
        Object obj = null;
        try {
            ObjectInput in = new ObjectInputStream(bis);
            obj = in.readObject();
            in.close();
        } catch (ClassNotFoundException classNotFoundException) {
            String className = classNotFoundException.getMessage();
            return deserializePlanMultipleJars(className, b);
        }catch (IOException e){
            e.printStackTrace();
        }
        return obj;
    }

    public static Object deserializePlan(String className, ByteString b){
        String jarPath = "/home/vsz/Scrivania/Jars/UniClient.jar";
        Object obj = null;
        try {
            URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{new File(jarPath).toURI().toURL()});
            Class<?> myInterfaceImplementationClass = classLoader.loadClass(className);
            ByteArrayInputStream bis = new ByteArrayInputStream(b.toByteArray());
            ObjectInputStream in = new ObjectInputStream(bis) {
                @Override
                protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                    return Class.forName(desc.getName(), false, classLoader);
                }
            };
            obj = in.readObject();
            in.close();
            return obj;
        }catch (MalformedURLException m){
            m.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static Object deserializePlanMultipleJars(String className, ByteString b){
        String jarPath = "/home/vsz/Scrivania/Jars";
        Object obj = null;

        File directory = new File(jarPath);
        File[] jarFiles = directory.listFiles((dir, name) -> name.endsWith(".jar"));
        if(jarFiles != null){
            for(File jarFile: jarFiles){
                try{
                    URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{jarFile.toURI().toURL()});
                    Class<?> myInterfaceImplementationClass = classLoader.loadClass(className);
                    ByteArrayInputStream bis = new ByteArrayInputStream(b.toByteArray());
                    ObjectInputStream in = new ObjectInputStream(bis) {
                        @Override
                        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                            return Class.forName(desc.getName(), false, classLoader);
                        }
                    };
                    obj = in.readObject();
                    in.close();
                    return obj;
                }catch (ClassNotFoundException e){
                    continue;
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }
}