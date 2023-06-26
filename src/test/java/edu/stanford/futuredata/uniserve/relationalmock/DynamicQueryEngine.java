package edu.stanford.futuredata.uniserve.relationalmock;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.QueryEngine;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class DynamicQueryEngine implements QueryEngine {
    List<RetrieveAndCombineQueryPlan<RMShard, List<RMRow>>> readPlans = new ArrayList<>();
    Broker broker;

    public DynamicQueryEngine(Broker broker){
        this.broker = broker;
    }

    public void fetchQueryPlans(String path) throws MalformedURLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        File jarFile = new File(path);
        URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{jarFile.toURI().toURL()});
        Class<?> loadedClass = classLoader.loadClass("org.example.Test");
        boolean implementsInterface = false;
        Class<?>[] interfaces = loadedClass.getInterfaces();
        for (Class<?> interfaceClass : interfaces) {
            if (interfaceClass.getName().equals("edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan")) {
                implementsInterface = true;
                break;
            }
        }
        if (implementsInterface) {
            // Create an instance of the loaded class
            RetrieveAndCombineQueryPlan<RMShard, List<RMRow>> instance =
                    (RetrieveAndCombineQueryPlan<RMShard, List<RMRow>>) loadedClass.newInstance();

            readPlans.add(instance);
            // Use the instance of the loaded class
        }
    }
    public void runReCQueryPlan(){
        if(!readPlans.isEmpty()){
            if(readPlans.get(0) != null) {
                List<RMRow> result = broker.retrieveAndCombineReadQuery(readPlans.get(0));
                readPlans.remove(0);
                System.out.println("\n\n\n\nHello there!");
            }
        }else{
            System.out.println("no read query plan has been fetched");
        }
    }
}
