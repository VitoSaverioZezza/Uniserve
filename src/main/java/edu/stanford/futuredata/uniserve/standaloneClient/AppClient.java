package edu.stanford.futuredata.uniserve.standaloneClient;

import edu.stanford.futuredata.uniserve.broker.Broker;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppClient {
    public static void main( String[] args ){
        checkIntegrity();
        String rootPath = args[1];
        String zkHost = args[2];
        int zkPort = Integer.parseInt(args[3]);
        int queryIndex = -1;
        if(args.length > 4 && args[4] != null){
            queryIndex = Integer.parseInt(args[4]);
        }
        Broker broker = new Broker(zkHost, zkPort);
        Map<Integer, List<Pair<String, Long>>> timings = new HashMap<>();

        System.out.println("Testing query "+args[0]+"_"+queryIndex+":\n\tzkHost: "+zkHost+
                "\n\tzkPort: "+zkPort+"\n\tdata path: "+rootPath);


        switch (args[0]){
            case "1":
                Query1 q1 = new Query1(broker, rootPath);
                q1.setQueryIndex(queryIndex);
                timings = q1.run();
                break;
            case "2":
                Query2 q2 = new Query2(broker, rootPath);
                q2.setQueryIndex(queryIndex);
                timings = q2.run();
                break;
            case "3":
                Query3 q3 = new Query3(broker, rootPath);
                q3.setQueryIndex(queryIndex);
                timings = q3.run();
                break;
            case "4":
                Query4 q4 = new Query4(broker, rootPath);
                q4.setQueryIndex(queryIndex);
                timings = q4.run();
                break;
            case "5":
                Query5 q5 = new Query5(broker, rootPath);
                q5.setQueryIndex(queryIndex);
                timings = q5.run();
                break;
            case "6":
                Query6 q6 = new Query6(broker, rootPath);
                q6.setQueryIndex(queryIndex);
                timings = q6.run();
                break;
            case "7":
                Query7 q7 = new Query7(broker, rootPath);
                q7.setQueryIndex(queryIndex);
                timings = q7.run();
                break;
            case "8":
                Query8 q8 = new Query8(broker, rootPath);
                q8.setQueryIndex(queryIndex);
                timings = q8.run();
                break;
            case "9":
                Query9 q9 = new Query9(broker, rootPath);
                q9.setQueryIndex(queryIndex);
                timings = q9.run();
                break;
            default:
                break;
        }
        for(Map.Entry<Integer, List<Pair<String, Long>>> run: timings.entrySet()){
            System.out.println("q1"+ run.getKey()+":");
            for(Pair<String, Long> times: run.getValue()){
                System.out.println("\t"+times.getValue0() + ": " + times.getValue1());
            }
        }
        broker.shutdownCluster();
    }

    private static void checkIntegrity(){
        for(int i = 0; i < TPC_DS_Inv.types.size(); i++) {
            if (TPC_DS_Inv.schemas.get(i).size() != TPC_DS_Inv.types.get(i).size()) {
                System.out.println("ERROR Schema " + i + " doesn't match size with schema types");
                System.out.println(TPC_DS_Inv.schemas.get(i).size() + " " + TPC_DS_Inv.types.get(i).size());
                return;
            }
        }
        if(
                TPC_DS_Inv.types.size() != TPC_DS_Inv.schemas.size() ||
                        TPC_DS_Inv.types.size() != TPC_DS_Inv.names.size() ||
                        TPC_DS_Inv.types.size() != TPC_DS_Inv.paths.size() ||
                        TPC_DS_Inv.types.size() != TPC_DS_Inv.keys.size() ||
                        TPC_DS_Inv.numberOfTables != TPC_DS_Inv.types.size()
        ){
            System.out.println("ERROR number of tables error");
        }
    }
}
