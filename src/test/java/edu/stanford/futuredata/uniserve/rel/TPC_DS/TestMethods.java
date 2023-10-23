package edu.stanford.futuredata.uniserve.rel.TPC_DS;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.DefaultAutoScaler;
import edu.stanford.futuredata.uniserve.coordinator.DefaultLoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.localcloud.LocalDataStoreCloud;
import edu.stanford.futuredata.uniserve.rel.RelTest;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.relational.RelShardFactory;
import edu.stanford.futuredata.uniserve.relationalapi.API;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.WriteQueryBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.javatuples.Pair;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static edu.stanford.futuredata.uniserve.localcloud.LocalDataStoreCloud.deleteDirectoryRecursion;

public class TestMethods {
    public static final Logger logger = Logger.getLogger(TestMethods.class);

    public static final String zkHost = "127.0.0.1";
    public static final Integer zkPort = 2181;

    public static void cleanUp(String zkHost, int zkPort) {
        // Clean up ZooKeeper
        String connectString = String.format("%s:%d", zkHost, zkPort);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework cf = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        cf.start();
        try {
            for (String child : cf.getChildren().forPath("/")) {
                if (!child.equals("zookeeper")) {
                    cf.delete().deletingChildrenIfNeeded().forPath("/" + child);
                }
            }
        } catch (Exception e) {
            logger.info("Zookeeper cleanup failed: {}");
        }finally {
            cf.close();
        }
        // Clean up directories.
        try {
            FileUtils.deleteDirectory(new File("/var/tmp/KVUniserve"));
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve"));
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve0"));
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve1"));
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve2"));
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve3"));
        } catch (IOException e) {
            logger.info("FS cleanup failed: {}");
        }
    }

    public static void a() throws IOException{
        Path LDSC = Path.of("src/main/LocalCloud/");
        if (Files.isDirectory(LDSC, LinkOption.NOFOLLOW_LINKS)) {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(LDSC)) {
                for (Path entry : entries) {
                    deleteDirectoryRecursion(entry);
                }
            }
        }
    }


    public static void startUpCleanUp() throws IOException {
        a();
        cleanUp(zkHost, zkPort);

    }

    public static void unitTestCleanUp() throws IOException {
        a();
        cleanUp(zkHost, zkPort);
    }


    public static void printRowList(List<RelRow> data) {
        for (RelRow row : data) {
            StringBuilder rowBuilder = new StringBuilder();
            rowBuilder.append("Row #" + data.indexOf(row) + " ");
            for (int j = 0; j < row.getSize() - 1; j++) {
                rowBuilder.append(row.getField(j) + ", ");
            }
            rowBuilder.append(row.getField(row.getSize() - 1));
            System.out.println(rowBuilder);
        }
    }

    public static final int numServers = 10;
    public Coordinator coordinator = null;
    public List<LocalDataStoreCloud> ldscList = new ArrayList<>();
    public List<DataStore<RelRow, RelShard>> dataStores = new ArrayList<>();
    public static final String rootPath = "C:\\Users\\saver\\Desktop\\db00_0625_clean";

    public void startServers(){
        coordinator = new Coordinator(
                null,
                new DefaultLoadBalancer(),
                new DefaultAutoScaler(),
                zkHost, zkPort,
                "127.0.0.1", 7777);
        coordinator.runLoadBalancerDaemon = false;
        coordinator.startServing();

        ldscList = new ArrayList<>();
        dataStores = new ArrayList<>();
        for(int i = 0; i<numServers; i++) {
            ldscList.add(new LocalDataStoreCloud());
            DataStore<RelRow, RelShard> dataStore = new DataStore<>(ldscList.get(i),
                    new RelShardFactory(),
                    Path.of("/var/tmp/RelUniserve"),
                    zkHost, zkPort,
                    "127.0.0.1", 8000 + i,
                    -1,
                    false
            );
            dataStore.startServing();
            dataStores.add(dataStore);
        }
    }
    public void stopServers(){
        coordinator.initiateShutdown();
        for(DataStore<RelRow, RelShard> dataStore:dataStores) {
            dataStore.shutDown();
        }
        coordinator.stopServing();
        try {
            for(LocalDataStoreCloud ldsc: ldscList) {
                ldsc.clear();
            }
        } catch (Exception e) {
            ;
        }
    }

    public void printOnFile(List<RelRow> rowList, List<String> schema, String fileName) throws IOException {
        System.out.println("Writing output...");
        String sinkPath = "C:\\Users\\saver\\Desktop\\debRes\\"+fileName+".csv";
        BufferedWriter datWriter = new BufferedWriter(new FileWriter(sinkPath));

        StringBuilder schemaBuilder = new StringBuilder();
        for(int i = 0; i<schema.size()-1; i++){
            schemaBuilder.append(schema.get(i));
            schemaBuilder.append("^");
        }
        schemaBuilder.append(schema.get(schema.size()-1));
        String schemaLine = schemaBuilder.toString();
        datWriter.write(schemaLine);
        datWriter.newLine();

        for(RelRow resRow: rowList){
            StringBuilder builder = new StringBuilder();
            for(int i = 0; i< resRow.getSize()-1; i++){
                builder.append(resRow.getField(i));
                builder.append("^");
            }
            builder.append(resRow.getField(resRow.getSize()-1));
            String line = builder.toString();
            datWriter.write(line);
            datWriter.newLine();
        }
        datWriter.close();
        System.out.println("Output written to " + sinkPath);
    }

    public void saveTimings(List<Pair<String, Long>> writeTimes, Long readTime, String filaname) throws IOException {
        System.out.println("Writing times...");
        String sinkPath = "C:\\Users\\saver\\Desktop\\Timings\\times"+filaname+".csv";

        File f = new File(sinkPath);
        if(!f.exists() && !f.isDirectory()) {
            BufferedWriter create = new BufferedWriter(new FileWriter(sinkPath));
            StringBuilder builder = new StringBuilder();
            for(Pair<String, Long> wrtieTimes: writeTimes){
                builder.append(wrtieTimes.getValue0());
                builder.append(";");
            }
            builder.append("ReadTime");
            create.write(builder.toString());
            create.newLine();
            create.close();
        }
        BufferedWriter datWriter = new BufferedWriter(new FileWriter(sinkPath, true));

        StringBuilder builder = new StringBuilder();
        for(Pair<String, Long> writeTime: writeTimes){
            builder.append(writeTime.getValue1());
            builder.append(";");
        }
        builder.append(readTime);
        String line = builder.toString();
        datWriter.write(line);
        datWriter.newLine();
        datWriter.close();
        System.out.println("Times written to " + sinkPath);
    }


    public List<Pair<String,Long>> loadDataInMem(Broker broker, List<String> tablesToLoad) {
        API api = new API(broker);
        boolean res = true;
        List<Pair<String, Long>> times = new ArrayList<>();

        List<WriteQueryBuilder> batch = new ArrayList<>();

        for (int i = 0; i < TPC_DS_Inv.numberOfTables; i++) {
            if(!tablesToLoad.contains(TPC_DS_Inv.names.get(i))){
                continue;
            }else{
                System.out.println("Loading data for table " + TPC_DS_Inv.names.get(i));
            }
            List<RelRow> memBuffer = new ArrayList<>();
            MemoryLoader memoryLoader = new MemoryLoader(i, memBuffer);
            if(!memoryLoader.run()){
                return null;
            }
            int shardNum = Math.min(Math.max(memBuffer.size()/1000, 1), Broker.SHARDS_PER_TABLE);
            res = api.createTable(TPC_DS_Inv.names.get(i))
                    .attributes(TPC_DS_Inv.schemas.get(i).toArray(new String[0]))
                    .keys(TPC_DS_Inv.schemas.get(i).toArray(new String[0]))
                    .shardNumber(shardNum)
                    .build().run();

            //long startTime = System.currentTimeMillis();
            //res = api.write().table(TPC_DS_Inv.names.get(i)).data(memBuffer.toArray(new RelRow[0])).build().run();
            //long elapsedTime = System.currentTimeMillis() - startTime;
            //times.add(new Pair<>(TPC_DS_Inv.names.get(i), elapsedTime));
            //memBuffer.clear();

            batch.add(api.write().table(TPC_DS_Inv.names.get(i)).data(memBuffer.toArray(new RelRow[0])).build());

            //if(!res){
            //    broker.shutdown();
            //    throw new RuntimeException("Write error for table "+TPC_DS_Inv.names.get(i));
            //}
        }
        Pair<Boolean, List<Pair<String,Long>>> results = api.writeBatch().setPlans(batch.toArray(new WriteQueryBuilder[0])).run();
        return results.getValue1();
    }

    private class MemoryLoader{
        private final int index;
        private final List<RelRow> sink;
        public MemoryLoader(int index, List<RelRow> sink){
            this.sink = sink;
            this.index = index;
        }
        public boolean run(){
            String path = TestMethods.rootPath + TPC_DS_Inv.paths.get(index);
            List<Integer> types = TPC_DS_Inv.types.get(index);

            int rowCount = 0;

            try (BufferedReader br = new BufferedReader(new FileReader(path))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if(rowCount % 10000 == 0){
                        //System.out.println("Row count: " + rowCount);
                    }
                    String[] parts = line.split("\\|");
                    List<Object> parsedRow = new ArrayList<>(types.size());
                    for(int i = 0; i<types.size(); i++){
                        fieldIndex = i;
                        if(i < parts.length){
                            parsedRow.add(parseField(parts[i], types.get(i)));
                        }else{
                            parsedRow.add(parseField("", types.get(i)));
                        }
                    }
                    if(parsedRow.size() == TPC_DS_Inv.schemas.get(index).size()){
                        sink.add(new RelRow(parsedRow.toArray()));
                        rowCount++;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }


        int fieldIndex =0;

        private Object parseField(String rawField, Integer type) throws IOException {
            if(rawField.isEmpty()){
                if(type.equals(TPC_DS_Inv.id__t)){
                    return -1;
                } else if (type.equals(TPC_DS_Inv.string__t)) {
                    return "";
                } else if (type.equals(TPC_DS_Inv.int__t)) {
                    return 0;
                } else if (type.equals(TPC_DS_Inv.dec__t)) {
                    return 0D;
                } else if (type.equals(TPC_DS_Inv.date__t)) {
                    try{
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        return simpleDateFormat.parse("0000-00-00");
                    }catch (ParseException e){
                        throw new IOException("null date is not a valid date so it seems");
                    }
                }else {
                    throw new IOException("Unsupported type definition");
                }
            }
            if(type.equals(TPC_DS_Inv.id__t)){
                return Integer.valueOf(rawField);
            } else if (type.equals(TPC_DS_Inv.string__t)) {
                return rawField;
            } else if (type.equals(TPC_DS_Inv.int__t)) {
                try {
                    return Integer.valueOf(rawField);
                }catch (NumberFormatException e){
                    throw new IOException("Raw field cannot be parsed as an Integer.\n" + e.getMessage());
                }
            } else if (type.equals(TPC_DS_Inv.dec__t)) {
                try {
                    return Double.valueOf(rawField);
                }catch (NumberFormatException e){
                    throw new IOException("Raw field cannot be parsed as a Double.\n" + e.getMessage());
                }
            } else if (type.equals(TPC_DS_Inv.date__t)) {
                try{
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    return simpleDateFormat.parse(rawField);
                }catch (ParseException e){
                    throw new IOException("Raw field cannot be parsed as Date.\n" + e.getMessage());
                }
            }else {
                throw new IOException("Unsupported type definition");
            }
        }
    }

}
