package edu.stanford.futuredata.uniserve.rel;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.DefaultAutoScaler;
import edu.stanford.futuredata.uniserve.coordinator.DefaultLoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.localcloud.LocalDataStoreCloud;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.relational.RelShardFactory;
import edu.stanford.futuredata.uniserve.relationalapi.API;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static edu.stanford.futuredata.uniserve.localcloud.LocalDataStoreCloud.deleteDirectoryRecursion;

public class Q3Test {
    private static final Logger logger = LoggerFactory.getLogger(Q3Test.class);

    private static String zkHost = "127.0.0.1";
    private static Integer zkPort = 2181;

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
            logger.info("Zookeeper cleanup failed: {}", e.getMessage());
        }
        // Clean up directories.
        try {
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve"));
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve0"));
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve1"));
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve2"));
            FileUtils.deleteDirectory(new File("/var/tmp/RelUniserve3"));
        } catch (IOException e) {
            logger.info("FS cleanup failed: {}", e.getMessage());
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


    @BeforeAll
    static void startUpCleanUp() throws IOException {
        a();
        cleanUp(zkHost, zkPort);

    }

    @AfterEach
    private void unitTestCleanUp() throws IOException {
        a();
        cleanUp(zkHost, zkPort);
    }
    @Test
    public void clean(){}


    private void printRowList(List<RelRow> data) {
        for (RelRow row : data) {
            StringBuilder rowBuilder = new StringBuilder();
            rowBuilder.append("Row #" + data.indexOf(row) + " ");
            for (int j = 0; j < row.getSize() - 1; j++) {
                rowBuilder.append(row.getField(j) + ", ");
            }
            rowBuilder.append(row.getField(row.getSize() - 1));
            System.out.println(rowBuilder.toString());
        }
    }

    int numServers = 10;
    Coordinator coordinator = null;
    List<LocalDataStoreCloud> ldscList = new ArrayList<>();
    List<DataStore<RelRow, RelShard>> dataStores = new ArrayList<>();
    String rootPath = "C:\\Users\\saver\\Desktop\\db00_0625_clean";

    private void startServers(){
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
    private void stopServers(){
        for(DataStore dataStore:dataStores) {
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


    @Test
    public void Q3Test(){
        startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        loadDataInMem(broker);
        ReadQuery j1 = api.join().sources(
                "date_dim", "store_sales",
                List.of("d_date_sk"), List.of("ss_sold_date_sk")
        ).filters(
                "d_moy == 12 && d_year == 1998", ""
        ).build();
        ReadQuery pj1 = api.read().select(
                "store_sales.ss_ext_sales_price",
                "store_sales.ss_customer_sk",
                "store_sales.ss_item_sk",
                "store_sales.ss_store_sk"
        ).alias(
                "ss_ext_sales_price",
                "ss_customer_sk",
                "ss_item_sk",
                "ss_store_sk"
        ).from(j1, "j1").build();

        ReadQuery j2 = api.join().sources(
                pj1, "item", "pj1",
                List.of("ss_item_sk"), List.of("i_item_sk")
        ).filters(
                "", "i_manager_id == 38"
        ).build();

        ReadQuery j3 = api.join().sources(
                j2, "customer", "j2",
                List.of("pj1.ss_customer_sk"), List.of("c_customer_sk")
        ).build();

        ReadQuery pj3 = api.read().select(
                "customer.c_current_addr_sk",
                "j2.pj1.ss_ext_sales_price",
                "j2.pj1.ss_store_sk",
                "j2.item.i_brand",
                "j2.item.i_brand_id",
                "j2.item.i_manufact_id",
                "j2.item.i_manufact"
        ).alias(
                "c_current_addr_sk",
                "ss_ext_sales_price",
                "ss_store_sk",
                "i_brand",
                "i_brand_id",
                "i_manufact_id",
                "i_manufact"
        ).from(j3, "j3").build();

        ReadQuery j4 = api.join().sources(
                pj3, "customer_address", "pj3",
                List.of("c_current_addr_sk"), List.of("ca_address_sk")
        ).build();

        ReadQuery pj4 = api.read().select(
                "pj3.ss_ext_sales_price",
                "pj3.ss_store_sk",
                "pj3.i_brand",
                "pj3.i_brand_id",
                "pj3.i_manufact_id",
                "pj3.i_manufact",
                "customer_address.ca_zip"
        ).alias(
                "ss_ext_sales_price",
                "ss_store_sk",
                "i_brand",
                "i_brand_id",
                "i_manufact_id",
                "i_manufact",
                "ca_zip"
        ).from(j4, "j4").build();

        ReadQuery j5 = api.join().sources(
                pj4, "store", "pj4",
                List.of("ss_store_sk"), List.of("s_store_sk")
        ).build();

        ReadQuery fpj5 = api.read().select(
                "pj4.i_brand_id",
                "pj4.i_brand",
                "pj4.i_manufact_id",
                "pj4.i_manufact"
        ).alias(
                "ss_ext_sales_price",
                "i_brand_id",
                "i_manufact_id",
                "i_manufact"
        ).sum(
                "pj4.ss_ext_sales_price", "ext_price"
        ).fromFilter(j5, "j5", "pj4.ca_zip != store.s_zip").build();

        RelReadQueryResults results = fpj5.run(broker);

        System.out.println("RESULTS:");
        printRowList(results.getData());

        System.out.println("\nreturning...");
        stopServers();
    }


    public List<String> tablesToLoad = List.of(
            "date_dim",
            "store_sales",
            "item",
            "customer",
            "customer_address",
            "store"
    );

    public void loadDataInMem(Broker broker) {
        API api = new API(broker);
        boolean res = true;
        for (int i = 0; i < TPC_DS_Inv.numberOfTables; i++) {
            if(!tablesToLoad.contains(TPC_DS_Inv.names.get(i))){
                continue;
            }else{
                System.out.println("Loading data for table " + TPC_DS_Inv.names.get(i));
            }
            List<RelRow> memBuffer = new ArrayList<>();
            MemoryLoader memoryLoader = new MemoryLoader(i, memBuffer);
            if(!memoryLoader.run()){
                return;
            }
            int shardNum = Math.min(Math.max(memBuffer.size()/1000, 1), Broker.SHARDS_PER_TABLE);
            res = api.createTable(TPC_DS_Inv.names.get(i))
                    .attributes(TPC_DS_Inv.schemas.get(i).toArray(new String[0]))
                    .keys(TPC_DS_Inv.schemas.get(i).toArray(new String[0]))
                    .shardNumber(shardNum)
                    .build().run();
            res = api.write().table(TPC_DS_Inv.names.get(i)).data(memBuffer.toArray(new RelRow[0])).build().run();
            memBuffer.clear();
            if(!res){
                broker.shutdown();
                throw new RuntimeException("Write error for table "+TPC_DS_Inv.names.get(i));
            }
        }
    }

    private class MemoryLoader{
        private final int index;
        private final List<RelRow> sink;
        public MemoryLoader(int index, List<RelRow> sink){
            this.sink = sink;
            this.index = index;
        }
        public boolean run(){
            String path = rootPath + TPC_DS_Inv.paths.get(index);
            List<Integer> types = TPC_DS_Inv.types.get(index);

            int rowCount = 0;

            try (BufferedReader br = new BufferedReader(new FileReader(path))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if(rowCount % 10000 == 0){
                        System.out.println("Row count: " + rowCount);
                    }
                    String[] parts = line.split("\\|");
                    List<Object> parsedRow = new ArrayList<>(types.size());
                    if (parts.length == types.size()) {
                        for(int i = 0; i<parts.length; i++){
                            parsedRow.add(parseField(parts[i],types.get(i)));
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
