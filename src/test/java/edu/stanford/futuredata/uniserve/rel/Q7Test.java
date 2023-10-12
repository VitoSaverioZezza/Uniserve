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

public class Q7Test {
    private static final Logger logger = LoggerFactory.getLogger(Q2Test.class);

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
    public void Q7Test(){
        startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        loadDataInMem(broker);


        ReadQuery storeSalesInApril2001 = api.join().select(
                "store_sales.ss_customer_sk",
                "store_sales.ss_item_sk",
                "store_sales.ss_ticket_number",
                "store_sales.ss_net_profit",
                "store_sales.ss_store_sk"
        ).alias(
                "ss_customer_sk",
                "ss_item_sk",
                "ss_ticket_number",
                "ss_net_profit",
                "ss_store_sk"
        ).sources(
                "store_sales", "date_dim",
                List.of("ss_sold_date_sk"), List.of("d_date_sk")
        ).filters(
                "", "d_moy == 4 && d_year == 2001"
        ).build();


        ReadQuery storeReturnsIn2001AfterApril = api.join().select(
                "store_returns.sr_customer_sk",
                "store_returns.sr_item_sk",
                "store_returns.sr_ticket_number",
                "store_returns.sr_net_loss"
        ).alias(
                "sr_customer_sk",
                "sr_item_sk",
                "sr_ticket_number",
                "sr_net_loss"
        ).sources(
                "store_returns", "date_dim",
                List.of("sr_returned_date_sk"), List.of("d_date_sk")
        ).filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001").build();


        ReadQuery catalogSalesIn2001AfterApril = api.join().select(
                "catalog_sales.cs_bill_customer_sk",
                "catalog_sales.cs_item_sk",
                "catalog_sales.cs_net_profit"
        ).alias(
                "cs_bill_customer_sk",
                "cs_item_sk",
                "cs_net_profit"
        ).sources(
                "catalog_sales", "date_dim",
                List.of("cs_sold_date_sk"), List.of("d_date_sk")
        ).filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001").build();

        ReadQuery returnedStoreAprilSales = api.join().select(
                "sales.ss_net_profit",
                "sales.ss_item_sk",
                "sales.ss_store_sk",
                "sales.ss_customer_sk",
                "returns.sr_net_loss"
        ).alias(
                "ss_net_profit",
                "ss_item_sk",
                "ss_store_sk",
                "ss_customer_sk",
                "sr_net_loss"
        ).sources(
                storeSalesInApril2001, storeReturnsIn2001AfterApril, "sales", "returns",
                List.of(
                        "ss_customer_sk",
                        "ss_item_sk",
                        "ss_ticket_number"),
                List.of(
                        "sr_customer_sk",
                        "sr_item_sk",
                        "sr_ticket_number"
                )
                ).build();

        ReadQuery returnedStoreSalesInAprilWhoLaterBoughtOnCatalog = api.join().select(
                "rsasp.ss_item_sk",
                "rsasp.ss_store_sk",
                "csaa.cs_net_profit",
                "rsasp.ss_net_profit",
                "rsasp.sr_net_loss"
        ).alias(
                "ss_item_sk",
                "ss_store_sk",
                "cs_net_profit",
                "ss_net_profit",
                "sr_net_loss"
        ).sources(
                returnedStoreAprilSales, catalogSalesIn2001AfterApril, "rsasp", "csaa",
                List.of("ss_customer_sk",
                        "ss_item_sk"),
                List.of(
                        "cs_bill_customer_sk",
                        "cs_item_sk")
        ).build();

        ReadQuery joinItem = api.join().select(
                "penultimateJoin.ss_store_sk",
                "item.i_item_id",
                "item.i_item_desc",
                "penultimateJoin.cs_net_profit",
                "penultimateJoin.ss_net_profit",
                "penultimateJoin.sr_net_loss"
        ).alias(
                "ss_store_sk",
                "i_item_id",
                "i_item_desc",
                "cs_net_profit",
                "ss_net_profit",
                "sr_net_loss"
        ).sources(
                returnedStoreSalesInAprilWhoLaterBoughtOnCatalog, "item", "penultimateJoin",
                List.of("ss_item_sk"), List.of("i_item_sk")
        ).build();

        ReadQuery joinStore = api.join().select(
                "j1.i_item_id",
                "j1.i_item_desc",
                "store.s_store_id",
                "store.s_store_name",
                "j1.cs_net_profit",
                "j1.ss_net_profit",
                "j1.sr_net_loss"
        ).alias(
                "i_item_id",
                "i_item_desc",
                "s_store_id",
                "s_store_name",
                "cs_net_profit",
                "ss_net_profit",
                "sr_net_loss"
        ).sources(
                joinItem, "store", "j1",
                List.of("ss_store_sk"), List.of("s_store_sk")
        ).build();


        ReadQuery finalQuery = api.read().select("i_item_id",
                        "i_item_desc",
                        "s_store_id",
                        "s_store_name"
                )
                .max("ss_net_profit", "store_sales_profit")
                .max("sr_net_loss", "store_returns_loss")
                .max("cs_net_profit", "catalog_sales_profit")
                .from(joinStore, "src")
                .build();


        RelReadQueryResults results = finalQuery.run(broker);

        System.out.println("RESULTS:");
        printRowList(results.getData());

        System.out.println("\nreturning...");
        stopServers();
    }


    public List<String> tablesToLoad = List.of(
            "store_sales",
            "store_returns",
            "catalog_sales",
            "date_dim",
            "store",
            "item"
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
            int shardNum = Math.min(Math.max(memBuffer.size(), 1), Broker.SHARDS_PER_TABLE);
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
                while ((line = br.readLine()) != null && rowCount < 100) {
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
