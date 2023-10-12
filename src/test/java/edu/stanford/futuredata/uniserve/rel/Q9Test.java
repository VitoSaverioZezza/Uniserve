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

public class Q9Test {
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
    public void Q8Test(){
        startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        loadDataInMem(broker);

        ReadQuery ssMarch99 = api.join().sources(
                "store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk")
        ).filters("", "d_year == 1999 && d_month == 3").build();

        ReadQuery booksSoldInStoreMarch99 = api.join().sources("item", ssMarch99, "ssMarch99", List.of("i_item_sk"), List.of("store_sales.ss_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery ssSrc = api.join().select("books.ssMarch99.store_sales.ss_ext_sales_price", "books.item.i_manufact_id").alias("ss_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInStoreMarch99, "customer_address", "books", List.of("ssMarch99.store_sales.ss_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ss = api.read().select("i_manufact_id").sum("ss_ext_sales_price", "total_sales").from(ssSrc, "ssSrc").build();

        ReadQuery csMarch99 = api.join().sources(
                "catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk")
        ).filters("", "d_year == 1999 && d_month == 3").build();

        ReadQuery booksSoldInCatalogMarch99 = api.join().sources("item", csMarch99, "csMarch99", List.of("i_item_sk"), List.of("catalog_sales.cs_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery csSrc = api.join().select("books.csMarch99.catalog_sales.cs_ext_sales_price", "books.item.i_manufact_id").alias("cs_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInCatalogMarch99, "customer_address", "books", List.of("csMarch99.catalog_sales.cs_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery cs = api.read().select("i_manufact_id").sum("cs_ext_sales_price", "total_sales").from(csSrc, "csSrc").build();

        ReadQuery wsMarch99 = api.join().sources(
                "web_sales", "date_dim", List.of("ws_sold_date_sk"), List.of("d_date_sk")
        ).filters("", "d_year == 1999 && d_month == 3").build();

        ReadQuery booksSoldOnlineMarch99 = api.join().sources("item", wsMarch99, "wsMarch99", List.of("i_item_sk"), List.of("web_sales.ws_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery wsSrc = api.join().select("books.wsMarch99.web_sales.ws_ext_sales_price", "books.item.i_manufact_id").alias("ws_ext_sales_price", "i_manufact_id")
                .sources(booksSoldOnlineMarch99, "customer_address", "books", List.of("wsMarch99.web_sales.ws_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ws = api.read().select("i_manufact_id").sum("ws_ext_sales_price", "total_sales").from(wsSrc, "csSrc").build();

        ReadQuery unionCsSs = api.union().sources(ss, cs, "ss", "ws").build();
        ReadQuery finalSrc = api.union().sources(unionCsSs, ws, "ss_cs", "ws").build();
        ReadQuery finalQuery = api.read().select("i_manufact_id").sum("total_sales", "total_sales").from(finalSrc, "src").build();

        RelReadQueryResults results = finalQuery.run(broker);

        System.out.println("RESULTS:");
        printRowList(results.getData());

        System.out.println("\nreturning...");
        broker.shutdown();
        stopServers();
    }
    /*-- start query 33 in stream 0 using template query33.tpl
WITH ss
     AS (SELECT i_manufact_id,
                Sum(ss_ext_sales_price) total_sales
         FROM   store_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_manufact_id IN (SELECT i_manufact_id
                                  FROM   item
                                  WHERE  i_category IN ( 'Books' ))
                AND ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 3
                AND ss_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
         GROUP  BY i_manufact_id),
*//*
     cs
     AS (SELECT i_manufact_id,
                Sum(cs_ext_sales_price) total_sales
         FROM   catalog_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_manufact_id IN (SELECT i_manufact_id
                                  FROM   item
                                  WHERE  i_category IN ( 'Books' ))
                AND cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 3
                AND cs_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
         GROUP  BY i_manufact_id),
         *//*
     ws
     AS (SELECT i_manufact_id,
                Sum(ws_ext_sales_price) total_sales
         FROM   web_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_manufact_id IN (SELECT i_manufact_id
                                  FROM   item
                                  WHERE  i_category IN ( 'Books' ))
                AND ws_item_sk = i_item_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 3
                AND ws_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
         GROUP  BY i_manufact_id)
         */
    /*
SELECT i_manufact_id,
               Sum(total_sales) total_sales
FROM   (SELECT *
        FROM   ss
        UNION ALL
        SELECT *
        FROM   cs
        UNION ALL
        SELECT *
        FROM   ws) tmp1
GROUP  BY i_manufact_id
 */








    public List<String> tablesToLoad = List.of(
            "catalog_sales",
            "web_sales",
            "store_sales",
            "date_dim",
            "item",
            "customer_address"
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
