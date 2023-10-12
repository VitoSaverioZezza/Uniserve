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

public class Q2Test {
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
    String rootPath = "C:\\Users\\saver\\Desktop\\db00_0625";

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

    int filmCount = 0;
    int actorCount = 0;
    int coursesCount = 0;
    int examsCount = 0;
    int professorsCount = 0;
    int studentsCount = 0;

    List<RelRow> filmRows = new ArrayList<>();
    List<RelRow> actorRows = new ArrayList<>();
    List<RelRow> coursesRows = new ArrayList<>();
    List<RelRow> examsRows = new ArrayList<>();
    List<RelRow> studentsRows = new ArrayList<>();
    List<RelRow> professorsRows = new ArrayList<>();


    @Test
    public void Q2Test(){
        startServers();

        Broker broker = new Broker(zkHost, zkPort);
        loadDataInMem(broker);
        API api = new API(broker);
        List<String> cs_c_join_schema = new ArrayList<>();
        cs_c_join_schema.addAll(TPC_DS_Inv.Catalog_sales_schema);
        cs_c_join_schema.addAll(TPC_DS_Inv.Customer_schema);
        List<String> cs_c_ca_join_schema = new ArrayList<>();
        cs_c_ca_join_schema.addAll(cs_c_join_schema);
        cs_c_ca_join_schema.addAll(TPC_DS_Inv.Customer_address_schema);
        List<String> final_schema = new ArrayList<>();
        final_schema.addAll(cs_c_ca_join_schema);
        final_schema.addAll(TPC_DS_Inv.Date_dim_schema);

        ReadQuery cs_c_join = api.join().sources("catalog_sales", "customer",
                         List.of("cs_bill_customer_sk"), List.of("c_customer_sk"))
                .alias(cs_c_join_schema.toArray(new String[0])).build();

        ReadQuery cs_c_ca_join = api.join().sources(cs_c_join, "customer_address", "cs_c_join",
                         List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .alias(cs_c_ca_join_schema.toArray(new String[0])).build();


        ReadQuery final_src = api.join().sources(cs_c_ca_join, "date_dim", "cs_c_ca_join"
                        , List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters(
                        "((ca_zip =~ '85669|86197|88274|83405|86475|85392|85460|80348|81792') || " +
                                "[\"CA\", \"WA\", \"GA\"].contains(ca_state) || cs_sales_price > 500) && ca_zip != \"\"",
                        "d_qoy == 1 && d_year == 1998")
                .alias(final_schema.toArray(new String[0])).build();
        ReadQuery res = api.read().select("ca_zip").sum("cs_sales_price", "sum_cs_sales_price").
                from(final_src, "src").build();
        RelReadQueryResults results = res.run(broker);

        RelReadQueryResults catalog_sales_all = api.read().select().from("catalog_sales").build().run(broker);
        RelReadQueryResults customer_all = api.read().select().from("customer").build().run(broker);
        RelReadQueryResults customer_address_all = api.read().select().from("customer_address").build().run(broker);
        RelReadQueryResults date_dim_all = api.read().select().from("date_dim").build().run(broker);

        System.out.println("\n\nALL CATALOG SALES");
        printRowList(catalog_sales_all.getData());
        System.out.println("\n\nALL CUSTOMERS");
        printRowList(customer_all.getData());
        System.out.println("\n\nALL CUSTOMER ADDRESS");
        printRowList(customer_address_all.getData());
        System.out.println("\n\nALL DATE DIM");
        printRowList(date_dim_all.getData());

        List<RelRow> j1Res = new ArrayList<>();
        for (RelRow csRow : catalog_sales_all.getData()){
            for (RelRow cRow: customer_all.getData()){
                if(csRow.getField(TPC_DS_Inv.Catalog_sales_schema.indexOf("cs_bill_customer_sk")).equals(cRow.getField(TPC_DS_Inv.Customer_schema.indexOf("c_customer_sk")))){
                   System.out.println("\t\t---matching value: " + cRow.getField(0));
                   List<Object> rawNew = new ArrayList<>();
                   for(int i = 0; i<csRow.getSize(); i++){
                       rawNew.add(csRow.getField(i));
                   }
                   for(int i = 0; i<cRow.getSize(); i++){
                       rawNew.add(cRow.getField(i));
                   }
                   j1Res.add(new RelRow(rawNew.toArray()));
                }
            }
        }


        cs_c_join.setIsThisSubquery(false);
        RelReadQueryResults c_cs_join_res = cs_c_join.run(broker);
        System.out.println("\n\nC CS JOIN RESULTS");
        printRowList(c_cs_join_res.getData());
        System.out.println("\n\nEXPECTED C CS JOIN RESULTS");
        printRowList(j1Res);


        List<RelRow> j2Res = new ArrayList<>();
        for (RelRow caRow : customer_address_all.getData()){
            for (RelRow j1Row: j1Res){
                if(j1Row.getField(cs_c_join_schema.indexOf("c_current_addr_sk")).equals(caRow.getField(TPC_DS_Inv.Customer_address_schema.indexOf("ca_address_sk")))){
                    System.out.println("\t\t---matching value: " + caRow.getField(TPC_DS_Inv.Customer_address_schema.indexOf("ca_address_sk")));
                    List<Object> rawNew = new ArrayList<>();
                    for(int i = 0; i<j1Row.getSize(); i++){
                        rawNew.add(j1Row.getField(i));
                    }
                    for(int i = 0; i<caRow.getSize(); i++){
                        rawNew.add(caRow.getField(i));
                    }
                    j2Res.add(new RelRow(rawNew.toArray()));
                }else{
                    System.out.println("\t\tvj1: " + j1Row.getField(cs_c_join_schema.indexOf("c_current_addr_sk")) +
                            " vJ2: " + caRow.getField(TPC_DS_Inv.Customer_address_schema.indexOf("ca_address_sk")));
                }
            }
        }

        cs_c_ca_join.setIsThisSubquery(false);
        RelReadQueryResults cs_c_ca_join_res = cs_c_ca_join.run(broker);
        System.out.println("\n\nC CS CA JOIN RESULTS");
        printRowList(cs_c_ca_join_res.getData());
        System.out.println(customer_address_all.getData().size());
        System.out.println(j1Res.size());
        System.out.println("\n\nEXPECTED C CS CA JOIN RESULTS");
        printRowList(j2Res);

        System.out.println("\n\n\n\n\n");
        printRowList(results.getData());
        System.out.println("-----\n\n\n\n\n");

        stopServers();
    }



    public void loadDataInMem(Broker broker) {
        API api = new API(broker);
        boolean res = true;
        for (int i = 0; i < TPC_DS_Inv.numberOfTables; i++) {
            List<RelRow> memBuffer = new ArrayList<>();
            MemoryLoader memoryLoader = new MemoryLoader(i, memBuffer, broker);
            if(!memoryLoader.run()){
                return;
            }
            int shardNum = Math.min(Math.max(memBuffer.size(), 1), Broker.SHARDS_PER_TABLE);
            res = api.createTable(TPC_DS_Inv.names.get(i))
                    .attributes(TPC_DS_Inv.schemas.get(i).toArray(new String[0]))
                    .keys(TPC_DS_Inv.schemas.get(i).toArray(new String[0]))
                    .shardNumber(shardNum)
                    .build().run();
            if(!res){
                System.out.println("Failed to create table " + TPC_DS_Inv.names.get(i) + "\nTable may be already registered");
            }
            System.out.println("Table " + TPC_DS_Inv.names.get(i) + " created");
            res = api.write().table(TPC_DS_Inv.names.get(i)).data(memBuffer.toArray(new RelRow[0])).build().run();
            System.out.println("Table " + TPC_DS_Inv.names.get(i) + " written");
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

        private final Broker broker;
        public MemoryLoader(int index, List<RelRow> sink, Broker broker){
            this.sink = sink;
            this.index = index;
            this.broker = broker;
        }
        public boolean run(){
            String path = rootPath + TPC_DS_Inv.paths.get(index);
            List<Integer> types = TPC_DS_Inv.types.get(index);

            int rowCount = 0;

            try (BufferedReader br = new BufferedReader(new FileReader(path))) {
                String line;
                while ((line = br.readLine()) != null && rowCount<100) {
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
                broker.shutdown();
                e.printStackTrace();
                return false;
            }
            System.out.println("Data for table " + TPC_DS_Inv.names.get(index) + " loaded successfully");
            System.out.println("Number of rows: " + sink.size());
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
