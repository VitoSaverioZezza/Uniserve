package edu.stanford.futuredata.uniserve.rel;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.coordinator.Coordinator;
import edu.stanford.futuredata.uniserve.coordinator.DefaultAutoScaler;
import edu.stanford.futuredata.uniserve.coordinator.DefaultLoadBalancer;
import edu.stanford.futuredata.uniserve.datastore.DataStore;
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

import static edu.stanford.futuredata.uniserve.localcloud.LocalDataStoreCloud.deleteDirectoryRecursion;

public class Q1Test {
    private static final Logger logger = LoggerFactory.getLogger(Q1Test.class);

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
    public void Q1Test(){
        startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        loadDataInMem(broker);

        ReadQuery j_ss_d = api.join()
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("(ss_sales_price >= 50 && ss_sales_price <=200) && ss_net_profit >= 50 && ss_net_profit <=300"
                        , "d_year == 2001")
                .build();


        ReadQuery j_ss_s_d = api.join().select(
                        "j_ss_d.store_sales.ss_net_profit",
                        "j_ss_d.store_sales.ss_cdemo_sk",
                        "j_ss_d.store_sales.ss_hdemo_sk",
                        "j_ss_d.store_sales.ss_sales_price",
                        "j_ss_d.store_sales.ss_addr_sk",
                        "j_ss_d.store_sales.ss_quantity",
                        "j_ss_d.store_sales.ss_ext_sales_price",
                        "j_ss_d.store_sales.ss_ext_wholesale_cost"
                ).alias(
                        "ss_net_profit",
                        "ss_cdemo_sk",
                        "ss_hdemo_sk",
                        "ss_sales_price",
                        "ss_addr_sk",
                        "ss_quantity",
                        "ss_ext_sales_price",
                        "ss_ext_wholesale_cost"
                ).sources("store", j_ss_d, "j_ss_d", List.of("s_store_sk"), List.of("store_sales.ss_store_sk"))
                .build();

        ReadQuery j_cd = api.join().sources(
                        j_ss_s_d, "customer_demographics", "j_ss_s_d",
                        List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("",
                        "(cd_marital_status == \"U\" && cd_education_status == \"Advanced Degree\") || " +
                                "(cd_marital_status == \"M\" && cd_education_status == \"Primary\") || " +
                                "(cd_marital_status == \"D\" && cd_education_status == \"Secondary\")")
                .build();

        ReadQuery j_cd_hd = api.join().sources(
                        j_cd, "household_demographics", "j_cd",
                        List.of("j_ss_s_d.ss_hdemo_sk"), List.of("hd_demo_sk")).filters("",
                        "hd_dep_count == 3 || hd_dep_count == 1")
                .build();


        ReadQuery firstPart = api.read().select(
                        "j_cd.j_ss_s_d.ss_quantity",
                        "j_cd.j_ss_s_d.ss_ext_sales_price",
                        "j_cd.j_ss_s_d.ss_ext_wholesale_cost",
                        "j_cd.j_ss_s_d.ss_addr_sk",
                        "j_cd.j_ss_s_d.ss_net_profit"
                ).alias(
                        "ss_quantity",
                        "ss_ext_sales_price",
                        "ss_ext_wholesale_cost",
                        "ss_addr_sk",
                        "ss_net_profit"
                )
                .fromFilter(j_cd_hd, "src",
                        "(j_cd.j_ss_s_d.cd_marital_status == \"U\" && j_cd.j_ss_s_d.cd_education_status == \"Advanced Degree\" && " +
                                "j_cd.j_ss_s_d.ss_sales_price >= 100 && j_cd.j_ss_s_d.ss_sales_price <= 150 && household_demographics.hd_dep_count == 3) || " +
                                "(j_cd.j_ss_s_d.cd_marital_status == \"M\" && j_cd.j_ss_s_d.cd_education_status == \"Primary\" && " +
                                "j_cd.j_ss_s_d.ss_sales_price >= 50 && j_cd.j_ss_s_d.ss_sales_price <= 100 && household_demographics.hd_dep_count == 1) || " +
                                "(j_cd.j_ss_s_d.cd_marital_status == \"D\" && j_cd.j_ss_s_d.cd_education_status == \"Secondary\" && " +
                                "j_cd.j_ss_s_d.ss_sales_price >= 150 && j_cd.j_ss_s_d.ss_sales_price <= 200 && household_demographics.hd_dep_count == 1)"
                ).build();

        ReadQuery final_source = api.join().sources(
                firstPart, "customer_address", "first_part",
                List.of("ss_addr_sk"), List.of("ca_address_sk")).filters("",
                "ca_country == \"United States\" && " +
                        "[\"AZ\",\"NE\",\"IA\",\"MS\",\"CA\",\"NV\",\"GA\",\"TX\",\"NJ\"].contains(ca_state)").build();

        ReadQuery q1 = api.read()
                .avg("first_part.ss_quantity", "avg_ss_quantity")
                .avg("first_part.ss_ext_sales_price", "avg_ss_ext_sales_price")
                .avg("first_part.ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                .sum("first_part.ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                .fromFilter(final_source, "f_src",
                        "([\"AZ\",\"NE\",\"IA\"].contains(customer_address.ca_state) && " +
                                "first_part.ss_sales_price >= 50.00 && " +
                                "first_part.ss_sales_price <= 100.00) ||" +
                                "([\"MS\",\"CA\",\"NV\"].contains(customer_address.ca_state) && " +
                                "first_part.ss_sales_price >= 150.00 && " +
                                "first_part.ss_sales_price <= 300.00) ||" +
                                "([\"GA\",\"TX\",\"NJ\"].contains(customer_address.ca_state) && " +
                                "first_part.ss_sales_price >= 50.00 && " +
                                "first_part.ss_sales_price <= 250.00)"
                ).build();

        RelReadQueryResults results = q1.run(broker);
        printRowList(results.getData());

        broker.shutdown();
        stopServers();
    }
    public List<String> tablesToLoad = List.of(
            "store_sales",
            "date_dim",
            "store",
            "customer_demographics",
            "household_demographics",
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
            int shardNum = Math.min(Math.max(memBuffer.size() / 1000, 1), Broker.SHARDS_PER_TABLE);
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

    int rowCount = 0;

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

            rowCount = 0;

            try (BufferedReader br = new BufferedReader(new FileReader(path))) {
                String line;
                while ((line = br.readLine()) != null && rowCount < 150000) {
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