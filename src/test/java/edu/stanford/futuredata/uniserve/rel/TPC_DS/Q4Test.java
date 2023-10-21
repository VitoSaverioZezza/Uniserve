package edu.stanford.futuredata.uniserve.rel.TPC_DS;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relationalapi.API;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static edu.stanford.futuredata.uniserve.rel.TPC_DS.TestMethods.*;

public class Q4Test {
    @BeforeAll
    static void startUpCleanUp() throws IOException {
        TestMethods.startUpCleanUp();
    }

    @AfterEach
    public void unitTestCleanUp() throws IOException {
        TestMethods.unitTestCleanUp();
    }
    @Test
    public void clean(){}

    public List<String> tablesToLoad = List.of(
            "inventory",
            "date_dim",
            "item",
            "warehouse"
    );

    /*
Query 22 of the TPC DS benchmark
SELECT i_product_name,
               i_brand,
               i_class,
               i_category,
               Avg(inv_quantity_on_hand) qoh
FROM   inventory,
       date_dim,
       item,
       warehouse
WHERE  inv_date_sk = d_date_sk
       AND inv_item_sk = i_item_sk
       AND inv_warehouse_sk = w_warehouse_sk
       AND d_month_seq BETWEEN 1205 AND 1205 + 11
GROUP  i_product_name, i_brand, i_class, i_category
     */
    @Test
    public void Q4Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        for (String tableName : tablesToLoad) {
            int index = TPC_DS_Inv.names.indexOf(tableName);
            int shardNum = Math.min(Math.max(TPC_DS_Inv.sizes.get(index) / 1000, 1), Broker.SHARDS_PER_TABLE);
            api.createTable(tableName)
                    .attributes(TPC_DS_Inv.schemas.get(index).toArray(new String[0]))
                    .keys(TPC_DS_Inv.keys.get(index).toArray(new String[0]))
                    .shardNumber(shardNum)
                    .build().run();
        }
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);
        ReadQuery j1 = api.join()
                .select("inventory.inv_quantity_on_hand", "inventory.inv_item_sk", "inventory.inv_warehouse_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk", "inv_warehouse_sk")
                .sources("inventory", "date_dim", List.of("inv_date_sk"), List.of("d_date_sk"))
                .filters("", "d_month_seq >= 1205 && d_month_seq <= 1216").build();

        ReadQuery j2 = api.join()
                .select("j1p.inv_quantity_on_hand", "j1p.inv_item_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk")
                .sources(j1, "warehouse", "j1p", List.of("inv_warehouse_sk"), List.of("w_warehouse_sk")).build();

        ReadQuery j3 = api.join().sources(j2, "item", "j2", List.of("inv_item_sk"), List.of("i_item_sk")).build();

        ReadQuery finalQuery = api.read()
                .select("item.i_product_name", "item.i_brand", "item.i_class", "item.i_category")
                .avg("j2.inv_quantity_on_hand", "qoh")
                .alias("i_product_name", "i_brand", "i_class", "i_category")
                .from(j3, "j3").build();

        System.out.println("RESULTS:");
        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() -t;
        printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res4");
        tm.stopServers();
        broker.shutdown();
        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q4timings");

    }
    @Test
    public void Q4_1Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        for (String tableName : tablesToLoad) {
            int index = TPC_DS_Inv.names.indexOf(tableName);
            int shardNum = Math.min(Math.max(TPC_DS_Inv.sizes.get(index) / 1000, 1), Broker.SHARDS_PER_TABLE);
            api.createTable(tableName)
                    .attributes(TPC_DS_Inv.schemas.get(index).toArray(new String[0]))
                    .keys(TPC_DS_Inv.keys.get(index).toArray(new String[0]))
                    .shardNumber(shardNum)
                    .build().run();
        }
        ReadQuery j1 = api.join()
                .select("inventory.inv_quantity_on_hand", "inventory.inv_item_sk", "inventory.inv_warehouse_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk", "inv_warehouse_sk")
                .sources("inventory", "date_dim", List.of("inv_date_sk"), List.of("d_date_sk"))
                .filters("", "d_month_seq >= 1205 && d_month_seq <= 1216").stored().build();

        j1.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery j2 = api.join()
                .select("j1p.inv_quantity_on_hand", "j1p.inv_item_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk")
                .sources(j1, "warehouse", "j1p", List.of("inv_warehouse_sk"), List.of("w_warehouse_sk")).build();

        ReadQuery j3 = api.join().sources(j2, "item", "j2", List.of("inv_item_sk"), List.of("i_item_sk")).build();

        ReadQuery finalQuery = api.read()
                .select("item.i_product_name", "item.i_brand", "item.i_class", "item.i_category")
                .avg("j2.inv_quantity_on_hand", "qoh")
                .alias("i_product_name", "i_brand", "i_class", "i_category")
                .from(j3, "j3").build();

        System.out.println("RESULTS:");
        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() -t;
        printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res4");
        tm.stopServers();
        broker.shutdown();
        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q4_1timings");

    }
    @Test
    public void Q4_2Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        for (String tableName : tablesToLoad) {
            int index = TPC_DS_Inv.names.indexOf(tableName);
            int shardNum = Math.min(Math.max(TPC_DS_Inv.sizes.get(index) / 1000, 1), Broker.SHARDS_PER_TABLE);
            api.createTable(tableName)
                    .attributes(TPC_DS_Inv.schemas.get(index).toArray(new String[0]))
                    .keys(TPC_DS_Inv.keys.get(index).toArray(new String[0]))
                    .shardNumber(shardNum)
                    .build().run();
        }
        ReadQuery j1 = api.join()
                .select("inventory.inv_quantity_on_hand", "inventory.inv_item_sk", "inventory.inv_warehouse_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk", "inv_warehouse_sk")
                .sources("inventory", "date_dim", List.of("inv_date_sk"), List.of("d_date_sk"))
                .filters("", "d_month_seq >= 1205 && d_month_seq <= 1216").build();

        ReadQuery j2 = api.join()
                .select("j1p.inv_quantity_on_hand", "j1p.inv_item_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk")
                .stored()
                .sources(j1, "warehouse", "j1p", List.of("inv_warehouse_sk"), List.of("w_warehouse_sk")).build();

        j2.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery j3 = api.join().sources(j2, "item", "j2", List.of("inv_item_sk"), List.of("i_item_sk")).build();

        ReadQuery finalQuery = api.read()
                .select("item.i_product_name", "item.i_brand", "item.i_class", "item.i_category")
                .avg("j2.inv_quantity_on_hand", "qoh")
                .alias("i_product_name", "i_brand", "i_class", "i_category")
                .from(j3, "j3").build();

        System.out.println("RESULTS:");
        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() -t;

        tm.stopServers();
        broker.shutdown();
        printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res4");
        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q4_3timings");

    }
    @Test
    public void Q4_3Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        for (String tableName : tablesToLoad) {
            int index = TPC_DS_Inv.names.indexOf(tableName);
            int shardNum = Math.min(Math.max(TPC_DS_Inv.sizes.get(index) / 1000, 1), Broker.SHARDS_PER_TABLE);
            api.createTable(tableName)
                    .attributes(TPC_DS_Inv.schemas.get(index).toArray(new String[0]))
                    .keys(TPC_DS_Inv.keys.get(index).toArray(new String[0]))
                    .shardNumber(shardNum)
                    .build().run();
        }
        ReadQuery j1 = api.join()
                .select("inventory.inv_quantity_on_hand", "inventory.inv_item_sk", "inventory.inv_warehouse_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk", "inv_warehouse_sk")
                .sources("inventory", "date_dim", List.of("inv_date_sk"), List.of("d_date_sk"))
                .filters("", "d_month_seq >= 1205 && d_month_seq <= 1216").build();

        ReadQuery j2 = api.join()
                .select("j1p.inv_quantity_on_hand", "j1p.inv_item_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk")
                .sources(j1, "warehouse", "j1p", List.of("inv_warehouse_sk"), List.of("w_warehouse_sk")).build();

        ReadQuery j3 = api.join().stored().sources(j2, "item", "j2", List.of("inv_item_sk"), List.of("i_item_sk")).build();

        j3.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);


        ReadQuery finalQuery = api.read()
                .select("item.i_product_name", "item.i_brand", "item.i_class", "item.i_category")
                .avg("j2.inv_quantity_on_hand", "qoh")
                .alias("i_product_name", "i_brand", "i_class", "i_category")
                .from(j3, "j3").build();

        System.out.println("RESULTS:");
        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() -t;
        printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res4");
        tm.stopServers();
        broker.shutdown();
        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q4_3timings");

    }
    @Test
    public void Q4_4Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        for (String tableName : tablesToLoad) {
            int index = TPC_DS_Inv.names.indexOf(tableName);
            int shardNum = Math.min(Math.max(TPC_DS_Inv.sizes.get(index) / 1000, 1), Broker.SHARDS_PER_TABLE);
            api.createTable(tableName)
                    .attributes(TPC_DS_Inv.schemas.get(index).toArray(new String[0]))
                    .keys(TPC_DS_Inv.keys.get(index).toArray(new String[0]))
                    .shardNumber(shardNum)
                    .build().run();
        }
        ReadQuery j1 = api.join()
                .select("inventory.inv_quantity_on_hand", "inventory.inv_item_sk", "inventory.inv_warehouse_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk", "inv_warehouse_sk")
                .sources("inventory", "date_dim", List.of("inv_date_sk"), List.of("d_date_sk"))
                .filters("", "d_month_seq >= 1205 && d_month_seq <= 1216").build();

        ReadQuery j2 = api.join()
                .select("j1p.inv_quantity_on_hand", "j1p.inv_item_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk")
                .sources(j1, "warehouse", "j1p", List.of("inv_warehouse_sk"), List.of("w_warehouse_sk")).build();

        ReadQuery j3 = api.join().sources(j2, "item", "j2", List.of("inv_item_sk"), List.of("i_item_sk")).build();

        ReadQuery finalQuery = api.read()
                .select("item.i_product_name", "item.i_brand", "item.i_class", "item.i_category")
                .avg("j2.inv_quantity_on_hand", "qoh")
                .alias("i_product_name", "i_brand", "i_class", "i_category")
                .from(j3, "j3").store().build();

        finalQuery.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);
        System.out.println("RESULTS:");
        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() -t;
        tm.stopServers();
        broker.shutdown();
        printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res4");

        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q4_4timings");

    }

}
