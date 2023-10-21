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

public class Q9Test {

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


    /*
    * query 42 of the TPC DS benchmark
SELECT dt.d_year,
       item.i_category_id,
       item.i_category,
       Sum(ss_ext_sales_price)
FROM   date_dim dt,
       store_sales,
       item
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk
       AND store_sales.ss_item_sk = item.i_item_sk
       AND item.i_manager_id = 1
       AND dt.d_moy = 12
       AND dt.d_year = 2000
GROUP  BY dt.d_year,
          item.i_category_id,
          item.i_category
ORDER  BY Sum(ss_ext_sales_price) DESC,
          dt.d_year,
          item.i_category_id,
          item.i_category


        q.d_year = t.d_year and,
       q.i_category_id = t.i_category_id and
       q.i_category = t.i_category and
       q.ss_ext_sales_price = t.ss_ext_sales_price and
* */
    @Test
    public void Q9Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(TestMethods.zkHost, TestMethods.zkPort);
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

        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery storeSalesInDec2000 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_ext_sales_price", "date_dim.d_year")
                .alias("ss_item_sk", "ss_ext_sales_price", "d_year")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 12 && d_year == 2000").build();

        ReadQuery ssInDec2000FromManager1 = api.join()
                .select("item.i_category_id", "item.i_category", "j1.ss_ext_sales_price", "j1.d_year")
                .sources(storeSalesInDec2000, "item", "j1", List.of("ss_item_sk"), List.of("i_item_sk"))
                .filters("", "i_manager_id == 1").build();

        ReadQuery finalQuery = api.read()
                .select("j1.d_year", "item.i_category_id", "item.i_category")
                .alias("d_year", "i_category_id", "i_category")
                .sum("j1.ss_ext_sales_price", "sum_ss_ext_sales_price")
                .from(ssInDec2000FromManager1, "src").build();


        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis()-t;

        broker.shutdown();
        tm.stopServers();
        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res9");

        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q9timings");
    }

    @Test
    public void Q9_1Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(TestMethods.zkHost, TestMethods.zkPort);
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

        ReadQuery storeSalesInDec2000 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_ext_sales_price", "date_dim.d_year")
                .alias("ss_item_sk", "ss_ext_sales_price", "d_year")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 12 && d_year == 2000").stored().build();

        storeSalesInDec2000.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery ssInDec2000FromManager1 = api.join()
                .select("item.i_category_id", "item.i_category", "j1.ss_ext_sales_price", "j1.d_year")
                .sources(storeSalesInDec2000, "item", "j1", List.of("ss_item_sk"), List.of("i_item_sk"))
                .filters("", "i_manager_id == 1").build();

        ReadQuery finalQuery = api.read()
                .select("j1.d_year", "item.i_category_id", "item.i_category")
                .alias("d_year", "i_category_id", "i_category")
                .sum("j1.ss_ext_sales_price", "sum_ss_ext_sales_price")
                .from(ssInDec2000FromManager1, "src").build();


        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis()-t;

        broker.shutdown();
        tm.stopServers();
        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res9");

        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q9_1timings");
    }
    @Test
    public void Q9_2Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(TestMethods.zkHost, TestMethods.zkPort);
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

        ReadQuery storeSalesInDec2000 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_ext_sales_price", "date_dim.d_year")
                .alias("ss_item_sk", "ss_ext_sales_price", "d_year")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 12 && d_year == 2000").build();

        ReadQuery ssInDec2000FromManager1 = api.join()
                .select("item.i_category_id", "item.i_category", "j1.ss_ext_sales_price", "j1.d_year")
                .sources(storeSalesInDec2000, "item", "j1", List.of("ss_item_sk"), List.of("i_item_sk"))
                .filters("", "i_manager_id == 1").stored().build();

        ssInDec2000FromManager1.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery finalQuery = api.read()
                .select("j1.d_year", "item.i_category_id", "item.i_category")
                .alias("d_year", "i_category_id", "i_category")
                .sum("j1.ss_ext_sales_price", "sum_ss_ext_sales_price")
                .from(ssInDec2000FromManager1, "src").build();


        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis()-t;

        broker.shutdown();
        tm.stopServers();
        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res9");

        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q9_2timings");
    }
    @Test
    public void Q9_3Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(TestMethods.zkHost, TestMethods.zkPort);
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

        ReadQuery storeSalesInDec2000 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_ext_sales_price", "date_dim.d_year")
                .alias("ss_item_sk", "ss_ext_sales_price", "d_year")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 12 && d_year == 2000").build();

        ReadQuery ssInDec2000FromManager1 = api.join()
                .select("item.i_category_id", "item.i_category", "j1.ss_ext_sales_price", "j1.d_year")
                .sources(storeSalesInDec2000, "item", "j1", List.of("ss_item_sk"), List.of("i_item_sk"))
                .filters("", "i_manager_id == 1").build();

        ReadQuery finalQuery = api.read()
                .select("j1.d_year", "item.i_category_id", "item.i_category")
                .alias("d_year", "i_category_id", "i_category")
                .sum("j1.ss_ext_sales_price", "sum_ss_ext_sales_price")
                .from(ssInDec2000FromManager1, "src").store().build();

        finalQuery.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis()-t;

        broker.shutdown();
        tm.stopServers();
        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res9");

        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q9_3timings");
    }

    public List<String> tablesToLoad = List.of(
            "store_sales","date_dim","item"
    );

}
