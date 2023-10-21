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

import static edu.stanford.futuredata.uniserve.rel.TPC_DS.TestMethods.zkHost;
import static edu.stanford.futuredata.uniserve.rel.TPC_DS.TestMethods.zkPort;

public class Q6Test {
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
query 26 of the TPC DS benchmark
SELECT                             q.i_item_id,
               Avg(cs_quantity)    q.agg1-t.agg1,
               Avg(cs_list_price)  q.agg2-t.agg2,
               Avg(cs_coupon_amt)  q.agg3-t.agg3,
               Avg(cs_sales_price) q.agg4-t.agg4
FROM   catalog_sales,
       customer_demographics,
       date_dim,
       item,
       promotion
WHERE  cs_sold_date_sk = d_date_sk
       AND cs_item_sk = i_item_sk
       AND cs_bill_cdemo_sk = cd_demo_sk
       AND cs_promo_sk = p_promo_sk
       AND cd_gender = 'F'
       AND cd_marital_status = 'W'
       AND cd_education_status = 'Secondary'
       AND ( p_channel_email = 'N'
              OR p_channel_event = 'N' )
       AND d_year = 2000
GROUP  BY i_item_id
* */

    @Test
    public void Q6Test() throws IOException {
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

        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join()
                .select("catalog_sales.cs_sold_date_sk", "catalog_sales.cs_promo_sk", "catalog_sales.cs_quantity", "catalog_sales.cs_list_price", "catalog_sales.cs_coupon_amt", "catalog_sales.cs_sales_price", "catalog_sales.cs_item_sk")
                .alias("cs_sold_date_sk", "cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources("catalog_sales", "customer_demographics", List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'Secondary'")
                .build();

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join()
                .select("q1.cs_promo_sk", "q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 2000").build();

        ReadQuery csToMWSEdInPromotion = api.join()
                .select("q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "q1", List.of("cs_promo_sk"), List.of("p_promo_sk"))
                .filters("", "p_channel_email == 'N' || p_channel_event == 'N'")
                .build();

        ReadQuery finalJoin = api.join().select("item.i_item_id", "cs.cs_quantity", "cs.cs_list_price", "cs.cs_coupon_amt", "cs.cs_sales_price")
                .alias("i_item_id", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price")
                .sources(csToMWSEdInPromotion, "item", "cs", List.of("cs_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .build();

        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis()-t;

        broker.shutdown();
        tm.stopServers();
        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res");

        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q6timings");
    }

    @Test
    public void Q6_1Test() throws IOException {
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

        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join()
                .select("catalog_sales.cs_sold_date_sk", "catalog_sales.cs_promo_sk", "catalog_sales.cs_quantity", "catalog_sales.cs_list_price", "catalog_sales.cs_coupon_amt", "catalog_sales.cs_sales_price", "catalog_sales.cs_item_sk")
                .alias("cs_sold_date_sk", "cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources("catalog_sales", "customer_demographics", List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'Secondary'")
                .stored().build();

        catSalesToMarriedWomenWithSecondaryEd.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join()
                .select("q1.cs_promo_sk", "q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 2000").build();

        ReadQuery csToMWSEdInPromotion = api.join()
                .select("q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "q1", List.of("cs_promo_sk"), List.of("p_promo_sk"))
                .filters("", "p_channel_email == 'N' || p_channel_event == 'N'")
                .build();

        ReadQuery finalJoin = api.join().select("item.i_item_id", "cs.cs_quantity", "cs.cs_list_price", "cs.cs_coupon_amt", "cs.cs_sales_price")
                .alias("i_item_id", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price")
                .sources(csToMWSEdInPromotion, "item", "cs", List.of("cs_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .build();

        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis()-t;

        broker.shutdown();
        tm.stopServers();
        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res6");

        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q6_1timings");
    }
    @Test
    public void Q6_2Test() throws IOException {
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

        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join()
                .select("catalog_sales.cs_sold_date_sk", "catalog_sales.cs_promo_sk", "catalog_sales.cs_quantity", "catalog_sales.cs_list_price", "catalog_sales.cs_coupon_amt", "catalog_sales.cs_sales_price", "catalog_sales.cs_item_sk")
                .alias("cs_sold_date_sk", "cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources("catalog_sales", "customer_demographics", List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'Secondary'")
                .build();

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join()
                .select("q1.cs_promo_sk", "q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 2000").stored().build();


        catSalesToMarriedWomenWithSecondaryEdIn2000.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery csToMWSEdInPromotion = api.join()
                .select("q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "q1", List.of("cs_promo_sk"), List.of("p_promo_sk"))
                .filters("", "p_channel_email == 'N' || p_channel_event == 'N'")
                .build();

        ReadQuery finalJoin = api.join().select("item.i_item_id", "cs.cs_quantity", "cs.cs_list_price", "cs.cs_coupon_amt", "cs.cs_sales_price")
                .alias("i_item_id", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price")
                .sources(csToMWSEdInPromotion, "item", "cs", List.of("cs_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .build();

        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis()-t;

        broker.shutdown();
        tm.stopServers();
        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res");

        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q6_2timings");
    }
    @Test
    public void Q6_3Test() throws IOException {
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

        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join()
                .select("catalog_sales.cs_sold_date_sk", "catalog_sales.cs_promo_sk", "catalog_sales.cs_quantity", "catalog_sales.cs_list_price", "catalog_sales.cs_coupon_amt", "catalog_sales.cs_sales_price", "catalog_sales.cs_item_sk")
                .alias("cs_sold_date_sk", "cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources("catalog_sales", "customer_demographics", List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'Secondary'")
                .build();

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join()
                .select("q1.cs_promo_sk", "q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 2000").build();

        ReadQuery csToMWSEdInPromotion = api.join()
                .select("q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "q1", List.of("cs_promo_sk"), List.of("p_promo_sk"))
                .filters("", "p_channel_email == 'N' || p_channel_event == 'N'")
                .stored().build();

        csToMWSEdInPromotion.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery finalJoin = api.join().select("item.i_item_id", "cs.cs_quantity", "cs.cs_list_price", "cs.cs_coupon_amt", "cs.cs_sales_price")
                .alias("i_item_id", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price")
                .sources(csToMWSEdInPromotion, "item", "cs", List.of("cs_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .build();

        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis()-t;

        broker.shutdown();
        tm.stopServers();
        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res");

        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q6_3timings");
    }
    @Test
    public void Q6_4Test() throws IOException {
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

        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join()
                .select("catalog_sales.cs_sold_date_sk", "catalog_sales.cs_promo_sk", "catalog_sales.cs_quantity", "catalog_sales.cs_list_price", "catalog_sales.cs_coupon_amt", "catalog_sales.cs_sales_price", "catalog_sales.cs_item_sk")
                .alias("cs_sold_date_sk", "cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources("catalog_sales", "customer_demographics", List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'Secondary'")
                .build();

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join()
                .select("q1.cs_promo_sk", "q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 2000").build();

        ReadQuery csToMWSEdInPromotion = api.join()
                .select("q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "q1", List.of("cs_promo_sk"), List.of("p_promo_sk"))
                .filters("", "p_channel_email == 'N' || p_channel_event == 'N'")
                .build();

        ReadQuery finalJoin = api.join().select("item.i_item_id", "cs.cs_quantity", "cs.cs_list_price", "cs.cs_coupon_amt", "cs.cs_sales_price")
                .alias("i_item_id", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price")
                .sources(csToMWSEdInPromotion, "item", "cs", List.of("cs_item_sk"), List.of("i_item_sk"))
                .stored().build();

        finalJoin.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .build();

        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis()-t;

        broker.shutdown();
        tm.stopServers();
        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res");

        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q6_4timings");
    }
    @Test
    public void Q6_5Test() throws IOException {
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

        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join()
                .select("catalog_sales.cs_sold_date_sk", "catalog_sales.cs_promo_sk", "catalog_sales.cs_quantity", "catalog_sales.cs_list_price", "catalog_sales.cs_coupon_amt", "catalog_sales.cs_sales_price", "catalog_sales.cs_item_sk")
                .alias("cs_sold_date_sk", "cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources("catalog_sales", "customer_demographics", List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'Secondary'")
                .build();

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join()
                .select("q1.cs_promo_sk", "q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 2000").build();

        ReadQuery csToMWSEdInPromotion = api.join()
                .select("q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "q1", List.of("cs_promo_sk"), List.of("p_promo_sk"))
                .filters("", "p_channel_email == 'N' || p_channel_event == 'N'")
                .build();

        ReadQuery finalJoin = api.join().select("item.i_item_id", "cs.cs_quantity", "cs.cs_list_price", "cs.cs_coupon_amt", "cs.cs_sales_price")
                .alias("i_item_id", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price")
                .sources(csToMWSEdInPromotion, "item", "cs", List.of("cs_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .store().build();

        finalQuery.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        long t = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis()-t;

        broker.shutdown();
        tm.stopServers();
        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res");

        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q6_5timings");
    }

    public List<String> tablesToLoad = List.of(
            "catalog_sales",
            "customer_demographics",
            "date_dim",
            "item",
            "promotion"
    );
}
