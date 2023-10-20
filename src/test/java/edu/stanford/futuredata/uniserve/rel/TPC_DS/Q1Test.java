package edu.stanford.futuredata.uniserve.rel.TPC_DS;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.rel.TPC_DS.TPC_DS_Inv;
import edu.stanford.futuredata.uniserve.rel.TPC_DS.TestMethods;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relationalapi.API;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class Q1Test {
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
    Query 13 of the TPS DC benchmark:

    SELECT Avg(ss_quantity),
       Avg(ss_ext_sales_price),
       Avg(ss_ext_wholesale_cost),
       Sum(ss_ext_wholesale_cost)
FROM   store_sales,
       store,
       customer_demographics,
       household_demographics,
       customer_address,
       date_dim
WHERE  s_store_sk = ss_store_sk
       AND ss_sold_date_sk = d_date_sk
       AND d_year = 2001
       AND (   ( ss_hdemo_sk = hd_demo_sk
                  AND cd_demo_sk = ss_cdemo_sk
                  AND cd_marital_status = 'U'
                  AND cd_education_status = 'Advanced Degree'
                  AND ss_sales_price BETWEEN 100.00 AND 150.00
                  AND hd_dep_count = 3 )
            OR ( ss_hdemo_sk = hd_demo_sk
                 AND cd_demo_sk = ss_cdemo_sk
                 AND cd_marital_status = 'M'
                 AND cd_education_status = 'Primary'
                 AND ss_sales_price BETWEEN 50.00 AND 100.00
                 AND hd_dep_count = 1 )
            OR ( ss_hdemo_sk = hd_demo_sk
                 AND cd_demo_sk = ss_cdemo_sk
                 AND cd_marital_status = 'D'
                 AND cd_education_status = 'Secondary'
                 AND ss_sales_price BETWEEN 150.00 AND 200.00
                 AND hd_dep_count = 1 ) )
       AND (   ( ss_addr_sk = ca_address_sk
                 AND ca_country = 'United States'
                 AND ca_state IN ( 'AZ', 'NE', 'IA' )
                 AND ss_net_profit BETWEEN 100 AND 200 )
            OR ( ss_addr_sk = ca_address_sk
                 AND ca_country = 'United States'
                 AND ca_state IN ( 'MS', 'CA', 'NV' )
                 AND ss_net_profit BETWEEN 150 AND 300 )
            OR ( ss_addr_sk = ca_address_sk
                 AND ca_country = 'United States'
                 AND ca_state IN ( 'GA', 'TX', 'NJ' )
                 AND ss_net_profit BETWEEN 50 AND 250 )
                 );
         */



    @Test
    public void Q1Test() throws IOException {
        for(int i = 0; i<5; i++) {
            startUpCleanUp();
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

            List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
            List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
            List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
            List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
            List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
            List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
            List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
            List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
            List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
            List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");

            List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

            ReadQuery j1 = api.join()
                    .select(j1RawSchema.toArray(new String[0]))
                    .alias(j1Schema.toArray(new String[0]))
                    .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                    .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                            " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                    .build();


            ReadQuery j2 = api.join()
                    .select(j2RawSchema.toArray(new String[0]))
                    .alias(j2Schema.toArray(new String[0]))
                    .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                    .build();

            ReadQuery j3 = api.join()
                    .select(j3RawSchema.toArray(new String[0]))
                    .alias(j3Schema.toArray(new String[0]))
                    .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                    .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                            "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                            "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                    .build();

            ReadQuery j4 = api.join()
                    .select(j4RawSchema.toArray(new String[0]))
                    .alias(j4Schema.toArray(new String[0]))
                    .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                            "hd_dep_count == 1 || hd_dep_count == 3")
                    .build();

            ReadQuery j5 = api.join()
                    .select(j5RawSchema.toArray(new String[0]))
                    .alias(j5Schema.toArray(new String[0]))
                    .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                            "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                    .build();

            ReadQuery q1 = api.read()
                    .avg("ss_quantity", "avg_ss_quantity")
                    .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                    .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                    .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                    .fromFilter(j5, "j5",
                            "(" +
                                    "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                    " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                    " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                    ") && (" +
                                    "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                    "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                    "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                    ")")
                    .build();

            long readStartTime = System.currentTimeMillis();
            RelReadQueryResults results = q1.run(broker);
            long elapsedTime = System.currentTimeMillis() - readStartTime;
            broker.shutdown();
            tm.stopServers();
            System.out.println("\n\nResults:\n");
            TestMethods.printRowList(results.getData());
            tm.printOnFile(results.getData(), results.getFieldNames(), "res1");

            System.out.println("Table writing times, with shard creation: ");
            for (Pair<String, Long> time : loadTimes) {
                System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
            }
            System.out.println("Read time: " + elapsedTime);
            tm.saveTimings(loadTimes, elapsedTime, "Q1timings");

            tm.stopServers();
            broker.shutdown();
            unitTestCleanUp();
        }
    }
    @Test
    public void Q1_1Test() throws IOException{
        for(int i = 0; i<5; i++) {
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

            List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
            List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
            List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
            List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
            List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
            List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
            List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
            List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
            List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
            List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");

            ReadQuery j1 = api.join()
                    .select(j1RawSchema.toArray(new String[0]))
                    .alias(j1Schema.toArray(new String[0]))
                    .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                    .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                            " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                    .stored()
                    .build();

            j1.run(broker);
            List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

            ReadQuery j2 = api.join()
                    .select(j2RawSchema.toArray(new String[0]))
                    .alias(j2Schema.toArray(new String[0]))
                    .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                    .build();

            ReadQuery j3 = api.join()
                    .select(j3RawSchema.toArray(new String[0]))
                    .alias(j3Schema.toArray(new String[0]))
                    .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                    .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                            "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                            "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                    .build();

            ReadQuery j4 = api.join()
                    .select(j4RawSchema.toArray(new String[0]))
                    .alias(j4Schema.toArray(new String[0]))
                    .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                            "hd_dep_count == 1 || hd_dep_count == 3")
                    .build();

            ReadQuery j5 = api.join()
                    .select(j5RawSchema.toArray(new String[0]))
                    .alias(j5Schema.toArray(new String[0]))
                    .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                            "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                    .build();

            ReadQuery q1 = api.read()
                    .avg("ss_quantity", "avg_ss_quantity")
                    .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                    .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                    .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                    .fromFilter(j5, "j5",
                            "(" +
                                    "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                    " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                    " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                    ") && (" +
                                    "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                    "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                    "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                    ")")
                    .build();

            long readStartTime = System.currentTimeMillis();
            RelReadQueryResults results = q1.run(broker);
            long elapsedTime = System.currentTimeMillis() - readStartTime;
            broker.shutdown();
            tm.stopServers();
            System.out.println("\n\nResults:\n");
            TestMethods.printRowList(results.getData());
            tm.printOnFile(results.getData(), results.getFieldNames(), "res1_1");

            System.out.println("Table writing times, with shard creation: ");
            for (Pair<String, Long> time : loadTimes) {
                System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
            }
            System.out.println("Read time: " + elapsedTime);
            tm.saveTimings(loadTimes, elapsedTime, "Q1_1timings");
            tm.stopServers();
            broker.shutdown();
            unitTestCleanUp();
        }
    }
    @Test
    public void Q1_2Test() throws IOException{
        for(int i = 0; i<5; i++) {
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

            List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
            List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
            List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
            List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
            List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
            List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
            List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
            List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
            List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
            List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");

            ReadQuery j1 = api.join()
                    .select(j1RawSchema.toArray(new String[0]))
                    .alias(j1Schema.toArray(new String[0]))
                    .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                    .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                            " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                    .build();


            ReadQuery j2 = api.join()
                    .select(j2RawSchema.toArray(new String[0]))
                    .alias(j2Schema.toArray(new String[0]))
                    .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                    .stored()
                    .build();

            j2.run(broker);
            List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

            ReadQuery j3 = api.join()
                    .select(j3RawSchema.toArray(new String[0]))
                    .alias(j3Schema.toArray(new String[0]))
                    .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                    .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                            "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                            "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                    .build();

            ReadQuery j4 = api.join()
                    .select(j4RawSchema.toArray(new String[0]))
                    .alias(j4Schema.toArray(new String[0]))
                    .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                            "hd_dep_count == 1 || hd_dep_count == 3")
                    .build();

            ReadQuery j5 = api.join()
                    .select(j5RawSchema.toArray(new String[0]))
                    .alias(j5Schema.toArray(new String[0]))
                    .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                            "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                    .build();

            ReadQuery q1 = api.read()
                    .avg("ss_quantity", "avg_ss_quantity")
                    .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                    .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                    .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                    .fromFilter(j5, "j5",
                            "(" +
                                    "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                    " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                    " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                    ") && (" +
                                    "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                    "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                    "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                    ")")
                    .build();

            long readStartTime = System.currentTimeMillis();
            RelReadQueryResults results = q1.run(broker);
            long elapsedTime = System.currentTimeMillis() - readStartTime;
            broker.shutdown();
            tm.stopServers();
            System.out.println("\n\nResults:\n");
            TestMethods.printRowList(results.getData());
            tm.printOnFile(results.getData(), results.getFieldNames(), "res1_2");

            System.out.println("Table writing times, with shard creation: ");
            for (Pair<String, Long> time : loadTimes) {
                System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
            }
            System.out.println("Read time: " + elapsedTime);
            tm.saveTimings(loadTimes, elapsedTime, "Q1_2timings");
            tm.stopServers();
            broker.shutdown();
            unitTestCleanUp();
        }
    }
    @Test
    public void Q1_3Test() throws IOException{
        for(int i = 0; i<5; i++) {
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

            List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
            List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
            List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
            List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
            List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
            List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
            List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
            List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
            List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
            List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");

            ReadQuery j1 = api.join()
                    .select(j1RawSchema.toArray(new String[0]))
                    .alias(j1Schema.toArray(new String[0]))
                    .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                    .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                            " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                    .build();

            ReadQuery j2 = api.join()
                    .select(j2RawSchema.toArray(new String[0]))
                    .alias(j2Schema.toArray(new String[0]))
                    .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                    .build();

            ReadQuery j3 = api.join()
                    .select(j3RawSchema.toArray(new String[0]))
                    .alias(j3Schema.toArray(new String[0]))
                    .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                    .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                            "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                            "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                    .stored()
                    .build();

            j3.run(broker);
            List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

            ReadQuery j4 = api.join()
                    .select(j4RawSchema.toArray(new String[0]))
                    .alias(j4Schema.toArray(new String[0]))
                    .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                            "hd_dep_count == 1 || hd_dep_count == 3")
                    .build();

            ReadQuery j5 = api.join()
                    .select(j5RawSchema.toArray(new String[0]))
                    .alias(j5Schema.toArray(new String[0]))
                    .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                            "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                    .build();

            ReadQuery q1 = api.read()
                    .avg("ss_quantity", "avg_ss_quantity")
                    .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                    .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                    .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                    .fromFilter(j5, "j5",
                            "(" +
                                    "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                    " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                    " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                    ") && (" +
                                    "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                    "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                    "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                    ")")
                    .build();

            long readStartTime = System.currentTimeMillis();
            RelReadQueryResults results = q1.run(broker);
            long elapsedTime = System.currentTimeMillis() - readStartTime;
            broker.shutdown();
            tm.stopServers();
            System.out.println("\n\nResults:\n");
            TestMethods.printRowList(results.getData());
            tm.printOnFile(results.getData(), results.getFieldNames(), "res1_3");

            System.out.println("Table writing times, with shard creation: ");
            for (Pair<String, Long> time : loadTimes) {
                System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
            }
            System.out.println("Read time: " + elapsedTime);
            tm.saveTimings(loadTimes, elapsedTime, "Q1_3timings");
            tm.stopServers();
            broker.shutdown();
            unitTestCleanUp();
        }
    }
    @Test
    public void Q1_4Test() throws IOException{
        for(int i = 0; i<5; i++) {
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

            List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
            List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
            List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
            List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
            List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
            List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
            List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
            List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
            List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
            List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");

            ReadQuery j1 = api.join()
                    .select(j1RawSchema.toArray(new String[0]))
                    .alias(j1Schema.toArray(new String[0]))
                    .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                    .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                            " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                    .build();

            ReadQuery j2 = api.join()
                    .select(j2RawSchema.toArray(new String[0]))
                    .alias(j2Schema.toArray(new String[0]))
                    .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                    .build();

            ReadQuery j3 = api.join()
                    .select(j3RawSchema.toArray(new String[0]))
                    .alias(j3Schema.toArray(new String[0]))
                    .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                    .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                            "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                            "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                    .build();

            ReadQuery j4 = api.join()
                    .select(j4RawSchema.toArray(new String[0]))
                    .alias(j4Schema.toArray(new String[0]))
                    .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                            "hd_dep_count == 1 || hd_dep_count == 3")
                    .stored()
                    .build();

            j4.run(broker);
            List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

            ReadQuery j5 = api.join()
                    .select(j5RawSchema.toArray(new String[0]))
                    .alias(j5Schema.toArray(new String[0]))
                    .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                            "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                    .build();

            ReadQuery q1 = api.read()
                    .avg("ss_quantity", "avg_ss_quantity")
                    .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                    .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                    .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                    .fromFilter(j5, "j5",
                            "(" +
                                    "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                    " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                    " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                    ") && (" +
                                    "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                    "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                    "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                    ")")
                    .build();

            long readStartTime = System.currentTimeMillis();
            RelReadQueryResults results = q1.run(broker);
            long elapsedTime = System.currentTimeMillis() - readStartTime;
            broker.shutdown();
            tm.stopServers();
            System.out.println("\n\nResults:\n");
            TestMethods.printRowList(results.getData());
            tm.printOnFile(results.getData(), results.getFieldNames(), "res1_4");

            System.out.println("Table writing times, with shard creation: ");
            for (Pair<String, Long> time : loadTimes) {
                System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
            }
            System.out.println("Read time: " + elapsedTime);
            tm.saveTimings(loadTimes, elapsedTime, "Q1_4timings");
            tm.stopServers();
            broker.shutdown();
            unitTestCleanUp();
        }
    }
    @Test
    public void Q1_5Test() throws IOException{
        for(int i = 0; i<5; i++) {
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

            List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
            List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
            List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
            List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
            List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
            List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
            List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
            List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
            List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
            List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");

            ReadQuery j1 = api.join()
                    .select(j1RawSchema.toArray(new String[0]))
                    .alias(j1Schema.toArray(new String[0]))
                    .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                    .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                            " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                    .build();

            ReadQuery j2 = api.join()
                    .select(j2RawSchema.toArray(new String[0]))
                    .alias(j2Schema.toArray(new String[0]))
                    .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                    .build();

            ReadQuery j3 = api.join()
                    .select(j3RawSchema.toArray(new String[0]))
                    .alias(j3Schema.toArray(new String[0]))
                    .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                    .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                            "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                            "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                    .build();

            ReadQuery j4 = api.join()
                    .select(j4RawSchema.toArray(new String[0]))
                    .alias(j4Schema.toArray(new String[0]))
                    .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                            "hd_dep_count == 1 || hd_dep_count == 3")
                    .build();

            ReadQuery j5 = api.join()
                    .select(j5RawSchema.toArray(new String[0]))
                    .alias(j5Schema.toArray(new String[0]))
                    .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                            "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                    .stored()
                    .build();

            j5.run(broker);
            List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

            ReadQuery q1 = api.read()
                    .avg("ss_quantity", "avg_ss_quantity")
                    .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                    .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                    .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                    .fromFilter(j5, "j5",
                            "(" +
                                    "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                    " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                    " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                    ") && (" +
                                    "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                    "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                    "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                    ")")
                    .build();

            long readStartTime = System.currentTimeMillis();
            RelReadQueryResults results = q1.run(broker);
            long elapsedTime = System.currentTimeMillis() - readStartTime;
            broker.shutdown();
            tm.stopServers();
            System.out.println("\n\nResults:\n");
            TestMethods.printRowList(results.getData());
            tm.printOnFile(results.getData(), results.getFieldNames(), "res1_5");

            System.out.println("Table writing times, with shard creation: ");
            for (Pair<String, Long> time : loadTimes) {
                System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
            }
            System.out.println("Read time: " + elapsedTime);
            tm.saveTimings(loadTimes, elapsedTime, "Q1_5timings");
            tm.stopServers();
            broker.shutdown();
            unitTestCleanUp();
        }
    }
    @Test
    public void Q1_6Test() throws IOException{
        for(int i = 0; i<5; i++) {
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

            List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
            List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
            List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
            List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
            List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
            List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
            List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
            List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
            List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
            List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");

            ReadQuery j1 = api.join()
                    .select(j1RawSchema.toArray(new String[0]))
                    .alias(j1Schema.toArray(new String[0]))
                    .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                    .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                            " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                    .build();

            ReadQuery j2 = api.join()
                    .select(j2RawSchema.toArray(new String[0]))
                    .alias(j2Schema.toArray(new String[0]))
                    .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                    .build();

            ReadQuery j3 = api.join()
                    .select(j3RawSchema.toArray(new String[0]))
                    .alias(j3Schema.toArray(new String[0]))
                    .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                    .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                            "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                            "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                    .build();

            ReadQuery j4 = api.join()
                    .select(j4RawSchema.toArray(new String[0]))
                    .alias(j4Schema.toArray(new String[0]))
                    .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                            "hd_dep_count == 1 || hd_dep_count == 3")
                    .build();

            ReadQuery j5 = api.join()
                    .select(j5RawSchema.toArray(new String[0]))
                    .alias(j5Schema.toArray(new String[0]))
                    .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                    .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                    "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                    "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                            "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                    .build();

            ReadQuery q1 = api.read()
                    .avg("ss_quantity", "avg_ss_quantity")
                    .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                    .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                    .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                    .fromFilter(j5, "j5",
                            "(" +
                                    "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                    " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                    " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                    ") && (" +
                                    "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                    "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                    "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                    ")")
                    .store()
                    .build();

            q1.run(broker);
            List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

            long readStartTime = System.currentTimeMillis();
            RelReadQueryResults results = q1.run(broker);
            long elapsedTime = System.currentTimeMillis() - readStartTime;
            broker.shutdown();
            tm.stopServers();
            System.out.println("\n\nResults:\n");
            TestMethods.printRowList(results.getData());
            tm.printOnFile(results.getData(), results.getFieldNames(), "res1_6");

            System.out.println("Table writing times, with shard creation: ");
            for (Pair<String, Long> time : loadTimes) {
                System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
            }
            System.out.println("Read time: " + elapsedTime);
            tm.saveTimings(loadTimes, elapsedTime, "Q1_6timings");
            tm.stopServers();
            broker.shutdown();
            unitTestCleanUp();
        }
    }

    public List<String> tablesToLoad = List.of(
            "store_sales",
            "date_dim",
            "store",
            "customer_demographics",
            "household_demographics",
            "customer_address"
    );
}