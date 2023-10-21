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

public class Q2Test {
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
    Query 15 of the TPS DC benchmark
    SELECT ca_zip, Sum(cs_sales_price)
    FROM   catalog_sales,
            customer,
            customer_address,
            date_dim
    WHERE  cs_bill_customer_sk = c_customer_sk
            AND c_current_addr_sk = ca_address_sk
            AND (Substr(ca_zip, 1, 5) IN ( '85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792' )
                OR ca_state IN ( 'CA', 'WA', 'GA' )
                OR cs_sales_price > 500)
            AND cs_sold_date_sk = d_date_sk
            AND d_qoy = 1
            AND d_year = 1998
    GROUP  BY ca_zip
*/

    @Test
    public void Q2Test() throws IOException {
        TestMethods tm = new TestMethods(); tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);

        for (String tableName : tablesToLoad) {
            int index = TPC_DS_Inv.names.indexOf(tableName);
            int shardNum = Math.min(Math.max(TPC_DS_Inv.sizes.get(index) / 10000, 1), Broker.SHARDS_PER_TABLE);
            api.createTable(tableName)
                    .attributes(TPC_DS_Inv.schemas.get(index).toArray(new String[0]))
                    .keys(TPC_DS_Inv.keys.get(index).toArray(new String[0]))
                    .shardNumber(shardNum)
                    .build().run();
        }

        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

       ReadQuery catalogSalesInFirstQ98 = api.join()
               .select("catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_sales_price")
               .alias("cs_bill_customer_sk", "cs_sales_price")
               .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
               .filters("", "d_qoy == 1 && d_year == 1998").build();

       ReadQuery customerAndAddresses = api.join()
               .select("customer_address.ca_state", "customer_address.ca_zip", "customer.c_customer_sk")
               .alias("ca_state", "ca_zip", "c_customer_sk")
               .sources("customer", "customer_address", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
               .build();

       ReadQuery finalJoin = api.join().select("j2.ca_zip", "j1.cs_sales_price", "j2.ca_state")
               .alias("ca_zip", "cs_sales_price", "ca_state")
               .sources(catalogSalesInFirstQ98, customerAndAddresses, "j1", "j2",List.of("cs_bill_customer_sk"), List.of("c_customer_sk"))
               .build();

       String filterPredicate = "" +
               " substringQUERY = def (x) { x.substring(0,5) }; "
                + "['85669', '86197', '88274', '83405','86475', '85392', '85460', '80348', '81792'].contains( substringQUERY(ca_zip) )" +
               " || cs_sales_price > 500 "
               + "||  ['CA', 'WA', 'GA'].contains(ca_state)"
               ;

       ReadQuery res = api.read().select("ca_zip").sum("cs_sales_price", "sum_prices")
               .fromFilter(finalJoin, "fJ", filterPredicate).build();
       long t = System.currentTimeMillis();
       RelReadQueryResults results = res.run(broker);
       long elapsedTime = System.currentTimeMillis() -t;
       broker.shutdown();
        tm.stopServers();
        System.out.println("\n\nResults:\n");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res2");

        System.out.println("Table writing times, with shard creation: ");
        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q2timings");
        tm.stopServers();
        broker.shutdown();
    }
    @Test
    public void Q2_1Test() throws IOException {
        TestMethods tm = new TestMethods(); tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);

        for (String tableName : tablesToLoad) {
            int index = TPC_DS_Inv.names.indexOf(tableName);
            int shardNum = Math.min(Math.max(TPC_DS_Inv.sizes.get(index) / 10000, 1), Broker.SHARDS_PER_TABLE);
            api.createTable(tableName)
                    .attributes(TPC_DS_Inv.schemas.get(index).toArray(new String[0]))
                    .keys(TPC_DS_Inv.keys.get(index).toArray(new String[0]))
                    .shardNumber(shardNum)
                    .build().run();
        }

        ReadQuery catalogSalesInFirstQ98 = api.join()
                .select("catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_sales_price")
                .alias("cs_bill_customer_sk", "cs_sales_price")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .stored()
                .filters("", "d_qoy == 1 && d_year == 1998").build();

        catalogSalesInFirstQ98.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery customerAndAddresses = api.join()
                .select("customer_address.ca_state", "customer_address.ca_zip", "customer.c_customer_sk")
                .alias("ca_state", "ca_zip", "c_customer_sk")
                .sources("customer", "customer_address", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();

        ReadQuery finalJoin = api.join().select("j2.ca_zip", "j1.cs_sales_price", "j2.ca_state")
                .alias("ca_zip", "cs_sales_price", "ca_state")
                .sources(catalogSalesInFirstQ98, customerAndAddresses, "j1", "j2",List.of("cs_bill_customer_sk"), List.of("c_customer_sk"))
                .build();

        String filterPredicate = "" +
                " substringQUERY = def (x) { x.substring(0,5) }; "
                + "['85669', '86197', '88274', '83405','86475', '85392', '85460', '80348', '81792'].contains( substringQUERY(ca_zip) )" +
                " || cs_sales_price > 500 "
                + "||  ['CA', 'WA', 'GA'].contains(ca_state)"
                ;

        ReadQuery res = api.read().select("ca_zip").sum("cs_sales_price", "sum_prices")
                .fromFilter(finalJoin, "fJ", filterPredicate).build();
        long t = System.currentTimeMillis();
        RelReadQueryResults results = res.run(broker);
        long elapsedTime = System.currentTimeMillis() -t;
        broker.shutdown();
        tm.stopServers();
        System.out.println("\n\nResults:\n");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res2");

        System.out.println("Table writing times, with shard creation: ");
        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q2_1timings");
        tm.stopServers();
        broker.shutdown();

    }
    @Test
    public void Q2_2Test() throws IOException {
        TestMethods tm = new TestMethods(); tm.startServers();
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

        ReadQuery catalogSalesInFirstQ98 = api.join()
                .select("catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_sales_price")
                .alias("cs_bill_customer_sk", "cs_sales_price")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_qoy == 1 && d_year == 1998").build();

        ReadQuery customerAndAddresses = api.join()
                .select("customer_address.ca_state", "customer_address.ca_zip", "customer.c_customer_sk")
                .alias("ca_state", "ca_zip", "c_customer_sk")
                .sources("customer", "customer_address", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .stored()
                .build();

        customerAndAddresses.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery finalJoin = api.join().select("j2.ca_zip", "j1.cs_sales_price", "j2.ca_state")
                .alias("ca_zip", "cs_sales_price", "ca_state")
                .sources(catalogSalesInFirstQ98, customerAndAddresses, "j1", "j2",List.of("cs_bill_customer_sk"), List.of("c_customer_sk"))
                .build();

        String filterPredicate = "" +
                " substringQUERY = def (x) { x.substring(0,5) }; "
                + "['85669', '86197', '88274', '83405','86475', '85392', '85460', '80348', '81792'].contains( substringQUERY(ca_zip) )" +
                " || cs_sales_price > 500 "
                + "||  ['CA', 'WA', 'GA'].contains(ca_state)"
                ;

        ReadQuery res = api.read().select("ca_zip").sum("cs_sales_price", "sum_prices")
                .fromFilter(finalJoin, "fJ", filterPredicate).build();
        long t = System.currentTimeMillis();
        RelReadQueryResults results = res.run(broker);
        long elapsedTime = System.currentTimeMillis() -t;
        broker.shutdown();
        tm.stopServers();
        System.out.println("\n\nResults:\n");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res2");

        System.out.println("Table writing times, with shard creation: ");
        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q2_2timings");
        tm.stopServers();
        broker.shutdown();

    }
    @Test
    public void Q2_3Test() throws IOException {
        TestMethods tm = new TestMethods(); tm.startServers();
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

        ReadQuery catalogSalesInFirstQ98 = api.join()
                .select("catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_sales_price")
                .alias("cs_bill_customer_sk", "cs_sales_price")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_qoy == 1 && d_year == 1998").build();

        ReadQuery customerAndAddresses = api.join()
                .select("customer_address.ca_state", "customer_address.ca_zip", "customer.c_customer_sk")
                .alias("ca_state", "ca_zip", "c_customer_sk")
                .sources("customer", "customer_address", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();

        ReadQuery finalJoin = api.join().select("j2.ca_zip", "j1.cs_sales_price", "j2.ca_state")
                .alias("ca_zip", "cs_sales_price", "ca_state")
                .sources(catalogSalesInFirstQ98, customerAndAddresses, "j1", "j2",List.of("cs_bill_customer_sk"), List.of("c_customer_sk"))
                .stored()
                .build();

        finalJoin.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        String filterPredicate = "" +
                " substringQUERY = def (x) { x.substring(0,5) }; "
                + "['85669', '86197', '88274', '83405','86475', '85392', '85460', '80348', '81792'].contains( substringQUERY(ca_zip) )" +
                " || cs_sales_price > 500 "
                + "||  ['CA', 'WA', 'GA'].contains(ca_state)"
                ;

        ReadQuery res = api.read().select("ca_zip").sum("cs_sales_price", "sum_prices")
                .fromFilter(finalJoin, "fJ", filterPredicate).build();
        long t = System.currentTimeMillis();
        RelReadQueryResults results = res.run(broker);
        long elapsedTime = System.currentTimeMillis() -t;
        broker.shutdown();
        tm.stopServers();
        System.out.println("\n\nResults:\n");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res2");

        System.out.println("Table writing times, with shard creation: ");
        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q2_3timings");
        tm.stopServers();
        broker.shutdown();

    }
    @Test
    public void Q2_4Test() throws IOException {
        TestMethods tm = new TestMethods(); tm.startServers();
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

        ReadQuery catalogSalesInFirstQ98 = api.join()
                .select("catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_sales_price")
                .alias("cs_bill_customer_sk", "cs_sales_price")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_qoy == 1 && d_year == 1998").build();

        ReadQuery customerAndAddresses = api.join()
                .select("customer_address.ca_state", "customer_address.ca_zip", "customer.c_customer_sk")
                .alias("ca_state", "ca_zip", "c_customer_sk")
                .sources("customer", "customer_address", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();

        ReadQuery finalJoin = api.join().select("j2.ca_zip", "j1.cs_sales_price", "j2.ca_state")
                .alias("ca_zip", "cs_sales_price", "ca_state")
                .sources(catalogSalesInFirstQ98, customerAndAddresses, "j1", "j2",List.of("cs_bill_customer_sk"), List.of("c_customer_sk"))
                .build();

        String filterPredicate = "" +
                " substringQUERY = def (x) { x.substring(0,5) }; "
                + "['85669', '86197', '88274', '83405','86475', '85392', '85460', '80348', '81792'].contains( substringQUERY(ca_zip) )" +
                " || cs_sales_price > 500 "
                + "||  ['CA', 'WA', 'GA'].contains(ca_state)"
                ;

        ReadQuery res = api.read().select("ca_zip").sum("cs_sales_price", "sum_prices")
                .fromFilter(finalJoin, "fJ", filterPredicate).store().build();

        res.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        long t = System.currentTimeMillis();
        RelReadQueryResults results = res.run(broker);
        long elapsedTime = System.currentTimeMillis() - t;
        broker.shutdown();
        tm.stopServers();
        System.out.println("\n\nResults:\n");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res2");

        System.out.println("Table writing times, with shard creation: ");
        for (Pair<String, Long> time : loadTimes) {
            System.out.println("\t" + time.getValue0() + ": " + time.getValue1() + "ms");
        }
        System.out.println("Read time: " + elapsedTime);
        tm.saveTimings(loadTimes, elapsedTime, "Q2_4timings");
        tm.stopServers();
        broker.shutdown();
    }

    public List<String> tablesToLoad = List.of(
            "catalog_sales",
            "customer",
            "customer_address",
            "date_dim"
    );
}