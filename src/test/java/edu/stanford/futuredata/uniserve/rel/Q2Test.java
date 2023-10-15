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
import static edu.stanford.futuredata.uniserve.rel.TestMethods.*;

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
            AND ( Substr(ca_zip, 1, 5) IN ( '85669', '86197', '88274', '83405',
                                       '86475', '85392', '85460', '80348',
                                       '81792' )
                OR ca_state IN ( 'CA', 'WA', 'GA' )
                OR cs_sales_price > 500
                )
            AND cs_sold_date_sk = d_date_sk
       AND d_qoy = 1
       AND d_year = 1998
GROUP  BY ca_zip
*/

    @Test
    public void Q2Test(){
        TestMethods tm = new TestMethods(); tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        tm.loadDataInMem(broker, tablesToLoad);

       ReadQuery catalogSalesInFirstQ98 = api.join().select(
               "catalog_sales.cs_bill_customer_sk", //join with customer
               "catalog_sales.cs_sales_price" //to be aggregated
       ).alias("cs_bill_customer_sk", "cs_sales_price"
       ).sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk")
       ).filters("", "d_qoy == 1 && d_year == 1998").build();

       //RelReadQueryResults j = catalogSalesInFirstQ98.run(broker);
       // System.out.println("\n\n\nfirst join size (UNISERVE): " + j.getData().size());

       ReadQuery customerAndAddresses = api.join().select(
               "customer_address.ca_state", //later condition
               "customer_address.ca_zip", //later condition and select
               "customer.c_customer_sk" //join
       ).alias("ca_state", "ca_zip", "c_customer_sk"
       ).sources("customer", "customer_address", List.of("c_current_addr_sk"), List.of("ca_address_sk")
       ).build();

       //j = customerAndAddresses.run(broker);
        //System.out.println("\n\n\nsecond join size (UNISERVE): " + j.getData().size());
        //tm.computePartialResultsQ2();


       ReadQuery finalJoin = api.join().select(
               "j2.ca_zip", //select and later condition
               "j1.cs_sales_price", //aggregate later
               "j2.ca_state" //later condition
       ).alias("ca_zip", "cs_sales_price", "ca_state"
       ).sources(catalogSalesInFirstQ98, customerAndAddresses, "j1", "j2"
       ,List.of("cs_bill_customer_sk"), List.of("c_customer_sk")).build();

       RelReadQueryResults j = finalJoin.run(broker);
       System.out.println("\n\n\nFinal join size (UNISERVE): " + j.getData().size());

       String filterPredicate = "" +
               " substringQUERY = def (x) { x.substring(0,5) }; "
                + "['85669', '86197', '88274', '83405','86475', '85392', '85460', '80348', '81792'].contains( substringQUERY(ca_zip) )" +
               " || cs_sales_price > 500 "
               + "||  ['CA', 'WA', 'GA'].contains(ca_state)"
               ;

       ReadQuery res = api.read().select("ca_zip").sum("cs_sales_price", "sum_prices")
               .fromFilter(finalJoin, "fJ", filterPredicate).build();

        RelReadQueryResults results = res.run(broker);
        broker.shutdown();
        tm.stopServers();
        System.out.println("\n\nResults:\n");
        TestMethods.printRowList(results.getData());
    }
    public List<String> tablesToLoad = List.of(
            "catalog_sales",
            "customer",
            "customer_address",
            "date_dim"
    );
}