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
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
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
import static edu.stanford.futuredata.uniserve.rel.TestMethods.zkHost;
import static edu.stanford.futuredata.uniserve.rel.TestMethods.zkPort;

public class Q3Test {
    @BeforeAll
    static void startUpCleanUp() throws IOException {
        TestMethods.startUpCleanUp();
    }

    @AfterEach
    private void unitTestCleanUp() throws IOException {
        TestMethods.unitTestCleanUp();
    }
    @Test
    public void clean(){}

    /*
    SELECT i_brand_id              brand_id,
               i_brand                 brand,
               i_manufact_id,
               i_manufact,
               Sum(ss_ext_sales_price) ext_price
FROM   date_dim,
       store_sales,
       item,
       customer,
       customer_address,
       store
WHERE  d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manager_id = 38
       AND d_moy = 12
       AND d_year = 1998
       AND ss_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND Substr(ca_zip, 1, 5) <> Substr(s_zip, 1, 5)
       AND ss_store_sk = s_store_sk
GROUP  BY i_brand,
          i_brand_id,
          i_manufact_id,
          i_manufact
ORDER  BY ext_price DESC,
          i_brand,
          i_brand_id,
          i_manufact_id,
          i_manufact
    * */

    @Test
    public void Q3Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery storeSalesInDec98 = api.join()
                .select("store_sales.ss_ext_sales_price",
                        "store_sales.ss_item_sk",
                        "store_sales.ss_store_sk",
                        "store_sales.ss_customer_sk",
                        "date_dim.d_moy",
                        "date_dim.d_year"
                )
                .alias("ss_ext_sales_price",
                        "ss_item_sk",
                        "ss_store_sk",
                        "ss_customer_sk",
                        "d_moy",
                        "d_year"
                )
                .sources("date_dim", "store_sales",
                        List.of("d_date_sk"), List.of("ss_sold_date_sk"))
                .filters("d_moy == 12 && d_year == 1998", "").build();

        ReadQuery storeSalesInDec98ByManager38 = api.join()
                .select("storeSalesInDec98.ss_ext_sales_price",
                        "storeSalesInDec98.ss_store_sk",
                        "storeSalesInDec98.ss_customer_sk",
                        "item.i_brand",
                        "item.i_brand_id",
                        "item.i_manufact_id",
                        "item.i_manufact")
                .alias("ss_ext_sales_price",
                        "ss_store_sk",
                        "ss_customer_sk",
                        "i_brand",
                        "i_brand_id",
                        "i_manufact_id",
                        "i_manufact")
                .sources(storeSalesInDec98, "item", "storeSalesInDec98",
                List.of("ss_item_sk"), List.of("i_item_sk")
        ).filters("", "i_manager_id == 38").build();

        //RelReadQueryResults j2 = storeSalesInDec98ByManager38.run(broker);
        //System.out.println("\n\nSecond join size (UNISERVE): " + j2.getData().size());

        ReadQuery j3 = api.join()
                .select("customer.c_current_addr_sk",
                        "j2.ss_ext_sales_price",
                        "j2.ss_store_sk",
                        "j2.i_brand",
                        "j2.i_brand_id",
                        "j2.i_manufact_id",
                        "j2.i_manufact")
                .alias("c_current_addr_sk",
                        "ss_ext_sales_price",
                        "ss_store_sk",
                        "i_brand",
                        "i_brand_id",
                        "i_manufact_id",
                        "i_manufact")
                .sources(storeSalesInDec98ByManager38, "customer", "j2",
                        List.of("ss_customer_sk"), List.of("c_customer_sk")
        ).build();

        //RelReadQueryResults j3r = j3.run(broker);
        //System.out.println("\n\nThird join size (UNISERVE): " + j3r.getData().size());

        ReadQuery j4 = api.join()
                .select("pj3.ss_ext_sales_price",
                        "pj3.ss_store_sk",
                        "pj3.i_brand",
                        "pj3.i_brand_id",
                        "pj3.i_manufact_id",
                        "pj3.i_manufact",
                        "customer_address.ca_zip")
                .alias("ss_ext_sales_price",
                        "ss_store_sk",
                        "i_brand",
                        "i_brand_id",
                        "i_manufact_id",
                        "i_manufact",
                        "ca_zip")
                .sources(j3, "customer_address", "pj3", List.of("c_current_addr_sk"), List.of("ca_address_sk")
        ).build();


        ReadQuery j5 = api.join().sources(
                j4, "store", "pj4",
                List.of("ss_store_sk"), List.of("s_store_sk")
        ).build();


        ReadQuery fpj5r = api.read().select(
                "pj4.i_brand_id",
                "pj4.i_brand",
                "pj4.i_manufact_id",
                "pj4.i_manufact",
                "pj4.ss_ext_sales_price"
        ).alias(
                "i_brand_id",
                "i_brand",
                "i_manufact_id",
                "i_manufact",
                "ss_ext_sales_price"
        ).fromFilter(j5, "j5", "substringQUERY = def (x) { x.substring(0,5) }; substringQUERY(pj4.ca_zip) != substringQUERY(store.s_zip)").build();

        ReadQuery fpj5 = api.read().select(
                "pj4.i_brand_id",
                "pj4.i_brand",
                "pj4.i_manufact_id",
                "pj4.i_manufact"
        ).alias(
                "i_brand_id",
                "i_brand",
                "i_manufact_id",
                "i_manufact"
        ).sum("pj4.ss_ext_sales_price", "ext_price"
        ).fromFilter(j5, "j5", "substringQUERY = def (x) { x.substring(0,5) }; substringQUERY(pj4.ca_zip) != substringQUERY(store.s_zip)").build();

        RelReadQueryResults results = fpj5.run(broker);
        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), List.of("i_brand_id", "i_brand", "i_manufact_id", "i_manufact", "sum_ss_ext_price"));


        System.out.println("\nreturning...");
        tm.stopServers();


    }


    public List<String> tablesToLoad = List.of(
            "date_dim",
            "store_sales",
            "item",
            "customer",
            "customer_address",
            "store"
    );
}
