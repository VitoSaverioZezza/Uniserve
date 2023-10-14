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

public class Q7Test {
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

    public List<String> tablesToLoad = List.of(
            "catalog_sales",
            "web_sales",
            "store_sales",
            "date_dim",
            "item",
            "customer_address"
    );

    @Test
    public void Q7Test(){
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(tm.zkHost, tm.zkPort);
        API api = new API(broker);
        tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery ssMarch99 = api.join().sources(
                "store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk")
        ).filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInStoreMarch99 = api.join().sources("item", ssMarch99, "ssMarch99", List.of("i_item_sk"), List.of("store_sales.ss_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery ssSrc = api.join().select("books.ssMarch99.store_sales.ss_ext_sales_price", "books.item.i_manufact_id").alias("ss_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInStoreMarch99, "customer_address", "books", List.of("ssMarch99.store_sales.ss_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ss = api.read().select("i_manufact_id").sum("ss_ext_sales_price", "total_sales").from(ssSrc, "ssSrc").build();

        ReadQuery csMarch99 = api.join().sources(
                "catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk")
        ).filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInCatalogMarch99 = api.join().sources("item", csMarch99, "csMarch99", List.of("i_item_sk"), List.of("catalog_sales.cs_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery csSrc = api.join().select("books.csMarch99.catalog_sales.cs_ext_sales_price", "books.item.i_manufact_id").alias("cs_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInCatalogMarch99, "customer_address", "books", List.of("csMarch99.catalog_sales.cs_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery cs = api.read().select("i_manufact_id").sum("cs_ext_sales_price", "total_sales").from(csSrc, "csSrc").build();

        ReadQuery wsMarch99 = api.join().sources(
                "web_sales", "date_dim", List.of("ws_sold_date_sk"), List.of("d_date_sk")
        ).filters("", "d_year == 1999 && d_moy == 3").build();

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
        TestMethods.printRowList(results.getData());

        System.out.println("\nreturning...");
        broker.shutdown();
        tm.stopServers();
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


}
