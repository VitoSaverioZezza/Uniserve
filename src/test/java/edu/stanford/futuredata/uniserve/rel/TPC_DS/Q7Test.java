package edu.stanford.futuredata.uniserve.rel.TPC_DS;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relationalapi.API;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class Q7Test {
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
            "catalog_sales",
            "web_sales",
            "store_sales",
            "date_dim",
            "item",
            "customer_address"
    );

    @Test
    public void Q7Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(TestMethods.zkHost, TestMethods.zkPort);
        API api = new API(broker);
        tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery ssMarch99 = api.join()
                .select("store_sales.ss_item_sk",
                        "store_sales.ss_addr_sk",
                        "store_sales.ss_ext_sales_price")
                .alias("ss_item_sk",
                        "ss_addr_sk",
                        "ss_ext_sales_price")
                .sources(
                "store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk")
        ).filters("", "d_year == 1999 && d_moy == 3").build();

        //SIZE OK

        ReadQuery booksSoldInStoreMarch99 = api.join()
                .select("item.i_manufact_id",
                        "ssMarch99.ss_addr_sk",
                        "ssMarch99.ss_ext_sales_price")
                .alias("i_manufact_id",
                        "ss_addr_sk",
                        "ss_ext_sales_price")
                .sources("item", ssMarch99, "ssMarch99",
                        List.of("i_item_sk"), List.of("ss_item_sk"))
                .filters("i_category == 'Books'", "").build();

        //SIZE OK

        ReadQuery ssSrc = api.join()
                .select("books.ss_ext_sales_price", "books.i_manufact_id")
                .alias("ss_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInStoreMarch99, "customer_address", "books",
                        List.of("ss_addr_sk"), List.of("ca_address_sk"))
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
        tm.printOnFile(results.getData(), results.getFieldNames(),"res7");

        System.out.println("\nreturning...");
        broker.shutdown();
        tm.stopServers();
    }
    /*query 33 of the TPC DS benchmark
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


q.i_manufact_id,
q.total_sales - t.total_sales

 */


}
