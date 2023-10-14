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
import static edu.stanford.futuredata.uniserve.rel.TestMethods.zkHost;
import static edu.stanford.futuredata.uniserve.rel.TestMethods.zkPort;

public class Q5Test {

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


    @Test
    public void Q5Test(){
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        tm.loadDataInMem(broker, tablesToLoad);


        ReadQuery storeSalesInApril2001 = api.join().sources(
                "store_sales", "date_dim",
                List.of("ss_sold_date_sk"), List.of("d_date_sk")
        ).filters("", "d_moy == 4 && d_year == 2001").build();

        ReadQuery storeReturnsIn2001AfterApril = api.join().sources(
                "store_returns", "date_dim",
                List.of("sr_returned_date_sk"), List.of("d_date_sk")
        ).filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001").build();

        ReadQuery catalogSalesIn2001AfterApril = api.join().sources(
                "catalog_sales", "date_dim",
                List.of("cs_sold_date_sk"), List.of("d_date_sk")
        ).filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001").build();

        ReadQuery returnedStoreAprilSales = api.join().sources(
                storeSalesInApril2001, storeReturnsIn2001AfterApril, "sales", "returns",
                List.of("store_sales.ss_customer_sk", "store_sales.ss_item_sk", "store_sales.ss_ticket_number"),
                List.of("store_returns.sr_customer_sk", "store_returns.sr_item_sk", "store_returns.sr_ticket_number")).build();

        ReadQuery returnedStoreAprilSalesPrj = api.read().select(
                "sales.store_sales.ss_net_profit",
                "sales.store_sales.ss_item_sk",
                "sales.store_sales.ss_store_sk",
                "sales.store_sales.ss_customer_sk",
                "returns.store_returns.sr_net_loss"
        ).alias(
                "ss_net_profit",
                "ss_item_sk",
                "ss_store_sk",
                "ss_customer_sk",
                "sr_net_loss"
        ).from(returnedStoreAprilSales, "rsas").build();

        ReadQuery returnedStoreSalesInAprilWhoLaterBoughtOnCatalog = api.join().sources(
                returnedStoreAprilSalesPrj, catalogSalesIn2001AfterApril, "rsasp", "csaa",
                List.of("ss_customer_sk", "ss_item_sk"), List.of("catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_item_sk")
        ).build();

        ReadQuery joinItem = api.join().sources(
                returnedStoreSalesInAprilWhoLaterBoughtOnCatalog, "item", "penultimateJoin",
                List.of("rsasp.ss_item_sk"), List.of("i_item_sk")
        ).build();

        ReadQuery joinStore = api.join().sources(
                joinItem, "store", "j1",
                List.of("penultimateJoin.rsasp.ss_store_sk"), List.of("s_store_sk")
        ).build();

        ReadQuery cleanSchema = api.read().select(
                "j1.item.i_item_id",
                "j1.item.i_item_desc",
                "store.s_store_id",
                "store.s_store_name",
                "j1.penultimateJoin.csaa.cs_net_profit",
                "j1.penultimateJoin.rsasp.ss_net_profit",
                "j1.penultimateJoin.rsasp.sr_net_loss"
        ).alias(
                "i_item_id",
                "i_item_desc",
                "s_store_id",
                "s_store_name",
                "cs_net_profit",
                "ss_net_profit",
                "sr_net_loss"
        ).from(joinStore, "js").build();

        ReadQuery finalQuery = api.read().select("i_item_id",
                        "i_item_desc",
                        "s_store_id",
                        "s_store_name"
                )
                .max("ss_net_profit", "store_sales_profit")
                .max("sr_net_loss", "store_returns_loss")
                .max("cs_net_profit", "catalog_sales_profit")
                .from(cleanSchema, "src")
                .build();
        RelReadQueryResults results = finalQuery.run(broker);

        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());

        System.out.println("\nreturning...");
        tm.stopServers();
    }


    public List<String> tablesToLoad = List.of(
            "store_sales",
            "store_returns",
            "catalog_sales",
            "date_dim",
            "store",
            "item"
    );

}
