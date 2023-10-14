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

public class Q6Test {
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
    public void Q6Test(){
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        tm.loadDataInMem(broker, tablesToLoad);
        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join().sources("catalog_sales", "customer_demographics",
                List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk")
        ).filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'secondary'").build();

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join().sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1",
                List.of("catalog_sales.cs_sold_date_sk"), List.of("d_date_sk")
        ).filters("", "d_year == 2000").build();

        ReadQuery csToMWSEdInPromotion = api.join().sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "csToMWSEd",
                List.of("q1.catalog_sales.cs_promo_sk"), List.of("p_promo_sk")
        ).filters(
                "",
                "p_channel_email == 'N' || p_channel_event == 'N'"
        ).build();

        ReadQuery finalJoin = api.join().select(
                "item.i_item_id",
                "cs.csToMWSEd.q1.catalog_sales.cs_quantity",
                "cs.csToMWSEd.q1.catalog_sales.cs_list_price",
                "cs.csToMWSEd.q1.catalog_sales.cs_coupon_amt",
                "cs.csToMWSEd.q1.catalog_sales.cs_sales_price"
        ).alias(
                "i_item_id",
                "cs_quantity",
                "cs_list_price",
                "cs_coupon_amt",
                "cs_sales_price"
        ).sources(csToMWSEdInPromotion, "item", "cs",
                List.of("csToMWSEd.q1.catalog_sales.cs_item_sk"), List.of("i_item_sk")).build();

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .build();

        RelReadQueryResults results = finalQuery.run(broker);

        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());

        System.out.println("\nreturning...");
        broker.shutdown();
        tm.stopServers();
    }

    public List<String> tablesToLoad = List.of(
            "catalog_sales",
            "customer_demographics",
            "date_dim",
            "item",
            "promotion"
    );
}
