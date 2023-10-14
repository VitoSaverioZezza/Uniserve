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
import static edu.stanford.futuredata.uniserve.rel.TestMethods.*;

public class Q4Test {
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
    public void Q4Test(){
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        tm.loadDataInMem(broker, tablesToLoad);
        ReadQuery j1 = api.join().sources(
                "inventory", "date_dim",
                List.of("inv_date_sk"), List.of("d_date_sk")
        ).filters("", "d_month_seq > 1205 && d_month_seq < 1205+11").build();

        ReadQuery j1p = api.read().select(
                "inventory.inv_quantity_on_hand",
                "inventory.inv_item_sk",
                "inventory.inv_warehouse_sk"
        ).alias(
                "inv_quantity_on_hand",
                "inv_item_sk",
                "inv_warehouse_sk"
        ).from(j1, "j1").build();

        ReadQuery j2 = api.join().sources(
                j1p, "warehouse", "j1p",
                List.of("inv_warehouse_sk"), List.of("w_warehouse_sk")
        ).build();

        ReadQuery j3 = api.join().sources(
                j2, "item", "j2",
                List.of("j1p.inv_item_sk"), List.of("i_item_sk")
        ).build();

        ReadQuery finalQuery = api.read()
                .select(
                        "item.i_product_name",
                        "item.i_brand",
                        "item.i_class",
                        "item.i_category"
                )
                .avg("j2.j1p.inv_quantity_on_hand", "qoh")
                .alias(
                        "i_product_name",
                        "i_brand",
                        "i_class",
                        "i_category"
                )
                .from(j3, "j3").build();


        RelReadQueryResults results = finalQuery.run(broker);

        System.out.println("RESULTS:");
        printRowList(results.getData());

        System.out.println("\nreturning...");
        tm.stopServers();
    }


    public List<String> tablesToLoad = List.of(
            "inventory",
            "date_dim",
            "item",
            "warehouse"
    );
}
