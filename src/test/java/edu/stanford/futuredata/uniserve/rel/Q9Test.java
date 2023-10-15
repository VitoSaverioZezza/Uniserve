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

public class Q9Test {

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
    * query 42 of the TPC DS benchmark
SELECT dt.d_year,
               item.i_category_id,
               item.i_category,
               Sum(ss_ext_sales_price)
FROM   date_dim dt,
       store_sales,
       item
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk
       AND store_sales.ss_item_sk = item.i_item_sk
       AND item.i_manager_id = 1
       AND dt.d_moy = 12
       AND dt.d_year = 2000
GROUP  BY dt.d_year,
          item.i_category_id,
          item.i_category
ORDER  BY Sum(ss_ext_sales_price) DESC,
          dt.d_year,
          item.i_category_id,
          item.i_category
LIMIT 100; */
    @Test
    public void Q9Test(){
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(TestMethods.zkHost, TestMethods.zkPort);
        API api = new API(broker);
        tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery storeSalesInDec2000 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_ext_sales_price", "date_dim.d_year")
                .alias("ss_item_sk", "ss_ext_sales_price", "d_year").sources(
                        "store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk")
                ).filters("", "d_moy == 12 && d_year == 2000").build();

        ReadQuery ssInDec2000FromManager1 = api.join().select("item.i_category_id", "item.i_category", "j1.ss_ext_sales_price", "j1.d_year")
                .sources(storeSalesInDec2000, "item", "j1", List.of("ss_item_sk"), List.of("i_item_sk"))
                .filters("", "i_manager_id == 1").build();

        ReadQuery q9 = api.read().select("j1.d_year", "item.i_category_id", "item.i_category")
                .sum("j1.ss_ext_sales_price", "sum_ss_ext_sales_price")
                .from(ssInDec2000FromManager1, "src").build();
        RelReadQueryResults results = q9.run(broker);


        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());

        System.out.println("\nreturning...");
        broker.shutdown();
        tm.stopServers();
    }

    public List<String> tablesToLoad = List.of(
            "store_sales","date_dim","item"
    );

}
