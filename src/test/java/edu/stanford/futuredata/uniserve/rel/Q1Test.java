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
    public void Q1Test(){
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(TestMethods.zkHost, TestMethods.zkPort);
        API api = new API(broker);
        tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery salesSubsetSoldIn2001 = api.join()
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("(ss_sales_price >= 50 && ss_sales_price <=200) && ss_net_profit >= 50 && ss_net_profit <=300"
                        , "d_year == 2001")
                .build();


        ReadQuery salesSubsetSoldIn2001StoreInfo = api.join().select(
                        "salesSubsetSoldIn2001.store_sales.ss_net_profit",
                        "salesSubsetSoldIn2001.store_sales.ss_cdemo_sk",
                        "salesSubsetSoldIn2001.store_sales.ss_hdemo_sk",
                        "salesSubsetSoldIn2001.store_sales.ss_sales_price",
                        "salesSubsetSoldIn2001.store_sales.ss_addr_sk",
                        "salesSubsetSoldIn2001.store_sales.ss_quantity",
                        "salesSubsetSoldIn2001.store_sales.ss_ext_sales_price",
                        "salesSubsetSoldIn2001.store_sales.ss_ext_wholesale_cost"
                ).alias(
                        "ss_net_profit",
                        "ss_cdemo_sk",
                        "ss_hdemo_sk",
                        "ss_sales_price",
                        "ss_addr_sk",
                        "ss_quantity",
                        "ss_ext_sales_price",
                        "ss_ext_wholesale_cost"
                ).sources("store", salesSubsetSoldIn2001, "salesSubsetSoldIn2001", List.of("s_store_sk"), List.of("store_sales.ss_store_sk"))
                .build();

        ReadQuery j_cd = api.join().sources(
                        salesSubsetSoldIn2001StoreInfo, "customer_demographics", "j_ss_s_d",
                        List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("",
                        "(cd_marital_status == \"U\" && cd_education_status == \"Advanced Degree\") || " +
                                "(cd_marital_status == \"M\" && cd_education_status == \"Primary\") || " +
                                "(cd_marital_status == \"D\" && cd_education_status == \"Secondary\")")
                .build();

        ReadQuery j_cd_hd = api.join().sources(
                        j_cd, "household_demographics", "j_cd",
                        List.of("j_ss_s_d.ss_hdemo_sk"), List.of("hd_demo_sk")).filters("",
                        "hd_dep_count == 3 || hd_dep_count == 1")
                .build();


        ReadQuery firstPart = api.read().select(
                        "j_cd.j_ss_s_d.ss_quantity",
                        "j_cd.j_ss_s_d.ss_ext_sales_price",
                        "j_cd.j_ss_s_d.ss_ext_wholesale_cost",
                        "j_cd.j_ss_s_d.ss_addr_sk",
                        "j_cd.j_ss_s_d.ss_net_profit"
                ).alias(
                        "ss_quantity",
                        "ss_ext_sales_price",
                        "ss_ext_wholesale_cost",
                        "ss_addr_sk",
                        "ss_net_profit"
                )
                .fromFilter(j_cd_hd, "src",
                        "(j_cd.j_ss_s_d.cd_marital_status == \"U\" && j_cd.j_ss_s_d.cd_education_status == \"Advanced Degree\" && " +
                                "j_cd.j_ss_s_d.ss_sales_price >= 100 && j_cd.j_ss_s_d.ss_sales_price <= 150 && household_demographics.hd_dep_count == 3) || " +
                                "(j_cd.j_ss_s_d.cd_marital_status == \"M\" && j_cd.j_ss_s_d.cd_education_status == \"Primary\" && " +
                                "j_cd.j_ss_s_d.ss_sales_price >= 50 && j_cd.j_ss_s_d.ss_sales_price <= 100 && household_demographics.hd_dep_count == 1) || " +
                                "(j_cd.j_ss_s_d.cd_marital_status == \"D\" && j_cd.j_ss_s_d.cd_education_status == \"Secondary\" && " +
                                "j_cd.j_ss_s_d.ss_sales_price >= 150 && j_cd.j_ss_s_d.ss_sales_price <= 200 && household_demographics.hd_dep_count == 1)"
                ).build();

        ReadQuery final_source = api.join().sources(
                firstPart, "customer_address", "first_part",
                List.of("ss_addr_sk"), List.of("ca_address_sk")).filters("",
                "ca_country == \"United States\" && " +
                        "[\"AZ\",\"NE\",\"IA\",\"MS\",\"CA\",\"NV\",\"GA\",\"TX\",\"NJ\"].contains(ca_state)").build();

        ReadQuery q1 = api.read()
                .avg("first_part.ss_quantity", "avg_ss_quantity")
                .avg("first_part.ss_ext_sales_price", "avg_ss_ext_sales_price")
                .avg("first_part.ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                .sum("first_part.ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                .fromFilter(final_source, "f_src",
                        "([\"AZ\",\"NE\",\"IA\"].contains(customer_address.ca_state) && " +
                                "first_part.ss_sales_price >= 50.00 && " +
                                "first_part.ss_sales_price <= 100.00) ||" +
                                "([\"MS\",\"CA\",\"NV\"].contains(customer_address.ca_state) && " +
                                "first_part.ss_sales_price >= 150.00 && " +
                                "first_part.ss_sales_price <= 300.00) ||" +
                                "([\"GA\",\"TX\",\"NJ\"].contains(customer_address.ca_state) && " +
                                "first_part.ss_sales_price >= 50.00 && " +
                                "first_part.ss_sales_price <= 250.00)"
                ).build();

        RelReadQueryResults results = q1.run(broker);
        broker.shutdown();
        tm.stopServers();
        System.out.println("\n\nResults:\n");
        //TestMethods.printRowList(results.getData());
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