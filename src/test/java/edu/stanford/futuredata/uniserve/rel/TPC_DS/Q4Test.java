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

import static edu.stanford.futuredata.uniserve.rel.TPC_DS.TestMethods.*;

public class Q4Test {
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
            "inventory",
            "date_dim",
            "item",
            "warehouse"
    );

    /*
Query 22 of the TPC DS benchmark
SELECT i_product_name,
               i_brand,
               i_class,
               i_category,
               Avg(inv_quantity_on_hand) qoh
FROM   inventory,
       date_dim,
       item,
       warehouse
WHERE  inv_date_sk = d_date_sk
       AND inv_item_sk = i_item_sk
       AND inv_warehouse_sk = w_warehouse_sk
       AND d_month_seq BETWEEN 1205 AND 1205 + 11
GROUP  i_product_name, i_brand, i_class, i_category
     */
    @Test
    public void Q4Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(zkHost, zkPort);
        API api = new API(broker);
        tm.loadDataInMem(broker, tablesToLoad);
        ReadQuery j1 = api.join().sources(
                "inventory", "date_dim",
                List.of("inv_date_sk"), List.of("d_date_sk")
        ).filters("", "d_month_seq >= 1205 && d_month_seq <= 1216").build();

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

        System.out.println("RESULTS:");
        RelReadQueryResults results = finalQuery.run(broker);
        printRowList(results.getData());
        tm.printOnFile(results.getData(), results.getFieldNames(),"res4");

        System.out.println("\nreturning...");
        tm.stopServers();
    }
}
