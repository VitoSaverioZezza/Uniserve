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

public class Q8Test {
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

    @Test
    public void Q8Test() throws IOException {
        TestMethods tm = new TestMethods();
        tm.startServers();
        Broker broker = new Broker(TestMethods.zkHost, TestMethods.zkPort);
        API api = new API(broker);
        tm.loadDataInMem(broker, tablesToLoad);

        String filterPredicate = "(" +
                "(i_category == 'Women'" +
                "&& (i_color == 'dim' || i_color == 'green')" +
                "&& (i_units == 'Gross' || i_units == 'Dozen')" +
                "&& (i_size == 'economy' || i_size == 'petite')) " +
                "||" +
                "(i_category == 'Women'" +
                "&& (i_color == 'navajo'  || i_color == 'aquamarine') " +
                "&& (i_units == 'Case'    || i_units == 'Unknown') " +
                "&& (i_size == 'large'    || i_size == 'N/A')) " +
                "||" +
                "(i_category == 'Men'" +
                "&&   (i_color == 'indian' || i_color == 'dark')" +
                "&&   (i_units == 'Oz' || i_units == 'Lb' )" +
                "&&   (i_size == 'extra large' || i_size == 'small'))" +
                "||" +
                "(i_category == 'Men'" +
                "&& ( i_color == 'peach' || i_color == 'purple' )" +
                "&& ( i_units == 'Tbl' || i_units == 'Bunch')" +
                "&& ( i_size == 'economy' || i_size == 'petite' ))" +
                "||" +
                "(i_category == 'Women'" +
                "&& ( i_color == 'orchid' || i_color == 'peru' )" +
                "&& ( i_units == 'Carton'  || i_units == 'Cup' )" +
                "&& ( i_size == 'economy' || i_size == 'petite')) " +
                "||" +
                "(i_category == 'Women'" +
                "&& ( i_color == 'violet' || i_color == 'papaya')" +
                "&& ( i_units == 'Ounce'  || i_units == 'Box')" +
                "&& ( i_size == 'large' || i_size == 'N/A'))" +
                "||" +
                "(i_category == 'Men'" +
                "&& ( i_color == 'drab' || i_color == 'grey' )" +
                "&& ( i_units == 'Each' || i_units == 'N/A' )" +
                "&& ( i_size == 'extra large' || i_size == 'small' ))" +
                "||" +
                "(i_category == 'Men'" +
                "&& ( i_color == 'chocolate' || i_color == 'antique' )" +
                "&& ( i_units == 'Dram' || i_units == 'Gram' )" +
                "&& ( i_size == 'economy' || i_size == 'petite' )) " +
                ")";

        ReadQuery join = api.join().sources(
                api.read().select("i_manufact")
                        .count("i_item_sk", "item_cnt")
                        .fromFilter("item", filterPredicate).build(),
                "item",
                "i1",
                List.of("i_manufact"), List.of("i_manufact")
        ).filters("item_cnt > 0", "i_manufact_id >= 765 && i_manufact_id < 805").build();

        ReadQuery finalQuery = api.read().select("item.i_product_name").from(join, "src")
                .distinct().build();
        RelReadQueryResults results = finalQuery.run(broker);


        System.out.println("RESULTS:");
        TestMethods.printRowList(results.getData());
        tm.printOnFile(results.getData(), List.of("i_product_name"),"res8");

        System.out.println("\nreturning...");
        broker.shutdown();
        tm.stopServers();
    }

    /*
    * query 41 of the TPC DS benchmark
SELECT Distinct(i_product_name)
FROM   item i1
WHERE  i_manufact_id BETWEEN 765 AND 765 + 40
       AND (SELECT Count(*) AS item_cnt
            FROM   item
            WHERE  ( i_manufact = i1.i_manufact
                     AND ( ( i_category = 'Women'
                             AND ( i_color = 'dim'
                                    OR i_color = 'green' )
                             AND ( i_units = 'Gross'
                                    OR i_units = 'Dozen' )
                             AND ( i_size = 'economy'
                                    OR i_size = 'petite' ) )
                            OR ( i_category = 'Women'
                                 AND ( i_color = 'navajo'
                                        OR i_color = 'aquamarine' )
                                 AND ( i_units = 'Case'
                                        OR i_units = 'Unknown' )
                                 AND ( i_size = 'large'
                                        OR i_size = 'N/A' ) )
                            OR ( i_category = 'Men'
                                 AND ( i_color = 'indian'
                                        OR i_color = 'dark' )
                                 AND ( i_units = 'Oz'
                                        OR i_units = 'Lb' )
                                 AND ( i_size = 'extra large'
                                        OR i_size = 'small' ) )
                            OR ( i_category = 'Men'
                                 AND ( i_color = 'peach'
                                        OR i_color = 'purple' )
                                 AND ( i_units = 'Tbl'
                                        OR i_units = 'Bunch' )
                                 AND ( i_size = 'economy'
                                        OR i_size = 'petite' ) ) ) )
                    OR ( i_manufact = i1.i_manufact
                         AND ( ( i_category = 'Women'
                                 AND ( i_color = 'orchid'
                                        OR i_color = 'peru' )
                                 AND ( i_units = 'Carton'
                                        OR i_units = 'Cup' )
                                 AND ( i_size = 'economy'
                                        OR i_size = 'petite' ) )
                                OR ( i_category = 'Women'
                                     AND ( i_color = 'violet'
                                            OR i_color = 'papaya' )
                                     AND ( i_units = 'Ounce'
                                            OR i_units = 'Box' )
                                     AND ( i_size = 'large'
                                            OR i_size = 'N/A' ) )
                                OR ( i_category = 'Men'
                                     AND ( i_color = 'drab'
                                            OR i_color = 'grey' )
                                     AND ( i_units = 'Each'
                                            OR i_units = 'N/A' )
                                     AND ( i_size = 'extra large'
                                            OR i_size = 'small' ) )
                                OR ( i_category = 'Men'
                                     AND ( i_color = 'chocolate'
                                            OR i_color = 'antique' )
                                     AND ( i_units = 'Dram'
                                            OR i_units = 'Gram' )
                                     AND ( i_size = 'economy'
                                            OR i_size = 'petite' ) ) ) )) > 0
ORDER  BY i_product_name
LIMIT 100; */


    public List<String> tablesToLoad = List.of(
            "item"
    );
}
