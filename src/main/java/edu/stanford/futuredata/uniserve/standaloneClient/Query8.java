package edu.stanford.futuredata.uniserve.standaloneClient;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relationalapi.API;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query8 {
    private final Broker broker;

    private String rootPath = "";
    public Query8(Broker broker, String rootPath){
        this.rootPath = rootPath;
        this.broker = broker;
    }

    private Map<String, RelReadQueryResults> resultsMap = new HashMap<>();
    private int queryIndex = -1;

    public void setQueryIndex(int queryIndex) {
        this.queryIndex = queryIndex;
    }
    public Map<Integer, List<Pair<String, Long>>> run(){
        API api = new API(broker);
        for(String tableName: tablesToLoad){
            int index = TPC_DS_Inv.names.indexOf(tableName);
            if(index == -1)
                return null;
            int shardNum = Math.min(Math.max(TPC_DS_Inv.Bsizes.get(index) / 10000, 1),Broker.SHARDS_PER_TABLE);
            api.createTable(tableName)
                    .attributes(TPC_DS_Inv.schemas.get(index).toArray(new String[0]))
                    .keys(TPC_DS_Inv.keys.get(index).toArray(new String[0]))
                    .shardNumber(shardNum).build().run();
        }
        Map<Integer, List<Pair<String, Long>>> timings = new HashMap<>();
        RelReadQueryResults results = null;

        switch (queryIndex){
            case 0:
                timings.put(0, runQ8_0());
                results = resultsMap.get("0");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 1:
                timings.put(1, runQ8_1());
                results = resultsMap.get("1");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 2:
                timings.put(2, runQ8_2());
                results = resultsMap.get("2");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 3:
                timings.put(3, runQ8_3());
                results = resultsMap.get("3");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
        }
        return timings;
    }
    public static final List<String> tablesToLoad = List.of(
            "item"
    );

    public List<Pair<String, Long>> runQ8_0(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

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


        ReadQuery i1 = api.read()
                .select("i_manufact")
                .count("i_item_sk", "item_cnt")
                .fromFilter("item", filterPredicate).build();

        ReadQuery join = api.join()
                .sources(i1, "item", "i1", List.of("i_manufact"), List.of("i_manufact"))
                .filters("item_cnt > 0", "i_manufact_id >= 765 && i_manufact_id < 805").build();

        ReadQuery finalQuery = api.read()
                .select("item.i_product_name")
                .from(join, "src")
                .distinct().build();


        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("0", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ8_1(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);
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


        ReadQuery i1 = api.read()
                .select("i_manufact")
                .count("i_item_sk", "item_cnt").store()
                .fromFilter("item", filterPredicate).build();

        i1.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery join = api.join()
                .sources(i1, "item", "i1", List.of("i_manufact"), List.of("i_manufact"))
                .filters("item_cnt > 0", "i_manufact_id >= 765 && i_manufact_id < 805").build();

        ReadQuery finalQuery = api.read()
                .select("item.i_product_name")
                .from(join, "src")
                .distinct().build();


        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("1", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ8_2(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

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


        ReadQuery i1 = api.read()
                .select("i_manufact")
                .count("i_item_sk", "item_cnt")
                .fromFilter("item", filterPredicate).build();

        ReadQuery join = api.join()
                .sources(i1, "item", "i1", List.of("i_manufact"), List.of("i_manufact"))
                .stored()
                .filters("item_cnt > 0", "i_manufact_id >= 765 && i_manufact_id < 805").build();


        join.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery finalQuery = api.read()
                .select("item.i_product_name")
                .from(join, "src")
                .distinct().build();


        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("2", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ8_3(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

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


        ReadQuery i1 = api.read()
                .select("i_manufact")
                .count("i_item_sk", "item_cnt")
                .fromFilter("item", filterPredicate).build();

        ReadQuery join = api.join()
                .sources(i1, "item", "i1", List.of("i_manufact"), List.of("i_manufact"))
                .filters("item_cnt > 0", "i_manufact_id >= 765 && i_manufact_id < 805").build();

        ReadQuery finalQuery = api.read()
                .store()
                .select("item.i_product_name")
                .from(join, "src")
                .distinct().build();

        finalQuery.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("3", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }

    //SQL Query definition:
    /*
    * Q41 in stream 0 using template query41.tpl
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




}
