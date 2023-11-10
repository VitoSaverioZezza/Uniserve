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

public class Query9 {
    private final Broker broker;

    private String rootPath = "";
    public Query9(Broker broker, String rootPath){
        this.rootPath = rootPath;
        this.broker = broker;
    }


    public final static List<String> tablesToLoad = List.of(
            "store_sales", "date_dim", "item"
    );
    private int queryIndex = -1;

    public void setQueryIndex(int queryIndex) {
        this.queryIndex = queryIndex;
    }
    private Map<String, RelReadQueryResults> resultsMap = new HashMap<>();

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
                timings.put(0, runQ9_0());
                results = resultsMap.get("0");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 1:
                timings.put(1, runQ9_1());
                results = resultsMap.get("1");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 2:
                timings.put(2, runQ9_2());
                results = resultsMap.get("2");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 3:
                timings.put(3, runQ9_3());
                results = resultsMap.get("3");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
        }
        return timings;
    }

    public List<Pair<String, Long>> runQ9_0(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery storeSalesInDec2000 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_ext_sales_price", "date_dim.d_year")
                .alias("ss_item_sk", "ss_ext_sales_price", "d_year")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 12 && d_year == 2000").build();

        ReadQuery ssInDec2000FromManager1 = api.join()
                .select("item.i_category_id", "item.i_category", "j1.ss_ext_sales_price", "j1.d_year")
                .sources(storeSalesInDec2000, "item", "j1", List.of("ss_item_sk"), List.of("i_item_sk"))
                .filters("", "i_manager_id == 1").build();

        ReadQuery finalQuery = api.read()
                .select("j1.d_year", "item.i_category_id", "item.i_category")
                .alias("d_year", "i_category_id", "i_category")
                .sum("j1.ss_ext_sales_price", "sum_ss_ext_sales_price")
                .from(ssInDec2000FromManager1, "src").build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("0", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ9_1(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery storeSalesInDec2000 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_ext_sales_price", "date_dim.d_year")
                .alias("ss_item_sk", "ss_ext_sales_price", "d_year")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 12 && d_year == 2000").stored().build();

        storeSalesInDec2000.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery ssInDec2000FromManager1 = api.join()
                .select("item.i_category_id", "item.i_category", "j1.ss_ext_sales_price", "j1.d_year")
                .sources(storeSalesInDec2000, "item", "j1", List.of("ss_item_sk"), List.of("i_item_sk"))
                .filters("", "i_manager_id == 1").build();

        ReadQuery finalQuery = api.read()
                .select("j1.d_year", "item.i_category_id", "item.i_category")
                .alias("d_year", "i_category_id", "i_category")
                .sum("j1.ss_ext_sales_price", "sum_ss_ext_sales_price")
                .from(ssInDec2000FromManager1, "src").build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("1", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ9_2(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery storeSalesInDec2000 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_ext_sales_price", "date_dim.d_year")
                .alias("ss_item_sk", "ss_ext_sales_price", "d_year")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 12 && d_year == 2000").build();

        ReadQuery ssInDec2000FromManager1 = api.join()
                .select("item.i_category_id", "item.i_category", "j1.ss_ext_sales_price", "j1.d_year")
                .sources(storeSalesInDec2000, "item", "j1", List.of("ss_item_sk"), List.of("i_item_sk"))
                .filters("", "i_manager_id == 1").stored().build();

        ssInDec2000FromManager1.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery finalQuery = api.read()
                .select("j1.d_year", "item.i_category_id", "item.i_category")
                .alias("d_year", "i_category_id", "i_category")
                .sum("j1.ss_ext_sales_price", "sum_ss_ext_sales_price")
                .from(ssInDec2000FromManager1, "src").build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("2", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ9_3(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery storeSalesInDec2000 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_ext_sales_price", "date_dim.d_year")
                .alias("ss_item_sk", "ss_ext_sales_price", "d_year")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 12 && d_year == 2000").build();

        ReadQuery ssInDec2000FromManager1 = api.join()
                .select("item.i_category_id", "item.i_category", "j1.ss_ext_sales_price", "j1.d_year")
                .sources(storeSalesInDec2000, "item", "j1", List.of("ss_item_sk"), List.of("i_item_sk"))
                .filters("", "i_manager_id == 1").build();

        ReadQuery finalQuery = api.read()
                .select("j1.d_year", "item.i_category_id", "item.i_category")
                .alias("d_year", "i_category_id", "i_category")
                .sum("j1.ss_ext_sales_price", "sum_ss_ext_sales_price")
                .from(ssInDec2000FromManager1, "src").store().build();

        finalQuery.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("3", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
}

/*-- start query 42 in stream 0 using template query42.tpl
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
          item.i_category; */
