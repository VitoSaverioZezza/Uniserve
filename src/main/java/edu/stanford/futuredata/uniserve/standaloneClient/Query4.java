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

public class Query4 {

    private final Broker broker;

    private String rootPath = "";

    public Query4(Broker broker, String rootPath){
        this.rootPath = rootPath;
        this.broker = broker;
    }
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
            int shardNum = Math.min(Math.max(TPC_DS_Inv.sizes.get(index) / 1000, 1),Broker.SHARDS_PER_TABLE);
            api.createTable(tableName)
                    .attributes(TPC_DS_Inv.schemas.get(index).toArray(new String[0]))
                    .keys(TPC_DS_Inv.keys.get(index).toArray(new String[0]))
                    .shardNumber(shardNum).build().run();
        }
        Map<Integer, List<Pair<String, Long>>> timings = new HashMap<>();
        RelReadQueryResults results = null;

        switch (queryIndex){
            case 0:
                timings.put(0, runQ4_0());
                results = resultsMap.get("0");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 1:
                timings.put(1, runQ4_1());
                results = resultsMap.get("1");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 2:
                timings.put(2, runQ4_2());
                results = resultsMap.get("2");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 3:
                timings.put(3, runQ4_3());
                results = resultsMap.get("3");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 4:
                timings.put(4, runQ4_4());
                results = resultsMap.get("4");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
        }
        return timings;
    }

    public final static List<String> tablesToLoad = List.of(
            "inventory",
            "date_dim",
            "item",
            "warehouse"
    );

    public List<Pair<String, Long>> runQ4_0(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);
        ReadQuery j1 = api.join()
                .select("inventory.inv_quantity_on_hand", "inventory.inv_item_sk", "inventory.inv_warehouse_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk", "inv_warehouse_sk")
                .sources("inventory", "date_dim", List.of("inv_date_sk"), List.of("d_date_sk"))
                .filters("", "d_month_seq >= 1205 && d_month_seq <= 1216").build();

        ReadQuery j2 = api.join()
                .select("j1p.inv_quantity_on_hand", "j1p.inv_item_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk")
                .sources(j1, "warehouse", "j1p", List.of("inv_warehouse_sk"), List.of("w_warehouse_sk")).build();

        ReadQuery j3 = api.join().sources(j2, "item", "j2", List.of("inv_item_sk"), List.of("i_item_sk")).build();

        ReadQuery finalQuery = api.read()
                .select("item.i_product_name", "item.i_brand", "item.i_class", "item.i_category")
                .avg("j2.inv_quantity_on_hand", "qoh")
                .alias("i_product_name", "i_brand", "i_class", "i_category")
                .from(j3, "j3").build();

        System.out.println("RESULTS:");
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("0", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ4_1(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);
        ReadQuery j1 = api.join()
                .select("inventory.inv_quantity_on_hand", "inventory.inv_item_sk", "inventory.inv_warehouse_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk", "inv_warehouse_sk")
                .sources("inventory", "date_dim", List.of("inv_date_sk"), List.of("d_date_sk"))
                .filters("", "d_month_seq >= 1205 && d_month_seq <= 1216").stored().build();

        j1.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery j2 = api.join()
                .select("j1p.inv_quantity_on_hand", "j1p.inv_item_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk")
                .sources(j1, "warehouse", "j1p", List.of("inv_warehouse_sk"), List.of("w_warehouse_sk")).build();

        ReadQuery j3 = api.join().sources(j2, "item", "j2", List.of("inv_item_sk"), List.of("i_item_sk")).build();

        ReadQuery finalQuery = api.read()
                .select("item.i_product_name", "item.i_brand", "item.i_class", "item.i_category")
                .avg("j2.inv_quantity_on_hand", "qoh")
                .alias("i_product_name", "i_brand", "i_class", "i_category")
                .from(j3, "j3").build();

        System.out.println("RESULTS:");
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("1", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ4_2(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);
        ReadQuery j1 = api.join()
                .select("inventory.inv_quantity_on_hand", "inventory.inv_item_sk", "inventory.inv_warehouse_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk", "inv_warehouse_sk")
                .sources("inventory", "date_dim", List.of("inv_date_sk"), List.of("d_date_sk"))
                .filters("", "d_month_seq >= 1205 && d_month_seq <= 1216").build();

        ReadQuery j2 = api.join()
                .select("j1p.inv_quantity_on_hand", "j1p.inv_item_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk")
                .stored()
                .sources(j1, "warehouse", "j1p", List.of("inv_warehouse_sk"), List.of("w_warehouse_sk")).build();

        j2.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery j3 = api.join().sources(j2, "item", "j2", List.of("inv_item_sk"), List.of("i_item_sk")).build();

        ReadQuery finalQuery = api.read()
                .select("item.i_product_name", "item.i_brand", "item.i_class", "item.i_category")
                .avg("j2.inv_quantity_on_hand", "qoh")
                .alias("i_product_name", "i_brand", "i_class", "i_category")
                .from(j3, "j3").build();

        System.out.println("RESULTS:");
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("2", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ4_3(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);
        ReadQuery j1 = api.join()
                .select("inventory.inv_quantity_on_hand", "inventory.inv_item_sk", "inventory.inv_warehouse_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk", "inv_warehouse_sk")
                .sources("inventory", "date_dim", List.of("inv_date_sk"), List.of("d_date_sk"))
                .filters("", "d_month_seq >= 1205 && d_month_seq <= 1216").build();

        ReadQuery j2 = api.join()
                .select("j1p.inv_quantity_on_hand", "j1p.inv_item_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk")
                .sources(j1, "warehouse", "j1p", List.of("inv_warehouse_sk"), List.of("w_warehouse_sk")).build();

        ReadQuery j3 = api.join()
                .select("item.i_product_name", "item.i_brand", "item.i_class", "item.i_category", "j2.inv_quantity_on_hand")
                .sources(j2, "item", "j2", List.of("inv_item_sk"), List.of("i_item_sk"))
                .stored().build();

        j3.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);


        ReadQuery finalQuery = api.read()
                .select("item.i_product_name", "item.i_brand", "item.i_class", "item.i_category")
                .avg("j2.inv_quantity_on_hand", "qoh")
                .alias("i_product_name", "i_brand", "i_class", "i_category")
                .from(j3, "j3").build();

        System.out.println("RESULTS:");
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("3", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ4_4(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);
        ReadQuery j1 = api.join()
                .select("inventory.inv_quantity_on_hand", "inventory.inv_item_sk", "inventory.inv_warehouse_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk", "inv_warehouse_sk")
                .sources("inventory", "date_dim", List.of("inv_date_sk"), List.of("d_date_sk"))
                .filters("", "d_month_seq >= 1205 && d_month_seq <= 1216").build();

        ReadQuery j2 = api.join()
                .select("j1p.inv_quantity_on_hand", "j1p.inv_item_sk")
                .alias("inv_quantity_on_hand", "inv_item_sk")
                .sources(j1, "warehouse", "j1p", List.of("inv_warehouse_sk"), List.of("w_warehouse_sk")).build();

        ReadQuery j3 = api.join().sources(j2, "item", "j2", List.of("inv_item_sk"), List.of("i_item_sk")).build();

        ReadQuery finalQuery = api.read()
                .select("item.i_product_name", "item.i_brand", "item.i_class", "item.i_category")
                .avg("j2.inv_quantity_on_hand", "qoh")
                .alias("i_product_name", "i_brand", "i_class", "i_category")
                .from(j3, "j3").store().build();

        finalQuery.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);
        System.out.println("RESULTS:");
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("4", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
}

/*
* -- start query 22 in stream 0 using template query22.tpl
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
GROUP  BY i_product_name, i_brand, i_class, i_category
*/