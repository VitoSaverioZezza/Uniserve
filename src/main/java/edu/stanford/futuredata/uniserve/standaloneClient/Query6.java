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

public class Query6 {
    private final Broker broker;

    private String rootPath = "";
    public Query6(Broker broker, String rootPath){
        this.broker = broker;
        this.rootPath = rootPath;
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
                timings.put(0, runQ6_0());
                results = resultsMap.get("0");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 1:
                timings.put(1, runQ6_1());
                results = resultsMap.get("1");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 2:
                timings.put(2, runQ6_2());
                results = resultsMap.get("2");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 3:
                timings.put(3, runQ6_3());
                results = resultsMap.get("3");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 4:
                timings.put(4, runQ6_4());
                results = resultsMap.get("4");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 5:
                timings.put(5, runQ6_5());
                results = resultsMap.get("5");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
        }
        return timings;
    }

    public static final List<String> tablesToLoad = List.of(
            "catalog_sales",
            "customer_demographics",
            "date_dim",
            "item",
            "promotion"
    );



    public List<Pair<String, Long>> runQ6_0(){
        TestMethods tm = new TestMethods(rootPath);

        API api = new API(broker);

        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join()
                .select("catalog_sales.cs_sold_date_sk", "catalog_sales.cs_promo_sk", "catalog_sales.cs_quantity", "catalog_sales.cs_list_price", "catalog_sales.cs_coupon_amt", "catalog_sales.cs_sales_price", "catalog_sales.cs_item_sk")
                .alias("cs_sold_date_sk", "cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources("catalog_sales", "customer_demographics", List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'Secondary'")
                .build();

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join()
                .select("q1.cs_promo_sk", "q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 2000").build();

        ReadQuery csToMWSEdInPromotion = api.join()
                .select("q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "q1", List.of("cs_promo_sk"), List.of("p_promo_sk"))
                .filters("", "p_channel_email == 'N' || p_channel_event == 'N'")
                .build();

        ReadQuery finalJoin = api.join().select("item.i_item_id", "cs.cs_quantity", "cs.cs_list_price", "cs.cs_coupon_amt", "cs.cs_sales_price")
                .alias("i_item_id", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price")
                .sources(csToMWSEdInPromotion, "item", "cs", List.of("cs_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("0", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ6_1(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        for (String tableName : tablesToLoad) {
            int index = TPC_DS_Inv.names.indexOf(tableName);
            int shardNum = Math.min(Math.max(TPC_DS_Inv.sizes.get(index) / 1000, 1), Broker.SHARDS_PER_TABLE);
            api.createTable(tableName)
                    .attributes(TPC_DS_Inv.schemas.get(index).toArray(new String[0]))
                    .keys(TPC_DS_Inv.keys.get(index).toArray(new String[0]))
                    .shardNumber(shardNum)
                    .build().run();
        }

        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join()
                .select("catalog_sales.cs_sold_date_sk", "catalog_sales.cs_promo_sk", "catalog_sales.cs_quantity", "catalog_sales.cs_list_price", "catalog_sales.cs_coupon_amt", "catalog_sales.cs_sales_price", "catalog_sales.cs_item_sk")
                .alias("cs_sold_date_sk", "cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources("catalog_sales", "customer_demographics", List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'Secondary'")
                .stored().build();

        catSalesToMarriedWomenWithSecondaryEd.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join()
                .select("q1.cs_promo_sk", "q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 2000").build();

        ReadQuery csToMWSEdInPromotion = api.join()
                .select("q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "q1", List.of("cs_promo_sk"), List.of("p_promo_sk"))
                .filters("", "p_channel_email == 'N' || p_channel_event == 'N'")
                .build();

        ReadQuery finalJoin = api.join().select("item.i_item_id", "cs.cs_quantity", "cs.cs_list_price", "cs.cs_coupon_amt", "cs.cs_sales_price")
                .alias("i_item_id", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price")
                .sources(csToMWSEdInPromotion, "item", "cs", List.of("cs_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("1", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ6_2(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join()
                .select("catalog_sales.cs_sold_date_sk", "catalog_sales.cs_promo_sk", "catalog_sales.cs_quantity", "catalog_sales.cs_list_price", "catalog_sales.cs_coupon_amt", "catalog_sales.cs_sales_price", "catalog_sales.cs_item_sk")
                .alias("cs_sold_date_sk", "cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources("catalog_sales", "customer_demographics", List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'Secondary'")
                .build();

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join()
                .select("q1.cs_promo_sk", "q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 2000").stored().build();


        catSalesToMarriedWomenWithSecondaryEdIn2000.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery csToMWSEdInPromotion = api.join()
                .select("q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "q1", List.of("cs_promo_sk"), List.of("p_promo_sk"))
                .filters("", "p_channel_email == 'N' || p_channel_event == 'N'")
                .build();

        ReadQuery finalJoin = api.join().select("item.i_item_id", "cs.cs_quantity", "cs.cs_list_price", "cs.cs_coupon_amt", "cs.cs_sales_price")
                .alias("i_item_id", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price")
                .sources(csToMWSEdInPromotion, "item", "cs", List.of("cs_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("2", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ6_3(){
        TestMethods tm = new TestMethods(rootPath);

        API api = new API(broker);

        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join()
                .select("catalog_sales.cs_sold_date_sk", "catalog_sales.cs_promo_sk", "catalog_sales.cs_quantity", "catalog_sales.cs_list_price", "catalog_sales.cs_coupon_amt", "catalog_sales.cs_sales_price", "catalog_sales.cs_item_sk")
                .alias("cs_sold_date_sk", "cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources("catalog_sales", "customer_demographics", List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'Secondary'")
                .build();

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join()
                .select("q1.cs_promo_sk", "q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 2000").build();

        ReadQuery csToMWSEdInPromotion = api.join()
                .select("q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "q1", List.of("cs_promo_sk"), List.of("p_promo_sk"))
                .filters("", "p_channel_email == 'N' || p_channel_event == 'N'")
                .stored().build();

        csToMWSEdInPromotion.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery finalJoin = api.join().select("item.i_item_id", "cs.cs_quantity", "cs.cs_list_price", "cs.cs_coupon_amt", "cs.cs_sales_price")
                .alias("i_item_id", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price")
                .sources(csToMWSEdInPromotion, "item", "cs", List.of("cs_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("3", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ6_4(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);


        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join()
                .select("catalog_sales.cs_sold_date_sk", "catalog_sales.cs_promo_sk", "catalog_sales.cs_quantity", "catalog_sales.cs_list_price", "catalog_sales.cs_coupon_amt", "catalog_sales.cs_sales_price", "catalog_sales.cs_item_sk")
                .alias("cs_sold_date_sk", "cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources("catalog_sales", "customer_demographics", List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'Secondary'")
                .build();

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join()
                .select("q1.cs_promo_sk", "q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 2000").build();

        ReadQuery csToMWSEdInPromotion = api.join()
                .select("q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "q1", List.of("cs_promo_sk"), List.of("p_promo_sk"))
                .filters("", "p_channel_email == 'N' || p_channel_event == 'N'")
                .build();

        ReadQuery finalJoin = api.join().select("item.i_item_id", "cs.cs_quantity", "cs.cs_list_price", "cs.cs_coupon_amt", "cs.cs_sales_price")
                .alias("i_item_id", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price")
                .sources(csToMWSEdInPromotion, "item", "cs", List.of("cs_item_sk"), List.of("i_item_sk"))
                .stored().build();

        finalJoin.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("4", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ6_5(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery catSalesToMarriedWomenWithSecondaryEd = api.join()
                .select("catalog_sales.cs_sold_date_sk", "catalog_sales.cs_promo_sk", "catalog_sales.cs_quantity", "catalog_sales.cs_list_price", "catalog_sales.cs_coupon_amt", "catalog_sales.cs_sales_price", "catalog_sales.cs_item_sk")
                .alias("cs_sold_date_sk", "cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources("catalog_sales", "customer_demographics", List.of("cs_bill_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("","cd_gender == 'F' && cd_marital_status == 'W' && cd_education_status == 'Secondary'")
                .build();

        ReadQuery catSalesToMarriedWomenWithSecondaryEdIn2000 = api.join()
                .select("q1.cs_promo_sk", "q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_promo_sk", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEd, "date_dim", "q1", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 2000").build();

        ReadQuery csToMWSEdInPromotion = api.join()
                .select("q1.cs_quantity", "q1.cs_list_price", "q1.cs_coupon_amt", "q1.cs_sales_price", "q1.cs_item_sk")
                .alias("cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price", "cs_item_sk")
                .sources(catSalesToMarriedWomenWithSecondaryEdIn2000, "promotion", "q1", List.of("cs_promo_sk"), List.of("p_promo_sk"))
                .filters("", "p_channel_email == 'N' || p_channel_event == 'N'")
                .build();

        ReadQuery finalJoin = api.join().select("item.i_item_id", "cs.cs_quantity", "cs.cs_list_price", "cs.cs_coupon_amt", "cs.cs_sales_price")
                .alias("i_item_id", "cs_quantity", "cs_list_price", "cs_coupon_amt", "cs_sales_price")
                .sources(csToMWSEdInPromotion, "item", "cs", List.of("cs_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery finalQuery = api.read()
                .select("i_item_id")
                .avg("cs_quantity",    "agg1")
                .avg("cs_list_price",  "agg2")
                .avg("cs_coupon_amt",  "agg3")
                .avg("cs_sales_price", "agg4")
                .from(finalJoin, "src")
                .store().build();

        finalQuery.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("5", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
}
