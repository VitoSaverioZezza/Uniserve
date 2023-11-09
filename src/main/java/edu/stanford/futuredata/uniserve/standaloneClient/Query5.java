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

public class Query5 {
    private final Broker broker;

    public Query5(Broker broker, String rootPath) {

        this.rootPath = rootPath;
        this.broker = broker;
    }
    private int queryIndex = -1;

    public void setQueryIndex(int queryIndex) {
        this.queryIndex = queryIndex;
    }
    private String rootPath = "";
    public final static List<String> tablesToLoad = List.of(
            "store_sales",
            "store_returns",
            "catalog_sales",
            "date_dim",
            "store",
            "item"
    );
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
                timings.put(0, runQ5_0());
                results = resultsMap.get("0");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 1:
                timings.put(1, runQ5_1());
                results = resultsMap.get("1");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 2:
                timings.put(2, runQ5_2());
                results = resultsMap.get("2");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 3:
                timings.put(3, runQ5_3());
                results = resultsMap.get("3");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 4:
                timings.put(4, runQ5_4());
                results = resultsMap.get("4");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 5:
                timings.put(5, runQ5_5());
                results = resultsMap.get("5");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 6:
                timings.put(6, runQ5_6());
                results = resultsMap.get("6");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 7:
                timings.put(7, runQ5_7());
                results = resultsMap.get("7");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 8:
                timings.put(8, runQ5_8());
                results = resultsMap.get("8");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
        }
        return timings;
    }

    public List<Pair<String, Long>> runQ5_0(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery storeSalesInApril2001 = api.join()
                .select("store_sales.ss_net_profit", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "store_sales.ss_ticket_number")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "ss_ticket_number")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 4 && d_year == 2001").build();

        ReadQuery storeReturnsIn2001AfterApril = api.join()
                .select("store_returns.sr_net_loss", "store_returns.sr_customer_sk", "store_returns.sr_item_sk", "store_returns.sr_ticket_number")
                .alias("sr_net_loss", "sr_customer_sk", "sr_item_sk", "sr_ticket_number")
                .sources("store_returns", "date_dim", List.of("sr_returned_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery catalogSalesIn2001AfterApril = api.join()
                .select("catalog_sales.cs_net_profit", "catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_item_sk")
                .alias("cs_net_profit", "cs_bill_customer_sk", "cs_item_sk")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery returnedStoreAprilSales = api.join()
                .select("sales.ss_net_profit", "sales.ss_item_sk", "sales.ss_store_sk", "sales.ss_customer_sk", "returns.sr_net_loss")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "sr_net_loss")
                .sources(storeSalesInApril2001, storeReturnsIn2001AfterApril, "sales", "returns", List.of("ss_customer_sk", "ss_item_sk", "ss_ticket_number"), List.of("sr_customer_sk", "sr_item_sk", "sr_ticket_number"))
                .build();

        ReadQuery returnedStoreSalesInAprilWhoLaterBoughtOnCatalog = api.join()
                .select("A.sr_net_loss", "A.ss_net_profit", "A.ss_item_sk", "A.ss_store_sk", "B.cs_net_profit")
                .alias("sr_net_loss", "ss_net_profit", "ss_item_sk", "ss_store_sk", "cs_net_profit")
                .sources(returnedStoreAprilSales, catalogSalesIn2001AfterApril, "A", "B", List.of("ss_customer_sk", "ss_item_sk"), List.of("cs_bill_customer_sk", "cs_item_sk"))
                .build();

        ReadQuery joinItem = api.join()
                .select("item.i_item_id", "item.i_item_desc", "C.ss_net_profit", "C.cs_net_profit", "C.sr_net_loss", "C.ss_store_sk")
                .alias("i_item_id", "i_item_desc", "ss_net_profit", "cs_net_profit", "sr_net_loss", "ss_store_sk")
                .sources(returnedStoreSalesInAprilWhoLaterBoughtOnCatalog, "item", "C", List.of("ss_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery joinStore = api.join()
                .select("j1.i_item_id", "j1.i_item_desc", "store.s_store_id", "store.s_store_name", "j1.ss_net_profit", "j1.sr_net_loss", "j1.cs_net_profit")
                .alias("i_item_id", "i_item_desc", "s_store_id", "s_store_name", "ss_net_profit", "sr_net_loss", "cs_net_profit")
                .sources(joinItem, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();

        ReadQuery finalQuery = api.read().select("i_item_id", "i_item_desc", "s_store_id", "s_store_name")
                .max("ss_net_profit", "store_sales_profit")
                .max("sr_net_loss", "store_returns_loss")
                .max("cs_net_profit", "catalog_sales_profit")
                .from(joinStore, "src")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("0", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }

    public List<Pair<String, Long>> runQ5_1(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery storeSalesInApril2001 = api.join()
                .select("store_sales.ss_net_profit", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "store_sales.ss_ticket_number")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "ss_ticket_number")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 4 && d_year == 2001").stored().build();

        storeSalesInApril2001.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery storeReturnsIn2001AfterApril = api.join()
                .select("store_returns.sr_net_loss", "store_returns.sr_customer_sk", "store_returns.sr_item_sk", "store_returns.sr_ticket_number")
                .alias("sr_net_loss", "sr_customer_sk", "sr_item_sk", "sr_ticket_number")
                .sources("store_returns", "date_dim", List.of("sr_returned_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery catalogSalesIn2001AfterApril = api.join()
                .select("catalog_sales.cs_net_profit", "catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_item_sk")
                .alias("cs_net_profit", "cs_bill_customer_sk", "cs_item_sk")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery returnedStoreAprilSales = api.join()
                .select("sales.ss_net_profit", "sales.ss_item_sk", "sales.ss_store_sk", "sales.ss_customer_sk", "returns.sr_net_loss")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "sr_net_loss")
                .sources(storeSalesInApril2001, storeReturnsIn2001AfterApril, "sales", "returns", List.of("ss_customer_sk", "ss_item_sk", "ss_ticket_number"), List.of("sr_customer_sk", "sr_item_sk", "sr_ticket_number"))
                .build();

        ReadQuery returnedStoreSalesInAprilWhoLaterBoughtOnCatalog = api.join()
                .select("A.sr_net_loss", "A.ss_net_profit", "A.ss_item_sk", "A.ss_store_sk", "B.cs_net_profit")
                .alias("sr_net_loss", "ss_net_profit", "ss_item_sk", "ss_store_sk", "cs_net_profit")
                .sources(returnedStoreAprilSales, catalogSalesIn2001AfterApril, "A", "B", List.of("ss_customer_sk", "ss_item_sk"), List.of("cs_bill_customer_sk", "cs_item_sk"))
                .build();

        ReadQuery joinItem = api.join()
                .select("item.i_item_id", "item.i_item_desc", "C.ss_net_profit", "C.cs_net_profit", "C.sr_net_loss", "C.ss_store_sk")
                .alias("i_item_id", "i_item_desc", "ss_net_profit", "cs_net_profit", "sr_net_loss", "ss_store_sk")
                .sources(returnedStoreSalesInAprilWhoLaterBoughtOnCatalog, "item", "C", List.of("ss_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery joinStore = api.join()
                .select("j1.i_item_id", "j1.i_item_desc", "store.s_store_id", "store.s_store_name", "j1.ss_net_profit", "j1.sr_net_loss", "j1.cs_net_profit")
                .alias("i_item_id", "i_item_desc", "s_store_id", "s_store_name", "ss_net_profit", "sr_net_loss", "cs_net_profit")
                .sources(joinItem, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();

        ReadQuery finalQuery = api.read().select("i_item_id", "i_item_desc", "s_store_id", "s_store_name")
                .max("ss_net_profit", "store_sales_profit")
                .max("sr_net_loss", "store_returns_loss")
                .max("cs_net_profit", "catalog_sales_profit")
                .from(joinStore, "src")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("1", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ5_2(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery storeSalesInApril2001 = api.join()
                .select("store_sales.ss_net_profit", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "store_sales.ss_ticket_number")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "ss_ticket_number")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 4 && d_year == 2001").build();

        ReadQuery storeReturnsIn2001AfterApril = api.join()
                .select("store_returns.sr_net_loss", "store_returns.sr_customer_sk", "store_returns.sr_item_sk", "store_returns.sr_ticket_number")
                .alias("sr_net_loss", "sr_customer_sk", "sr_item_sk", "sr_ticket_number")
                .sources("store_returns", "date_dim", List.of("sr_returned_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001").stored()
                .build();

        storeReturnsIn2001AfterApril.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery catalogSalesIn2001AfterApril = api.join()
                .select("catalog_sales.cs_net_profit", "catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_item_sk")
                .alias("cs_net_profit", "cs_bill_customer_sk", "cs_item_sk")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery returnedStoreAprilSales = api.join()
                .select("sales.ss_net_profit", "sales.ss_item_sk", "sales.ss_store_sk", "sales.ss_customer_sk", "returns.sr_net_loss")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "sr_net_loss")
                .sources(storeSalesInApril2001, storeReturnsIn2001AfterApril, "sales", "returns", List.of("ss_customer_sk", "ss_item_sk", "ss_ticket_number"), List.of("sr_customer_sk", "sr_item_sk", "sr_ticket_number"))
                .build();

        ReadQuery returnedStoreSalesInAprilWhoLaterBoughtOnCatalog = api.join()
                .select("A.sr_net_loss", "A.ss_net_profit", "A.ss_item_sk", "A.ss_store_sk", "B.cs_net_profit")
                .alias("sr_net_loss", "ss_net_profit", "ss_item_sk", "ss_store_sk", "cs_net_profit")
                .sources(returnedStoreAprilSales, catalogSalesIn2001AfterApril, "A", "B", List.of("ss_customer_sk", "ss_item_sk"), List.of("cs_bill_customer_sk", "cs_item_sk"))
                .build();

        ReadQuery joinItem = api.join()
                .select("item.i_item_id", "item.i_item_desc", "C.ss_net_profit", "C.cs_net_profit", "C.sr_net_loss", "C.ss_store_sk")
                .alias("i_item_id", "i_item_desc", "ss_net_profit", "cs_net_profit", "sr_net_loss", "ss_store_sk")
                .sources(returnedStoreSalesInAprilWhoLaterBoughtOnCatalog, "item", "C", List.of("ss_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery joinStore = api.join()
                .select("j1.i_item_id", "j1.i_item_desc", "store.s_store_id", "store.s_store_name", "j1.ss_net_profit", "j1.sr_net_loss", "j1.cs_net_profit")
                .alias("i_item_id", "i_item_desc", "s_store_id", "s_store_name", "ss_net_profit", "sr_net_loss", "cs_net_profit")
                .sources(joinItem, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();

        ReadQuery finalQuery = api.read().select("i_item_id", "i_item_desc", "s_store_id", "s_store_name")
                .max("ss_net_profit", "store_sales_profit")
                .max("sr_net_loss", "store_returns_loss")
                .max("cs_net_profit", "catalog_sales_profit")
                .from(joinStore, "src")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("2", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ5_3(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery storeSalesInApril2001 = api.join()
                .select("store_sales.ss_net_profit", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "store_sales.ss_ticket_number")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "ss_ticket_number")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 4 && d_year == 2001").build();

        ReadQuery storeReturnsIn2001AfterApril = api.join()
                .select("store_returns.sr_net_loss", "store_returns.sr_customer_sk", "store_returns.sr_item_sk", "store_returns.sr_ticket_number")
                .alias("sr_net_loss", "sr_customer_sk", "sr_item_sk", "sr_ticket_number")
                .sources("store_returns", "date_dim", List.of("sr_returned_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery catalogSalesIn2001AfterApril = api.join()
                .select("catalog_sales.cs_net_profit", "catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_item_sk")
                .alias("cs_net_profit", "cs_bill_customer_sk", "cs_item_sk")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .stored().build();

        catalogSalesIn2001AfterApril.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery returnedStoreAprilSales = api.join()
                .select("sales.ss_net_profit", "sales.ss_item_sk", "sales.ss_store_sk", "sales.ss_customer_sk", "returns.sr_net_loss")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "sr_net_loss")
                .sources(storeSalesInApril2001, storeReturnsIn2001AfterApril, "sales", "returns", List.of("ss_customer_sk", "ss_item_sk", "ss_ticket_number"), List.of("sr_customer_sk", "sr_item_sk", "sr_ticket_number"))
                .build();

        ReadQuery returnedStoreSalesInAprilWhoLaterBoughtOnCatalog = api.join()
                .select("A.sr_net_loss", "A.ss_net_profit", "A.ss_item_sk", "A.ss_store_sk", "B.cs_net_profit")
                .alias("sr_net_loss", "ss_net_profit", "ss_item_sk", "ss_store_sk", "cs_net_profit")
                .sources(returnedStoreAprilSales, catalogSalesIn2001AfterApril, "A", "B", List.of("ss_customer_sk", "ss_item_sk"), List.of("cs_bill_customer_sk", "cs_item_sk"))
                .build();

        ReadQuery joinItem = api.join()
                .select("item.i_item_id", "item.i_item_desc", "C.ss_net_profit", "C.cs_net_profit", "C.sr_net_loss", "C.ss_store_sk")
                .alias("i_item_id", "i_item_desc", "ss_net_profit", "cs_net_profit", "sr_net_loss", "ss_store_sk")
                .sources(returnedStoreSalesInAprilWhoLaterBoughtOnCatalog, "item", "C", List.of("ss_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery joinStore = api.join()
                .select("j1.i_item_id", "j1.i_item_desc", "store.s_store_id", "store.s_store_name", "j1.ss_net_profit", "j1.sr_net_loss", "j1.cs_net_profit")
                .alias("i_item_id", "i_item_desc", "s_store_id", "s_store_name", "ss_net_profit", "sr_net_loss", "cs_net_profit")
                .sources(joinItem, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();

        ReadQuery finalQuery = api.read().select("i_item_id", "i_item_desc", "s_store_id", "s_store_name")
                .max("ss_net_profit", "store_sales_profit")
                .max("sr_net_loss", "store_returns_loss")
                .max("cs_net_profit", "catalog_sales_profit")
                .from(joinStore, "src")
                .build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("3", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ5_4(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery storeSalesInApril2001 = api.join()
                .select("store_sales.ss_net_profit", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "store_sales.ss_ticket_number")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "ss_ticket_number")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 4 && d_year == 2001").build();

        ReadQuery storeReturnsIn2001AfterApril = api.join()
                .select("store_returns.sr_net_loss", "store_returns.sr_customer_sk", "store_returns.sr_item_sk", "store_returns.sr_ticket_number")
                .alias("sr_net_loss", "sr_customer_sk", "sr_item_sk", "sr_ticket_number")
                .sources("store_returns", "date_dim", List.of("sr_returned_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery catalogSalesIn2001AfterApril = api.join()
                .select("catalog_sales.cs_net_profit", "catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_item_sk")
                .alias("cs_net_profit", "cs_bill_customer_sk", "cs_item_sk")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery returnedStoreAprilSales = api.join()
                .select("sales.ss_net_profit", "sales.ss_item_sk", "sales.ss_store_sk", "sales.ss_customer_sk", "returns.sr_net_loss")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "sr_net_loss")
                .sources(storeSalesInApril2001, storeReturnsIn2001AfterApril, "sales", "returns", List.of("ss_customer_sk", "ss_item_sk", "ss_ticket_number"), List.of("sr_customer_sk", "sr_item_sk", "sr_ticket_number"))
                .stored().build();

        returnedStoreAprilSales.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery returnedStoreSalesInAprilWhoLaterBoughtOnCatalog = api.join()
                .select("A.sr_net_loss", "A.ss_net_profit", "A.ss_item_sk", "A.ss_store_sk", "B.cs_net_profit")
                .alias("sr_net_loss", "ss_net_profit", "ss_item_sk", "ss_store_sk", "cs_net_profit")
                .sources(returnedStoreAprilSales, catalogSalesIn2001AfterApril, "A", "B", List.of("ss_customer_sk", "ss_item_sk"), List.of("cs_bill_customer_sk", "cs_item_sk"))
                .build();

        ReadQuery joinItem = api.join()
                .select("item.i_item_id", "item.i_item_desc", "C.ss_net_profit", "C.cs_net_profit", "C.sr_net_loss", "C.ss_store_sk")
                .alias("i_item_id", "i_item_desc", "ss_net_profit", "cs_net_profit", "sr_net_loss", "ss_store_sk")
                .sources(returnedStoreSalesInAprilWhoLaterBoughtOnCatalog, "item", "C", List.of("ss_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery joinStore = api.join()
                .select("j1.i_item_id", "j1.i_item_desc", "store.s_store_id", "store.s_store_name", "j1.ss_net_profit", "j1.sr_net_loss", "j1.cs_net_profit")
                .alias("i_item_id", "i_item_desc", "s_store_id", "s_store_name", "ss_net_profit", "sr_net_loss", "cs_net_profit")
                .sources(joinItem, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();

        ReadQuery finalQuery = api.read().select("i_item_id", "i_item_desc", "s_store_id", "s_store_name")
                .max("ss_net_profit", "store_sales_profit")
                .max("sr_net_loss", "store_returns_loss")
                .max("cs_net_profit", "catalog_sales_profit")
                .from(joinStore, "src")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("4", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ5_5(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery storeSalesInApril2001 = api.join()
                .select("store_sales.ss_net_profit", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "store_sales.ss_ticket_number")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "ss_ticket_number")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 4 && d_year == 2001").build();

        ReadQuery storeReturnsIn2001AfterApril = api.join()
                .select("store_returns.sr_net_loss", "store_returns.sr_customer_sk", "store_returns.sr_item_sk", "store_returns.sr_ticket_number")
                .alias("sr_net_loss", "sr_customer_sk", "sr_item_sk", "sr_ticket_number")
                .sources("store_returns", "date_dim", List.of("sr_returned_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery catalogSalesIn2001AfterApril = api.join()
                .select("catalog_sales.cs_net_profit", "catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_item_sk")
                .alias("cs_net_profit", "cs_bill_customer_sk", "cs_item_sk")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery returnedStoreAprilSales = api.join()
                .select("sales.ss_net_profit", "sales.ss_item_sk", "sales.ss_store_sk", "sales.ss_customer_sk", "returns.sr_net_loss")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "sr_net_loss")
                .sources(storeSalesInApril2001, storeReturnsIn2001AfterApril, "sales", "returns", List.of("ss_customer_sk", "ss_item_sk", "ss_ticket_number"), List.of("sr_customer_sk", "sr_item_sk", "sr_ticket_number"))
                .build();

        ReadQuery returnedStoreSalesInAprilWhoLaterBoughtOnCatalog = api.join()
                .select("A.sr_net_loss", "A.ss_net_profit", "A.ss_item_sk", "A.ss_store_sk", "B.cs_net_profit")
                .alias("sr_net_loss", "ss_net_profit", "ss_item_sk", "ss_store_sk", "cs_net_profit")
                .sources(returnedStoreAprilSales, catalogSalesIn2001AfterApril, "A", "B", List.of("ss_customer_sk", "ss_item_sk"), List.of("cs_bill_customer_sk", "cs_item_sk"))
                .stored().build();


        returnedStoreSalesInAprilWhoLaterBoughtOnCatalog.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery joinItem = api.join()
                .select("item.i_item_id", "item.i_item_desc", "C.ss_net_profit", "C.cs_net_profit", "C.sr_net_loss", "C.ss_store_sk")
                .alias("i_item_id", "i_item_desc", "ss_net_profit", "cs_net_profit", "sr_net_loss", "ss_store_sk")
                .sources(returnedStoreSalesInAprilWhoLaterBoughtOnCatalog, "item", "C", List.of("ss_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery joinStore = api.join()
                .select("j1.i_item_id", "j1.i_item_desc", "store.s_store_id", "store.s_store_name", "j1.ss_net_profit", "j1.sr_net_loss", "j1.cs_net_profit")
                .alias("i_item_id", "i_item_desc", "s_store_id", "s_store_name", "ss_net_profit", "sr_net_loss", "cs_net_profit")
                .sources(joinItem, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();

        ReadQuery finalQuery = api.read().select("i_item_id", "i_item_desc", "s_store_id", "s_store_name")
                .max("ss_net_profit", "store_sales_profit")
                .max("sr_net_loss", "store_returns_loss")
                .max("cs_net_profit", "catalog_sales_profit")
                .from(joinStore, "src")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("5", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ5_6(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery storeSalesInApril2001 = api.join()
                .select("store_sales.ss_net_profit", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "store_sales.ss_ticket_number")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "ss_ticket_number")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 4 && d_year == 2001").build();

        ReadQuery storeReturnsIn2001AfterApril = api.join()
                .select("store_returns.sr_net_loss", "store_returns.sr_customer_sk", "store_returns.sr_item_sk", "store_returns.sr_ticket_number")
                .alias("sr_net_loss", "sr_customer_sk", "sr_item_sk", "sr_ticket_number")
                .sources("store_returns", "date_dim", List.of("sr_returned_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery catalogSalesIn2001AfterApril = api.join()
                .select("catalog_sales.cs_net_profit", "catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_item_sk")
                .alias("cs_net_profit", "cs_bill_customer_sk", "cs_item_sk")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery returnedStoreAprilSales = api.join()
                .select("sales.ss_net_profit", "sales.ss_item_sk", "sales.ss_store_sk", "sales.ss_customer_sk", "returns.sr_net_loss")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "sr_net_loss")
                .sources(storeSalesInApril2001, storeReturnsIn2001AfterApril, "sales", "returns", List.of("ss_customer_sk", "ss_item_sk", "ss_ticket_number"), List.of("sr_customer_sk", "sr_item_sk", "sr_ticket_number"))
                .build();

        ReadQuery returnedStoreSalesInAprilWhoLaterBoughtOnCatalog = api.join()
                .select("A.sr_net_loss", "A.ss_net_profit", "A.ss_item_sk", "A.ss_store_sk", "B.cs_net_profit")
                .alias("sr_net_loss", "ss_net_profit", "ss_item_sk", "ss_store_sk", "cs_net_profit")
                .sources(returnedStoreAprilSales, catalogSalesIn2001AfterApril, "A", "B", List.of("ss_customer_sk", "ss_item_sk"), List.of("cs_bill_customer_sk", "cs_item_sk"))
                .build();

        ReadQuery joinItem = api.join()
                .select("item.i_item_id", "item.i_item_desc", "C.ss_net_profit", "C.cs_net_profit", "C.sr_net_loss", "C.ss_store_sk")
                .alias("i_item_id", "i_item_desc", "ss_net_profit", "cs_net_profit", "sr_net_loss", "ss_store_sk")
                .sources(returnedStoreSalesInAprilWhoLaterBoughtOnCatalog, "item", "C", List.of("ss_item_sk"), List.of("i_item_sk"))
                .stored().build();

        joinItem.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery joinStore = api.join()
                .select("j1.i_item_id", "j1.i_item_desc", "store.s_store_id", "store.s_store_name", "j1.ss_net_profit", "j1.sr_net_loss", "j1.cs_net_profit")
                .alias("i_item_id", "i_item_desc", "s_store_id", "s_store_name", "ss_net_profit", "sr_net_loss", "cs_net_profit")
                .sources(joinItem, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();

        ReadQuery finalQuery = api.read().select("i_item_id", "i_item_desc", "s_store_id", "s_store_name")
                .max("ss_net_profit", "store_sales_profit")
                .max("sr_net_loss", "store_returns_loss")
                .max("cs_net_profit", "catalog_sales_profit")
                .from(joinStore, "src")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("6", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ5_7(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery storeSalesInApril2001 = api.join()
                .select("store_sales.ss_net_profit", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "store_sales.ss_ticket_number")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "ss_ticket_number")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 4 && d_year == 2001").build();

        ReadQuery storeReturnsIn2001AfterApril = api.join()
                .select("store_returns.sr_net_loss", "store_returns.sr_customer_sk", "store_returns.sr_item_sk", "store_returns.sr_ticket_number")
                .alias("sr_net_loss", "sr_customer_sk", "sr_item_sk", "sr_ticket_number")
                .sources("store_returns", "date_dim", List.of("sr_returned_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery catalogSalesIn2001AfterApril = api.join()
                .select("catalog_sales.cs_net_profit", "catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_item_sk")
                .alias("cs_net_profit", "cs_bill_customer_sk", "cs_item_sk")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery returnedStoreAprilSales = api.join()
                .select("sales.ss_net_profit", "sales.ss_item_sk", "sales.ss_store_sk", "sales.ss_customer_sk", "returns.sr_net_loss")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "sr_net_loss")
                .sources(storeSalesInApril2001, storeReturnsIn2001AfterApril, "sales", "returns", List.of("ss_customer_sk", "ss_item_sk", "ss_ticket_number"), List.of("sr_customer_sk", "sr_item_sk", "sr_ticket_number"))
                .build();

        ReadQuery returnedStoreSalesInAprilWhoLaterBoughtOnCatalog = api.join()
                .select("A.sr_net_loss", "A.ss_net_profit", "A.ss_item_sk", "A.ss_store_sk", "B.cs_net_profit")
                .alias("sr_net_loss", "ss_net_profit", "ss_item_sk", "ss_store_sk", "cs_net_profit")
                .sources(returnedStoreAprilSales, catalogSalesIn2001AfterApril, "A", "B", List.of("ss_customer_sk", "ss_item_sk"), List.of("cs_bill_customer_sk", "cs_item_sk"))
                .build();

        ReadQuery joinItem = api.join()
                .select("item.i_item_id", "item.i_item_desc", "C.ss_net_profit", "C.cs_net_profit", "C.sr_net_loss", "C.ss_store_sk")
                .alias("i_item_id", "i_item_desc", "ss_net_profit", "cs_net_profit", "sr_net_loss", "ss_store_sk")
                .sources(returnedStoreSalesInAprilWhoLaterBoughtOnCatalog, "item", "C", List.of("ss_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery joinStore = api.join()
                .select("j1.i_item_id", "j1.i_item_desc", "store.s_store_id", "store.s_store_name", "j1.ss_net_profit", "j1.sr_net_loss", "j1.cs_net_profit")
                .alias("i_item_id", "i_item_desc", "s_store_id", "s_store_name", "ss_net_profit", "sr_net_loss", "cs_net_profit")
                .sources(joinItem, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .stored().build();

        joinStore.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery finalQuery = api.read().select("i_item_id", "i_item_desc", "s_store_id", "s_store_name")
                .max("ss_net_profit", "store_sales_profit")
                .max("sr_net_loss", "store_returns_loss")
                .max("cs_net_profit", "catalog_sales_profit")
                .from(joinStore, "src")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("7", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ5_8(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery storeSalesInApril2001 = api.join()
                .select("store_sales.ss_net_profit", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "store_sales.ss_ticket_number")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "ss_ticket_number")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy == 4 && d_year == 2001").build();

        ReadQuery storeReturnsIn2001AfterApril = api.join()
                .select("store_returns.sr_net_loss", "store_returns.sr_customer_sk", "store_returns.sr_item_sk", "store_returns.sr_ticket_number")
                .alias("sr_net_loss", "sr_customer_sk", "sr_item_sk", "sr_ticket_number")
                .sources("store_returns", "date_dim", List.of("sr_returned_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery catalogSalesIn2001AfterApril = api.join()
                .select("catalog_sales.cs_net_profit", "catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_item_sk")
                .alias("cs_net_profit", "cs_bill_customer_sk", "cs_item_sk")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_moy >= 4 && d_moy <= 12 && d_year == 2001")
                .build();

        ReadQuery returnedStoreAprilSales = api.join()
                .select("sales.ss_net_profit", "sales.ss_item_sk", "sales.ss_store_sk", "sales.ss_customer_sk", "returns.sr_net_loss")
                .alias("ss_net_profit", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "sr_net_loss")
                .sources(storeSalesInApril2001, storeReturnsIn2001AfterApril, "sales", "returns", List.of("ss_customer_sk", "ss_item_sk", "ss_ticket_number"), List.of("sr_customer_sk", "sr_item_sk", "sr_ticket_number"))
                .build();

        ReadQuery returnedStoreSalesInAprilWhoLaterBoughtOnCatalog = api.join()
                .select("A.sr_net_loss", "A.ss_net_profit", "A.ss_item_sk", "A.ss_store_sk", "B.cs_net_profit")
                .alias("sr_net_loss", "ss_net_profit", "ss_item_sk", "ss_store_sk", "cs_net_profit")
                .sources(returnedStoreAprilSales, catalogSalesIn2001AfterApril, "A", "B", List.of("ss_customer_sk", "ss_item_sk"), List.of("cs_bill_customer_sk", "cs_item_sk"))
                .build();

        ReadQuery joinItem = api.join()
                .select("item.i_item_id", "item.i_item_desc", "C.ss_net_profit", "C.cs_net_profit", "C.sr_net_loss", "C.ss_store_sk")
                .alias("i_item_id", "i_item_desc", "ss_net_profit", "cs_net_profit", "sr_net_loss", "ss_store_sk")
                .sources(returnedStoreSalesInAprilWhoLaterBoughtOnCatalog, "item", "C", List.of("ss_item_sk"), List.of("i_item_sk"))
                .build();

        ReadQuery joinStore = api.join()
                .select("j1.i_item_id", "j1.i_item_desc", "store.s_store_id", "store.s_store_name", "j1.ss_net_profit", "j1.sr_net_loss", "j1.cs_net_profit")
                .alias("i_item_id", "i_item_desc", "s_store_id", "s_store_name", "ss_net_profit", "sr_net_loss", "cs_net_profit")
                .sources(joinItem, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();

        ReadQuery finalQuery = api.read().select("i_item_id", "i_item_desc", "s_store_id", "s_store_name")
                .max("ss_net_profit", "store_sales_profit")
                .max("sr_net_loss", "store_returns_loss")
                .max("cs_net_profit", "catalog_sales_profit")
                .from(joinStore, "src").store()
                .build();

        finalQuery.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("8", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
}

/*-- start query 25 in stream 0 using template query25.tpl
SELECT         "i_item_id",
               "i_item_desc",
               "s_store_id",
               "s_store_name",
               Max(ss_net_profit) AS store_sales_profit,
               Max(sr_net_loss)   AS store_returns_loss,
               Max(cs_net_profit) AS catalog_sales_profit
FROM   store_sales,
       store_returns,
       catalog_sales,
       date_dim d1,
       date_dim d2,
       date_dim d3,
       store,
       item
WHERE  d1.d_moy = 4
       AND d1.d_year = 2001
       AND d1.d_date_sk = ss_sold_date_sk
       AND i_item_sk = ss_item_sk
       AND s_store_sk = ss_store_sk
       AND ss_customer_sk = sr_customer_sk
       AND ss_item_sk = sr_item_sk
       AND ss_ticket_number = sr_ticket_number
       AND sr_returned_date_sk = d2.d_date_sk
       AND d2.d_moy BETWEEN 4 AND 10
       AND d2.d_year = 2001
       AND sr_customer_sk = cs_bill_customer_sk
       AND sr_item_sk = cs_item_sk
       AND cs_sold_date_sk = d3.d_date_sk
       AND d3.d_moy BETWEEN 4 AND 10
       AND d3.d_year = 2001
GROUP  BY i_item_id,
          i_item_desc,
          s_store_id,
          s_store_name
ORDER  BY i_item_id,
          i_item_desc,
          s_store_id,
          s_store_name
LIMIT 100; */
