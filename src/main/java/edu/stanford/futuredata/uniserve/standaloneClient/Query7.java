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

public class Query7 {
    private final Broker broker;
    private String rootPath = "";
    public Query7(Broker broker, String rootPath) {
        this.broker = broker;
        this.rootPath = rootPath;
    }
    public static final List<String> tablesToLoad = List.of(
            "catalog_sales",
            "web_sales",
            "store_sales",
            "date_dim",
            "item",
            "customer_address"
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
            int shardNum = Math.min(Math.max(TPC_DS_Inv.sizes.get(index) / 10000, 1),Broker.SHARDS_PER_TABLE);
            api.createTable(tableName)
                    .attributes(TPC_DS_Inv.schemas.get(index).toArray(new String[0]))
                    .keys(TPC_DS_Inv.keys.get(index).toArray(new String[0]))
                    .shardNumber(shardNum).build().run();
        }
        Map<Integer, List<Pair<String, Long>>> timings = new HashMap<>();
        RelReadQueryResults results = null;

        switch (queryIndex){
            case 0:
                timings.put(0, runQ7_0());
                results = resultsMap.get("0");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 1:
                timings.put(1, runQ7_1());
                results = resultsMap.get("1");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 2:
                timings.put(2, runQ7_2());
                results = resultsMap.get("2");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 3:
                timings.put(3, runQ7_3());
                results = resultsMap.get("3");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 4:
                timings.put(4, runQ7_4());
                results = resultsMap.get("4");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 5:
                timings.put(5, runQ7_5());
                results = resultsMap.get("5");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 6:
                timings.put(6, runQ7_6());
                results = resultsMap.get("6");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 7:
                timings.put(7, runQ7_7());
                results = resultsMap.get("7");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 8:
                timings.put(8, runQ7_8());
                results = resultsMap.get("8");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
        }
        return timings;
    }

    public List<Pair<String, Long>> runQ7_0(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery ssMarch99 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_addr_sk", "store_sales.ss_ext_sales_price")
                .alias("ss_item_sk", "ss_addr_sk", "ss_ext_sales_price")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInStoreMarch99 = api.join()
                .select("item.i_manufact_id", "ssMarch99.ss_addr_sk", "ssMarch99.ss_ext_sales_price")
                .alias("i_manufact_id", "ss_addr_sk", "ss_ext_sales_price")
                .sources("item", ssMarch99, "ssMarch99", List.of("i_item_sk"), List.of("ss_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery ssSrc = api.join()
                .select("books.ss_ext_sales_price", "books.i_manufact_id")
                .alias("ss_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInStoreMarch99, "customer_address", "books", List.of("ss_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ss = api.read().select("i_manufact_id").sum("ss_ext_sales_price", "total_sales").from(ssSrc, "ssSrc").build();

        ReadQuery csMarch99 = api.join()
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInCatalogMarch99 = api.join()
                .sources("item", csMarch99, "csMarch99", List.of("i_item_sk"), List.of("catalog_sales.cs_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery csSrc = api.join()
                .select("books.csMarch99.catalog_sales.cs_ext_sales_price", "books.item.i_manufact_id")
                .alias("cs_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInCatalogMarch99, "customer_address", "books", List.of("csMarch99.catalog_sales.cs_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery cs = api.read().select("i_manufact_id").sum("cs_ext_sales_price", "total_sales").from(csSrc, "csSrc").build();

        ReadQuery wsMarch99 = api.join()
                .sources("web_sales", "date_dim", List.of("ws_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldOnlineMarch99 = api.join()
                .sources("item", wsMarch99, "wsMarch99", List.of("i_item_sk"), List.of("web_sales.ws_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery wsSrc = api.join().select("books.wsMarch99.web_sales.ws_ext_sales_price", "books.item.i_manufact_id").alias("ws_ext_sales_price", "i_manufact_id")
                .sources(booksSoldOnlineMarch99, "customer_address", "books", List.of("wsMarch99.web_sales.ws_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ws = api.read().select("i_manufact_id").sum("ws_ext_sales_price", "total_sales").from(wsSrc, "csSrc").build();

        ReadQuery unionCsSs = api.union().sources(ss, cs, "ss", "ws").build();
        ReadQuery finalSrc = api.union().sources(unionCsSs, ws, "ss_cs", "ws").build();
        ReadQuery finalQuery = api.read().select("i_manufact_id").sum("total_sales", "total_sales").from(finalSrc, "src").build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("0", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ7_1(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery ssMarch99 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_addr_sk", "store_sales.ss_ext_sales_price")
                .alias("ss_item_sk", "ss_addr_sk", "ss_ext_sales_price")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInStoreMarch99 = api.join()
                .select("item.i_manufact_id", "ssMarch99.ss_addr_sk", "ssMarch99.ss_ext_sales_price")
                .alias("i_manufact_id", "ss_addr_sk", "ss_ext_sales_price")
                .sources("item", ssMarch99, "ssMarch99", List.of("i_item_sk"), List.of("ss_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery ssSrc = api.join()
                .select("books.ss_ext_sales_price", "books.i_manufact_id")
                .alias("ss_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInStoreMarch99, "customer_address", "books", List.of("ss_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ss = api.read().store().select("i_manufact_id").sum("ss_ext_sales_price", "total_sales").from(ssSrc, "ssSrc").build();

        ss.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery csMarch99 = api.join()
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInCatalogMarch99 = api.join()
                .sources("item", csMarch99, "csMarch99", List.of("i_item_sk"), List.of("catalog_sales.cs_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery csSrc = api.join()
                .select("books.csMarch99.catalog_sales.cs_ext_sales_price", "books.item.i_manufact_id")
                .alias("cs_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInCatalogMarch99, "customer_address", "books", List.of("csMarch99.catalog_sales.cs_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery cs = api.read().select("i_manufact_id").sum("cs_ext_sales_price", "total_sales").from(csSrc, "csSrc").build();

        ReadQuery wsMarch99 = api.join()
                .sources("web_sales", "date_dim", List.of("ws_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldOnlineMarch99 = api.join()
                .sources("item", wsMarch99, "wsMarch99", List.of("i_item_sk"), List.of("web_sales.ws_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery wsSrc = api.join().select("books.wsMarch99.web_sales.ws_ext_sales_price", "books.item.i_manufact_id").alias("ws_ext_sales_price", "i_manufact_id")
                .sources(booksSoldOnlineMarch99, "customer_address", "books", List.of("wsMarch99.web_sales.ws_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ws = api.read().select("i_manufact_id").sum("ws_ext_sales_price", "total_sales").from(wsSrc, "csSrc").build();

        ReadQuery unionCsSs = api.union().sources(ss, cs, "ss", "ws").build();
        ReadQuery finalSrc = api.union().sources(unionCsSs, ws, "ss_cs", "ws").build();
        ReadQuery finalQuery = api.read().select("i_manufact_id").sum("total_sales", "total_sales").from(finalSrc, "src").build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("1", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ7_2(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery ssMarch99 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_addr_sk", "store_sales.ss_ext_sales_price")
                .alias("ss_item_sk", "ss_addr_sk", "ss_ext_sales_price")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInStoreMarch99 = api.join()
                .select("item.i_manufact_id", "ssMarch99.ss_addr_sk", "ssMarch99.ss_ext_sales_price")
                .alias("i_manufact_id", "ss_addr_sk", "ss_ext_sales_price")
                .sources("item", ssMarch99, "ssMarch99", List.of("i_item_sk"), List.of("ss_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery ssSrc = api.join()
                .select("books.ss_ext_sales_price", "books.i_manufact_id")
                .alias("ss_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInStoreMarch99, "customer_address", "books", List.of("ss_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ss = api.read().select("i_manufact_id").sum("ss_ext_sales_price", "total_sales").from(ssSrc, "ssSrc").build();

        ReadQuery csMarch99 = api.join()
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInCatalogMarch99 = api.join()
                .sources("item", csMarch99, "csMarch99", List.of("i_item_sk"), List.of("catalog_sales.cs_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery csSrc = api.join()
                .select("books.csMarch99.catalog_sales.cs_ext_sales_price", "books.item.i_manufact_id")
                .alias("cs_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInCatalogMarch99, "customer_address", "books", List.of("csMarch99.catalog_sales.cs_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery cs = api.read().store().select("i_manufact_id").sum("cs_ext_sales_price", "total_sales").from(csSrc, "csSrc").build();

        cs.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery wsMarch99 = api.join()
                .sources("web_sales", "date_dim", List.of("ws_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldOnlineMarch99 = api.join()
                .sources("item", wsMarch99, "wsMarch99", List.of("i_item_sk"), List.of("web_sales.ws_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery wsSrc = api.join().select("books.wsMarch99.web_sales.ws_ext_sales_price", "books.item.i_manufact_id").alias("ws_ext_sales_price", "i_manufact_id")
                .sources(booksSoldOnlineMarch99, "customer_address", "books", List.of("wsMarch99.web_sales.ws_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ws = api.read().select("i_manufact_id").sum("ws_ext_sales_price", "total_sales").from(wsSrc, "csSrc").build();

        ReadQuery unionCsSs = api.union().sources(ss, cs, "ss", "ws").build();
        ReadQuery finalSrc = api.union().sources(unionCsSs, ws, "ss_cs", "ws").build();
        ReadQuery finalQuery = api.read().select("i_manufact_id").sum("total_sales", "total_sales").from(finalSrc, "src").build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("2", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ7_3(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery ssMarch99 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_addr_sk", "store_sales.ss_ext_sales_price")
                .alias("ss_item_sk", "ss_addr_sk", "ss_ext_sales_price")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInStoreMarch99 = api.join()
                .select("item.i_manufact_id", "ssMarch99.ss_addr_sk", "ssMarch99.ss_ext_sales_price")
                .alias("i_manufact_id", "ss_addr_sk", "ss_ext_sales_price")
                .sources("item", ssMarch99, "ssMarch99", List.of("i_item_sk"), List.of("ss_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery ssSrc = api.join()
                .select("books.ss_ext_sales_price", "books.i_manufact_id")
                .alias("ss_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInStoreMarch99, "customer_address", "books", List.of("ss_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ss = api.read().select("i_manufact_id").sum("ss_ext_sales_price", "total_sales").from(ssSrc, "ssSrc").build();

        ReadQuery csMarch99 = api.join()
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInCatalogMarch99 = api.join()
                .sources("item", csMarch99, "csMarch99", List.of("i_item_sk"), List.of("catalog_sales.cs_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery csSrc = api.join()
                .select("books.csMarch99.catalog_sales.cs_ext_sales_price", "books.item.i_manufact_id")
                .alias("cs_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInCatalogMarch99, "customer_address", "books", List.of("csMarch99.catalog_sales.cs_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery cs = api.read().select("i_manufact_id").sum("cs_ext_sales_price", "total_sales").from(csSrc, "csSrc").build();

        ReadQuery wsMarch99 = api.join()
                .sources("web_sales", "date_dim", List.of("ws_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldOnlineMarch99 = api.join()
                .sources("item", wsMarch99, "wsMarch99", List.of("i_item_sk"), List.of("web_sales.ws_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery wsSrc = api.join().select("books.wsMarch99.web_sales.ws_ext_sales_price", "books.item.i_manufact_id").alias("ws_ext_sales_price", "i_manufact_id")
                .sources(booksSoldOnlineMarch99, "customer_address", "books", List.of("wsMarch99.web_sales.ws_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ws = api.read().store().select("i_manufact_id").sum("ws_ext_sales_price", "total_sales").from(wsSrc, "csSrc").build();

        ws.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery unionCsSs = api.union().sources(ss, cs, "ss", "ws").build();
        ReadQuery finalSrc = api.union().sources(unionCsSs, ws, "ss_cs", "ws").build();
        ReadQuery finalQuery = api.read().select("i_manufact_id").sum("total_sales", "total_sales").from(finalSrc, "src").build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("3", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ7_4(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery ssMarch99 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_addr_sk", "store_sales.ss_ext_sales_price")
                .alias("ss_item_sk", "ss_addr_sk", "ss_ext_sales_price")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInStoreMarch99 = api.join()
                .select("item.i_manufact_id", "ssMarch99.ss_addr_sk", "ssMarch99.ss_ext_sales_price")
                .alias("i_manufact_id", "ss_addr_sk", "ss_ext_sales_price")
                .sources("item", ssMarch99, "ssMarch99", List.of("i_item_sk"), List.of("ss_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery ssSrc = api.join()
                .select("books.ss_ext_sales_price", "books.i_manufact_id")
                .alias("ss_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInStoreMarch99, "customer_address", "books", List.of("ss_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ss = api.read().store().select("i_manufact_id").sum("ss_ext_sales_price", "total_sales").from(ssSrc, "ssSrc").build();

        ss.run(broker);

        ReadQuery csMarch99 = api.join()
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInCatalogMarch99 = api.join()
                .sources("item", csMarch99, "csMarch99", List.of("i_item_sk"), List.of("catalog_sales.cs_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery csSrc = api.join()
                .select("books.csMarch99.catalog_sales.cs_ext_sales_price", "books.item.i_manufact_id")
                .alias("cs_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInCatalogMarch99, "customer_address", "books", List.of("csMarch99.catalog_sales.cs_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery cs = api.read().store().select("i_manufact_id").sum("cs_ext_sales_price", "total_sales").from(csSrc, "csSrc").build();

        cs.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery wsMarch99 = api.join()
                .sources("web_sales", "date_dim", List.of("ws_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldOnlineMarch99 = api.join()
                .sources("item", wsMarch99, "wsMarch99", List.of("i_item_sk"), List.of("web_sales.ws_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery wsSrc = api.join().select("books.wsMarch99.web_sales.ws_ext_sales_price", "books.item.i_manufact_id").alias("ws_ext_sales_price", "i_manufact_id")
                .sources(booksSoldOnlineMarch99, "customer_address", "books", List.of("wsMarch99.web_sales.ws_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ws = api.read().select("i_manufact_id").sum("ws_ext_sales_price", "total_sales").from(wsSrc, "csSrc").build();

        ReadQuery unionCsSs = api.union().sources(ss, cs, "ss", "ws").build();
        ReadQuery finalSrc = api.union().sources(unionCsSs, ws, "ss_cs", "ws").build();
        ReadQuery finalQuery = api.read().select("i_manufact_id").sum("total_sales", "total_sales").from(finalSrc, "src").build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("4", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ7_5(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery ssMarch99 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_addr_sk", "store_sales.ss_ext_sales_price")
                .alias("ss_item_sk", "ss_addr_sk", "ss_ext_sales_price")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInStoreMarch99 = api.join()
                .select("item.i_manufact_id", "ssMarch99.ss_addr_sk", "ssMarch99.ss_ext_sales_price")
                .alias("i_manufact_id", "ss_addr_sk", "ss_ext_sales_price")
                .sources("item", ssMarch99, "ssMarch99", List.of("i_item_sk"), List.of("ss_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery ssSrc = api.join()
                .select("books.ss_ext_sales_price", "books.i_manufact_id")
                .alias("ss_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInStoreMarch99, "customer_address", "books", List.of("ss_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ss = api.read().store().select("i_manufact_id").sum("ss_ext_sales_price", "total_sales").from(ssSrc, "ssSrc").build();

        ss.run(broker);

        ReadQuery csMarch99 = api.join()
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInCatalogMarch99 = api.join()
                .sources("item", csMarch99, "csMarch99", List.of("i_item_sk"), List.of("catalog_sales.cs_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery csSrc = api.join()
                .select("books.csMarch99.catalog_sales.cs_ext_sales_price", "books.item.i_manufact_id")
                .alias("cs_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInCatalogMarch99, "customer_address", "books", List.of("csMarch99.catalog_sales.cs_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery cs = api.read().select("i_manufact_id").sum("cs_ext_sales_price", "total_sales").from(csSrc, "csSrc").build();

        ReadQuery wsMarch99 = api.join()
                .sources("web_sales", "date_dim", List.of("ws_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldOnlineMarch99 = api.join()
                .sources("item", wsMarch99, "wsMarch99", List.of("i_item_sk"), List.of("web_sales.ws_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery wsSrc = api.join().select("books.wsMarch99.web_sales.ws_ext_sales_price", "books.item.i_manufact_id").alias("ws_ext_sales_price", "i_manufact_id")
                .sources(booksSoldOnlineMarch99, "customer_address", "books", List.of("wsMarch99.web_sales.ws_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ws = api.read().store().select("i_manufact_id").sum("ws_ext_sales_price", "total_sales").from(wsSrc, "csSrc").build();

        ws.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery unionCsSs = api.union().sources(ss, cs, "ss", "ws").build();
        ReadQuery finalSrc = api.union().sources(unionCsSs, ws, "ss_cs", "ws").build();
        ReadQuery finalQuery = api.read().select("i_manufact_id").sum("total_sales", "total_sales").from(finalSrc, "src").build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("5", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ7_6(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery ssMarch99 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_addr_sk", "store_sales.ss_ext_sales_price")
                .alias("ss_item_sk", "ss_addr_sk", "ss_ext_sales_price")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInStoreMarch99 = api.join()
                .select("item.i_manufact_id", "ssMarch99.ss_addr_sk", "ssMarch99.ss_ext_sales_price")
                .alias("i_manufact_id", "ss_addr_sk", "ss_ext_sales_price")
                .sources("item", ssMarch99, "ssMarch99", List.of("i_item_sk"), List.of("ss_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery ssSrc = api.join()
                .select("books.ss_ext_sales_price", "books.i_manufact_id")
                .alias("ss_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInStoreMarch99, "customer_address", "books", List.of("ss_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ss = api.read().select("i_manufact_id").sum("ss_ext_sales_price", "total_sales").from(ssSrc, "ssSrc").build();

        ReadQuery csMarch99 = api.join()
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInCatalogMarch99 = api.join()
                .sources("item", csMarch99, "csMarch99", List.of("i_item_sk"), List.of("catalog_sales.cs_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery csSrc = api.join()
                .select("books.csMarch99.catalog_sales.cs_ext_sales_price", "books.item.i_manufact_id")
                .alias("cs_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInCatalogMarch99, "customer_address", "books", List.of("csMarch99.catalog_sales.cs_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery cs = api.read().store().select("i_manufact_id").sum("cs_ext_sales_price", "total_sales").from(csSrc, "csSrc").build();

        cs.run(broker);

        ReadQuery wsMarch99 = api.join()
                .sources("web_sales", "date_dim", List.of("ws_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldOnlineMarch99 = api.join()
                .sources("item", wsMarch99, "wsMarch99", List.of("i_item_sk"), List.of("web_sales.ws_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery wsSrc = api.join().select("books.wsMarch99.web_sales.ws_ext_sales_price", "books.item.i_manufact_id").alias("ws_ext_sales_price", "i_manufact_id")
                .sources(booksSoldOnlineMarch99, "customer_address", "books", List.of("wsMarch99.web_sales.ws_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ws = api.read().store().select("i_manufact_id").sum("ws_ext_sales_price", "total_sales").from(wsSrc, "csSrc").build();

        ws.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery unionCsSs = api.union().sources(ss, cs, "ss", "ws").build();
        ReadQuery finalSrc = api.union().sources(unionCsSs, ws, "ss_cs", "ws").build();
        ReadQuery finalQuery = api.read().select("i_manufact_id").sum("total_sales", "total_sales").from(finalSrc, "src").build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("6", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ7_7(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery ssMarch99 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_addr_sk", "store_sales.ss_ext_sales_price")
                .alias("ss_item_sk", "ss_addr_sk", "ss_ext_sales_price")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInStoreMarch99 = api.join()
                .select("item.i_manufact_id", "ssMarch99.ss_addr_sk", "ssMarch99.ss_ext_sales_price")
                .alias("i_manufact_id", "ss_addr_sk", "ss_ext_sales_price")
                .sources("item", ssMarch99, "ssMarch99", List.of("i_item_sk"), List.of("ss_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery ssSrc = api.join()
                .select("books.ss_ext_sales_price", "books.i_manufact_id")
                .alias("ss_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInStoreMarch99, "customer_address", "books", List.of("ss_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ss = api.read().store().select("i_manufact_id").sum("ss_ext_sales_price", "total_sales").from(ssSrc, "ssSrc").build();

        ss.run(broker);

        ReadQuery csMarch99 = api.join()
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInCatalogMarch99 = api.join()
                .sources("item", csMarch99, "csMarch99", List.of("i_item_sk"), List.of("catalog_sales.cs_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery csSrc = api.join()
                .select("books.csMarch99.catalog_sales.cs_ext_sales_price", "books.item.i_manufact_id")
                .alias("cs_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInCatalogMarch99, "customer_address", "books", List.of("csMarch99.catalog_sales.cs_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery cs = api.read().store().select("i_manufact_id").sum("cs_ext_sales_price", "total_sales").from(csSrc, "csSrc").build();


        cs.run(broker);


        ReadQuery wsMarch99 = api.join()
                .sources("web_sales", "date_dim", List.of("ws_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldOnlineMarch99 = api.join()
                .sources("item", wsMarch99, "wsMarch99", List.of("i_item_sk"), List.of("web_sales.ws_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery wsSrc = api.join().select("books.wsMarch99.web_sales.ws_ext_sales_price", "books.item.i_manufact_id").alias("ws_ext_sales_price", "i_manufact_id")
                .sources(booksSoldOnlineMarch99, "customer_address", "books", List.of("wsMarch99.web_sales.ws_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ws = api.read().store().select("i_manufact_id").sum("ws_ext_sales_price", "total_sales").from(wsSrc, "csSrc").build();

        ws.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery unionCsSs = api.union().sources(ss, cs, "ss", "ws").build();
        ReadQuery finalSrc = api.union().sources(unionCsSs, ws, "ss_cs", "ws").build();
        ReadQuery finalQuery = api.read().select("i_manufact_id").sum("total_sales", "total_sales").from(finalSrc, "src").build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("7", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ7_8(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery ssMarch99 = api.join()
                .select("store_sales.ss_item_sk", "store_sales.ss_addr_sk", "store_sales.ss_ext_sales_price")
                .alias("ss_item_sk", "ss_addr_sk", "ss_ext_sales_price")
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInStoreMarch99 = api.join()
                .select("item.i_manufact_id", "ssMarch99.ss_addr_sk", "ssMarch99.ss_ext_sales_price")
                .alias("i_manufact_id", "ss_addr_sk", "ss_ext_sales_price")
                .sources("item", ssMarch99, "ssMarch99", List.of("i_item_sk"), List.of("ss_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery ssSrc = api.join()
                .select("books.ss_ext_sales_price", "books.i_manufact_id")
                .alias("ss_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInStoreMarch99, "customer_address", "books", List.of("ss_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ss = api.read().select("i_manufact_id").sum("ss_ext_sales_price", "total_sales").from(ssSrc, "ssSrc").build();

        ReadQuery csMarch99 = api.join()
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldInCatalogMarch99 = api.join()
                .sources("item", csMarch99, "csMarch99", List.of("i_item_sk"), List.of("catalog_sales.cs_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery csSrc = api.join()
                .select("books.csMarch99.catalog_sales.cs_ext_sales_price", "books.item.i_manufact_id")
                .alias("cs_ext_sales_price", "i_manufact_id")
                .sources(booksSoldInCatalogMarch99, "customer_address", "books", List.of("csMarch99.catalog_sales.cs_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery cs = api.read().select("i_manufact_id").sum("cs_ext_sales_price", "total_sales").from(csSrc, "csSrc").build();

        ReadQuery wsMarch99 = api.join()
                .sources("web_sales", "date_dim", List.of("ws_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_year == 1999 && d_moy == 3").build();

        ReadQuery booksSoldOnlineMarch99 = api.join()
                .sources("item", wsMarch99, "wsMarch99", List.of("i_item_sk"), List.of("web_sales.ws_item_sk"))
                .filters("i_category == 'Books'", "").build();

        ReadQuery wsSrc = api.join().select("books.wsMarch99.web_sales.ws_ext_sales_price", "books.item.i_manufact_id").alias("ws_ext_sales_price", "i_manufact_id")
                .sources(booksSoldOnlineMarch99, "customer_address", "books", List.of("wsMarch99.web_sales.ws_bill_addr_sk"), List.of("ca_address_sk"))
                .filters("", "ca_gmt_offset == -5").build();

        ReadQuery ws = api.read().select("i_manufact_id").sum("ws_ext_sales_price", "total_sales").from(wsSrc, "csSrc").build();

        ReadQuery unionCsSs = api.union().sources(ss, cs, "ss", "ws").build();
        ReadQuery finalSrc = api.union().sources(unionCsSs, ws, "ss_cs", "ws").build();
        ReadQuery finalQuery = api.read().store().select("i_manufact_id")
                .sum("total_sales", "total_sales").from(finalSrc, "src").build();

        finalQuery.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = finalQuery.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("8", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    /*
    Q33
WITH ss
     AS (SELECT i_manufact_id,
                Sum(ss_ext_sales_price) total_sales
         FROM   store_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_manufact_id IN (SELECT i_manufact_id
                                  FROM   item
                                  WHERE  i_category IN ( 'Books' ))
                AND ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 3
                AND ss_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
         GROUP  BY i_manufact_id),
*//*
     cs
     AS (SELECT i_manufact_id,
                Sum(cs_ext_sales_price) total_sales
         FROM   catalog_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_manufact_id IN (SELECT i_manufact_id
                                  FROM   item
                                  WHERE  i_category IN ( 'Books' ))
                AND cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 3
                AND cs_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
         GROUP  BY i_manufact_id),
         *//*
     ws
     AS (SELECT i_manufact_id,
                Sum(ws_ext_sales_price) total_sales
         FROM   web_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_manufact_id IN (SELECT i_manufact_id
                                  FROM   item
                                  WHERE  i_category IN ( 'Books' ))
                AND ws_item_sk = i_item_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 3
                AND ws_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
         GROUP  BY i_manufact_id)
         */
    /*
SELECT i_manufact_id,
               Sum(total_sales) total_sales
FROM   (SELECT *
        FROM   ss
        UNION ALL
        SELECT *
        FROM   cs
        UNION ALL
        SELECT *
        FROM   ws) tmp1
GROUP  BY i_manufact_id
 */



}
