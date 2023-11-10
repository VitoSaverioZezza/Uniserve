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

public class Query3 {

    private final Broker broker;
    private Map<String, RelReadQueryResults> resultsMap = new HashMap<>();
    private String rootPath = "";

    public Query3(Broker broker, String rootPath){
        this.broker = broker;
        this.rootPath = rootPath;
    }
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
            int shardNum = Math.min(Math.max(TPC_DS_Inv.Bsizes.get(index) / 1000, 1),Broker.SHARDS_PER_TABLE);
            api.createTable(tableName)
                    .attributes(TPC_DS_Inv.schemas.get(index).toArray(new String[0]))
                    .keys(TPC_DS_Inv.keys.get(index).toArray(new String[0]))
                    .shardNumber(shardNum).build().run();
        }
        Map<Integer, List<Pair<String, Long>>> timings = new HashMap<>();
        RelReadQueryResults results = null;

        switch (queryIndex){
            case 0:
                timings.put(0, runQ3_0());
                results = resultsMap.get("0");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 1:
                timings.put(1, runQ3_1());
                results = resultsMap.get("1");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 2:
                timings.put(2, runQ3_2());
                results = resultsMap.get("2");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 3:
                timings.put(3, runQ3_3());
                results = resultsMap.get("3");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 4:
                timings.put(4, runQ3_4());
                results = resultsMap.get("4");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 5:
                timings.put(5, runQ3_5());
                results = resultsMap.get("5");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 6:
                timings.put(6, runQ3_6());
                results = resultsMap.get("6");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
        }
        return timings;
    }

    public static final List<String> tablesToLoad = List.of(
            "date_dim",
            "store_sales",
            "item",
            "customer",
            "customer_address",
            "store"
    );
    public List<Pair<String, Long>> runQ3_0(){
        TestMethods tm = new TestMethods(rootPath);

        API api = new API(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        List<String> rawJ1Schema = List.of("store_sales.ss_ext_sales_price", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "date_dim.d_moy", "date_dim.d_year");
        List<String> j1Schema = List.of("ss_ext_sales_price", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "d_moy", "d_year");
        List<String> rawJ2Schema = List.of("storeSalesInDec98.ss_ext_sales_price", "storeSalesInDec98.ss_store_sk", "storeSalesInDec98.ss_customer_sk", "item.i_brand", "item.i_brand_id", "item.i_manufact_id", "item.i_manufact");
        List<String> j2Schema = List.of("ss_ext_sales_price", "ss_store_sk", "ss_customer_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact");

        ReadQuery storeSalesInDec98 = api.join()
                .select(rawJ1Schema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("date_dim", "store_sales", List.of("d_date_sk"), List.of("ss_sold_date_sk"))
                .filters("d_moy == 12 && d_year == 1998", "").build();

        ReadQuery storeSalesInDec98ByManager38 = api.join()
                .select(rawJ2Schema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(storeSalesInDec98, "item", "storeSalesInDec98",
                        List.of("ss_item_sk"), List.of("i_item_sk")
                ).filters("", "i_manager_id == 38").build();

        ReadQuery j3 = api.join()
                .select("customer.c_current_addr_sk", "j2.ss_ext_sales_price", "j2.ss_store_sk", "j2.i_brand", "j2.i_brand_id", "j2.i_manufact_id", "j2.i_manufact")
                .alias("c_current_addr_sk", "ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact")
                .sources(storeSalesInDec98ByManager38, "customer", "j2",
                        List.of("ss_customer_sk"), List.of("c_customer_sk")
                ).build();

        ReadQuery j4 = api.join()
                .select("pj3.ss_ext_sales_price", "pj3.ss_store_sk", "pj3.i_brand", "pj3.i_brand_id", "pj3.i_manufact_id", "pj3.i_manufact", "customer_address.ca_zip")
                .alias("ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact", "ca_zip")
                .sources(j3, "customer_address", "pj3", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();


        ReadQuery j5 = api.join().sources(j4, "store", "pj4", List.of("ss_store_sk"), List.of("s_store_sk")).build();

        ReadQuery res = api.read().select("pj4.i_brand_id", "pj4.i_brand", "pj4.i_manufact_id", "pj4.i_manufact")
                .alias("i_brand_id", "i_brand", "i_manufact_id", "i_manufact")
                .sum("pj4.ss_ext_sales_price", "ext_price")
                .fromFilter(j5, "j5", "substringQUERY = def (x) { x.substring(0,5) }; substringQUERY(pj4.ca_zip) != substringQUERY(store.s_zip)")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = res.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("0", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ3_1(){
        TestMethods tm = new TestMethods(rootPath);

        API api = new API(broker);

        List<String> rawJ1Schema = List.of("store_sales.ss_ext_sales_price", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "date_dim.d_moy", "date_dim.d_year");
        List<String> j1Schema = List.of("ss_ext_sales_price", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "d_moy", "d_year");
        List<String> rawJ2Schema = List.of("storeSalesInDec98.ss_ext_sales_price", "storeSalesInDec98.ss_store_sk", "storeSalesInDec98.ss_customer_sk", "item.i_brand", "item.i_brand_id", "item.i_manufact_id", "item.i_manufact");
        List<String> j2Schema = List.of("ss_ext_sales_price", "ss_store_sk", "ss_customer_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact");

        ReadQuery storeSalesInDec98 = api.join()
                .select(rawJ1Schema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("date_dim", "store_sales", List.of("d_date_sk"), List.of("ss_sold_date_sk"))
                .stored()
                .filters("d_moy == 12 && d_year == 1998", "").build();

        storeSalesInDec98.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery storeSalesInDec98ByManager38 = api.join()
                .select(rawJ2Schema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(storeSalesInDec98, "item", "storeSalesInDec98",
                        List.of("ss_item_sk"), List.of("i_item_sk")
                ).filters("", "i_manager_id == 38").build();

        ReadQuery j3 = api.join()
                .select("customer.c_current_addr_sk", "j2.ss_ext_sales_price", "j2.ss_store_sk", "j2.i_brand", "j2.i_brand_id", "j2.i_manufact_id", "j2.i_manufact")
                .alias("c_current_addr_sk", "ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact")
                .sources(storeSalesInDec98ByManager38, "customer", "j2",
                        List.of("ss_customer_sk"), List.of("c_customer_sk")
                ).build();

        ReadQuery j4 = api.join()
                .select("pj3.ss_ext_sales_price", "pj3.ss_store_sk", "pj3.i_brand", "pj3.i_brand_id", "pj3.i_manufact_id", "pj3.i_manufact", "customer_address.ca_zip")
                .alias("ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact", "ca_zip")
                .sources(j3, "customer_address", "pj3", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();


        ReadQuery j5 = api.join().sources(j4, "store", "pj4", List.of("ss_store_sk"), List.of("s_store_sk")).build();

        ReadQuery res = api.read().select("pj4.i_brand_id", "pj4.i_brand", "pj4.i_manufact_id", "pj4.i_manufact")
                .alias("i_brand_id", "i_brand", "i_manufact_id", "i_manufact")
                .sum("pj4.ss_ext_sales_price", "ext_price")
                .fromFilter(j5, "j5", "substringQUERY = def (x) { x.substring(0,5) }; substringQUERY(pj4.ca_zip) != substringQUERY(store.s_zip)")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = res.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("1", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ3_2(){
        TestMethods tm = new TestMethods(rootPath);

        API api = new API(broker);

        List<String> rawJ1Schema = List.of("store_sales.ss_ext_sales_price", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "date_dim.d_moy", "date_dim.d_year");
        List<String> j1Schema = List.of("ss_ext_sales_price", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "d_moy", "d_year");
        List<String> rawJ2Schema = List.of("storeSalesInDec98.ss_ext_sales_price", "storeSalesInDec98.ss_store_sk", "storeSalesInDec98.ss_customer_sk", "item.i_brand", "item.i_brand_id", "item.i_manufact_id", "item.i_manufact");
        List<String> j2Schema = List.of("ss_ext_sales_price", "ss_store_sk", "ss_customer_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact");

        ReadQuery storeSalesInDec98 = api.join()
                .select(rawJ1Schema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("date_dim", "store_sales", List.of("d_date_sk"), List.of("ss_sold_date_sk"))
                .filters("d_moy == 12 && d_year == 1998", "").build();

        ReadQuery storeSalesInDec98ByManager38 = api.join()
                .select(rawJ2Schema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(storeSalesInDec98, "item", "storeSalesInDec98", List.of("ss_item_sk"), List.of("i_item_sk"))
                .filters("", "i_manager_id == 38")
                .stored().build();

        storeSalesInDec98ByManager38.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery j3 = api.join()
                .select("customer.c_current_addr_sk", "j2.ss_ext_sales_price", "j2.ss_store_sk", "j2.i_brand", "j2.i_brand_id", "j2.i_manufact_id", "j2.i_manufact")
                .alias("c_current_addr_sk", "ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact")
                .sources(storeSalesInDec98ByManager38, "customer", "j2",
                        List.of("ss_customer_sk"), List.of("c_customer_sk")
                ).build();

        ReadQuery j4 = api.join()
                .select("pj3.ss_ext_sales_price", "pj3.ss_store_sk", "pj3.i_brand", "pj3.i_brand_id", "pj3.i_manufact_id", "pj3.i_manufact", "customer_address.ca_zip")
                .alias("ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact", "ca_zip")
                .sources(j3, "customer_address", "pj3", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();


        ReadQuery j5 = api.join().sources(j4, "store", "pj4", List.of("ss_store_sk"), List.of("s_store_sk")).build();

        ReadQuery fpj5 = api.read().select("pj4.i_brand_id", "pj4.i_brand", "pj4.i_manufact_id", "pj4.i_manufact")
                .alias("i_brand_id", "i_brand", "i_manufact_id", "i_manufact")
                .sum("pj4.ss_ext_sales_price", "ext_price")
                .fromFilter(j5, "j5", "substringQUERY = def (x) { x.substring(0,5) }; substringQUERY(pj4.ca_zip) != substringQUERY(store.s_zip)")
                .build();

        long t = System.currentTimeMillis();
        RelReadQueryResults results = fpj5.run(broker);
        long elapsedTime = System.currentTimeMillis() - t;

        resultsMap.put("2", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ3_3(){
        TestMethods tm = new TestMethods(rootPath);

        API api = new API(broker);

        List<String> rawJ1Schema = List.of("store_sales.ss_ext_sales_price", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "date_dim.d_moy", "date_dim.d_year");
        List<String> j1Schema = List.of("ss_ext_sales_price", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "d_moy", "d_year");
        List<String> rawJ2Schema = List.of("storeSalesInDec98.ss_ext_sales_price", "storeSalesInDec98.ss_store_sk", "storeSalesInDec98.ss_customer_sk", "item.i_brand", "item.i_brand_id", "item.i_manufact_id", "item.i_manufact");
        List<String> j2Schema = List.of("ss_ext_sales_price", "ss_store_sk", "ss_customer_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact");

        ReadQuery storeSalesInDec98 = api.join()
                .select(rawJ1Schema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("date_dim", "store_sales", List.of("d_date_sk"), List.of("ss_sold_date_sk"))
                .filters("d_moy == 12 && d_year == 1998", "").build();

        ReadQuery storeSalesInDec98ByManager38 = api.join()
                .select(rawJ2Schema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(storeSalesInDec98, "item", "storeSalesInDec98",
                        List.of("ss_item_sk"), List.of("i_item_sk")
                ).filters("", "i_manager_id == 38").build();

        ReadQuery j3 = api.join()
                .select("customer.c_current_addr_sk", "j2.ss_ext_sales_price", "j2.ss_store_sk", "j2.i_brand", "j2.i_brand_id", "j2.i_manufact_id", "j2.i_manufact")
                .alias("c_current_addr_sk", "ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact")
                .sources(storeSalesInDec98ByManager38, "customer", "j2",
                        List.of("ss_customer_sk"), List.of("c_customer_sk")).stored().build();

        j3.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery j4 = api.join()
                .select("pj3.ss_ext_sales_price", "pj3.ss_store_sk", "pj3.i_brand", "pj3.i_brand_id", "pj3.i_manufact_id", "pj3.i_manufact", "customer_address.ca_zip")
                .alias("ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact", "ca_zip")
                .sources(j3, "customer_address", "pj3", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();


        ReadQuery j5 = api.join().sources(j4, "store", "pj4", List.of("ss_store_sk"), List.of("s_store_sk")).build();

        ReadQuery fpj5 = api.read().select("pj4.i_brand_id", "pj4.i_brand", "pj4.i_manufact_id", "pj4.i_manufact")
                .alias("i_brand_id", "i_brand", "i_manufact_id", "i_manufact")
                .sum("pj4.ss_ext_sales_price", "ext_price")
                .fromFilter(j5, "j5", "substringQUERY = def (x) { x.substring(0,5) }; substringQUERY(pj4.ca_zip) != substringQUERY(store.s_zip)")
                .build();

        long t = System.currentTimeMillis();
        RelReadQueryResults results = fpj5.run(broker);
        long elapsedTime = System.currentTimeMillis() - t;

        resultsMap.put("3", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ3_4(){
        TestMethods tm = new TestMethods(rootPath);

        API api = new API(broker);

        List<String> rawJ1Schema = List.of("store_sales.ss_ext_sales_price", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "date_dim.d_moy", "date_dim.d_year");
        List<String> j1Schema = List.of("ss_ext_sales_price", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "d_moy", "d_year");
        List<String> rawJ2Schema = List.of("storeSalesInDec98.ss_ext_sales_price", "storeSalesInDec98.ss_store_sk", "storeSalesInDec98.ss_customer_sk", "item.i_brand", "item.i_brand_id", "item.i_manufact_id", "item.i_manufact");
        List<String> j2Schema = List.of("ss_ext_sales_price", "ss_store_sk", "ss_customer_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact");

        ReadQuery storeSalesInDec98 = api.join()
                .select(rawJ1Schema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("date_dim", "store_sales", List.of("d_date_sk"), List.of("ss_sold_date_sk"))
                .filters("d_moy == 12 && d_year == 1998", "").build();

        ReadQuery storeSalesInDec98ByManager38 = api.join()
                .select(rawJ2Schema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(storeSalesInDec98, "item", "storeSalesInDec98",
                        List.of("ss_item_sk"), List.of("i_item_sk")
                ).filters("", "i_manager_id == 38").build();

        ReadQuery j3 = api.join()
                .select("customer.c_current_addr_sk", "j2.ss_ext_sales_price", "j2.ss_store_sk", "j2.i_brand", "j2.i_brand_id", "j2.i_manufact_id", "j2.i_manufact")
                .alias("c_current_addr_sk", "ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact")
                .sources(storeSalesInDec98ByManager38, "customer", "j2",
                        List.of("ss_customer_sk"), List.of("c_customer_sk")
                ).build();

        ReadQuery j4 = api.join()
                .select("pj3.ss_ext_sales_price", "pj3.ss_store_sk", "pj3.i_brand", "pj3.i_brand_id", "pj3.i_manufact_id", "pj3.i_manufact", "customer_address.ca_zip")
                .alias("ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact", "ca_zip")
                .sources(j3, "customer_address", "pj3", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .stored().build();

        j4.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery j5 = api.join().sources(j4, "store", "pj4", List.of("ss_store_sk"), List.of("s_store_sk")).build();

        ReadQuery fpj5 = api.read().select("pj4.i_brand_id", "pj4.i_brand", "pj4.i_manufact_id", "pj4.i_manufact")
                .alias("i_brand_id", "i_brand", "i_manufact_id", "i_manufact")
                .sum("pj4.ss_ext_sales_price", "ext_price")
                .fromFilter(j5, "j5", "substringQUERY = def (x) { x.substring(0,5) }; substringQUERY(pj4.ca_zip) != substringQUERY(store.s_zip)")
                .build();

        long t = System.currentTimeMillis();
        RelReadQueryResults results = fpj5.run(broker);
        long elapsedTime = System.currentTimeMillis() - t;

        resultsMap.put("4", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ3_5(){
        TestMethods tm = new TestMethods(rootPath);

        API api = new API(broker);

        List<String> rawJ1Schema = List.of("store_sales.ss_ext_sales_price", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "date_dim.d_moy", "date_dim.d_year");
        List<String> j1Schema = List.of("ss_ext_sales_price", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "d_moy", "d_year");
        List<String> rawJ2Schema = List.of("storeSalesInDec98.ss_ext_sales_price", "storeSalesInDec98.ss_store_sk", "storeSalesInDec98.ss_customer_sk", "item.i_brand", "item.i_brand_id", "item.i_manufact_id", "item.i_manufact");
        List<String> j2Schema = List.of("ss_ext_sales_price", "ss_store_sk", "ss_customer_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact");

        ReadQuery storeSalesInDec98 = api.join()
                .select(rawJ1Schema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("date_dim", "store_sales", List.of("d_date_sk"), List.of("ss_sold_date_sk"))
                .filters("d_moy == 12 && d_year == 1998", "").build();

        ReadQuery storeSalesInDec98ByManager38 = api.join()
                .select(rawJ2Schema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(storeSalesInDec98, "item", "storeSalesInDec98",
                        List.of("ss_item_sk"), List.of("i_item_sk")
                ).filters("", "i_manager_id == 38").build();

        ReadQuery j3 = api.join()
                .select("customer.c_current_addr_sk", "j2.ss_ext_sales_price", "j2.ss_store_sk", "j2.i_brand", "j2.i_brand_id", "j2.i_manufact_id", "j2.i_manufact")
                .alias("c_current_addr_sk", "ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact")
                .sources(storeSalesInDec98ByManager38, "customer", "j2",
                        List.of("ss_customer_sk"), List.of("c_customer_sk")
                ).build();

        ReadQuery j4 = api.join()
                .select("pj3.ss_ext_sales_price", "pj3.ss_store_sk", "pj3.i_brand", "pj3.i_brand_id", "pj3.i_manufact_id", "pj3.i_manufact", "customer_address.ca_zip")
                .alias("ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact", "ca_zip")
                .sources(j3, "customer_address", "pj3", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();


        ReadQuery j5 = api.join()
                .sources(j4, "store", "pj4", List.of("ss_store_sk"), List.of("s_store_sk"))
                .stored().build();

        j5.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);


        ReadQuery fpj5 = api.read().select("pj4.i_brand_id", "pj4.i_brand", "pj4.i_manufact_id", "pj4.i_manufact")
                .alias("i_brand_id", "i_brand", "i_manufact_id", "i_manufact")
                .sum("pj4.ss_ext_sales_price", "ext_price")
                .fromFilter(j5, "j5", "substringQUERY = def (x) { x.substring(0,5) }; substringQUERY(pj4.ca_zip) != substringQUERY(store.s_zip)")
                .build();

        long t = System.currentTimeMillis();
        RelReadQueryResults results = fpj5.run(broker);
        long elapsedTime = System.currentTimeMillis() - t;

        resultsMap.put("5", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ3_6(){
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

        List<String> rawJ1Schema = List.of("store_sales.ss_ext_sales_price", "store_sales.ss_item_sk", "store_sales.ss_store_sk", "store_sales.ss_customer_sk", "date_dim.d_moy", "date_dim.d_year");
        List<String> j1Schema = List.of("ss_ext_sales_price", "ss_item_sk", "ss_store_sk", "ss_customer_sk", "d_moy", "d_year");
        List<String> rawJ2Schema = List.of("storeSalesInDec98.ss_ext_sales_price", "storeSalesInDec98.ss_store_sk", "storeSalesInDec98.ss_customer_sk", "item.i_brand", "item.i_brand_id", "item.i_manufact_id", "item.i_manufact");
        List<String> j2Schema = List.of("ss_ext_sales_price", "ss_store_sk", "ss_customer_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact");

        ReadQuery storeSalesInDec98 = api.join()
                .select(rawJ1Schema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("date_dim", "store_sales", List.of("d_date_sk"), List.of("ss_sold_date_sk"))
                .filters("d_moy == 12 && d_year == 1998", "").build();

        ReadQuery storeSalesInDec98ByManager38 = api.join()
                .select(rawJ2Schema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(storeSalesInDec98, "item", "storeSalesInDec98",
                        List.of("ss_item_sk"), List.of("i_item_sk")
                ).filters("", "i_manager_id == 38").build();

        ReadQuery j3 = api.join()
                .select("customer.c_current_addr_sk", "j2.ss_ext_sales_price", "j2.ss_store_sk", "j2.i_brand", "j2.i_brand_id", "j2.i_manufact_id", "j2.i_manufact")
                .alias("c_current_addr_sk", "ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact")
                .sources(storeSalesInDec98ByManager38, "customer", "j2",
                        List.of("ss_customer_sk"), List.of("c_customer_sk")
                ).build();

        ReadQuery j4 = api.join()
                .select("pj3.ss_ext_sales_price", "pj3.ss_store_sk", "pj3.i_brand", "pj3.i_brand_id", "pj3.i_manufact_id", "pj3.i_manufact", "customer_address.ca_zip")
                .alias("ss_ext_sales_price", "ss_store_sk", "i_brand", "i_brand_id", "i_manufact_id", "i_manufact", "ca_zip")
                .sources(j3, "customer_address", "pj3", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();


        ReadQuery j5 = api.join().sources(j4, "store", "pj4", List.of("ss_store_sk"), List.of("s_store_sk")).build();

        ReadQuery fpj5 = api.read().select("pj4.i_brand_id", "pj4.i_brand", "pj4.i_manufact_id", "pj4.i_manufact")
                .alias("i_brand_id", "i_brand", "i_manufact_id", "i_manufact")
                .sum("pj4.ss_ext_sales_price", "ext_price")
                .fromFilter(j5, "j5", "substringQUERY = def (x) { x.substring(0,5) }; substringQUERY(pj4.ca_zip) != substringQUERY(store.s_zip)")
                .store().build();

        fpj5.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        long t = System.currentTimeMillis();
        RelReadQueryResults results = fpj5.run(broker);
        long elapsedTime = System.currentTimeMillis() - t;

        resultsMap.put("6", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
}


/*
* SELECT i_brand_id              brand_id,
               i_brand                 brand,
               i_manufact_id,
               i_manufact,
               Sum(ss_ext_sales_price) ext_price
FROM   date_dim,
       store_sales,
       item,
       customer,
       customer_address,
       store
WHERE  d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND ss_customer_sk = c_customer_sk
       AND ss_store_sk = s_store_sk
       AND c_current_addr_sk = ca_address_sk
       *
       AND i_manager_id = 38
       AND d_moy = 12
       AND d_year = 1998
       AND Substr(ca_zip, 1, 5) <> Substr(s_zip, 1, 5)
       *
GROUP  BY i_brand,
          i_brand_id,
          i_manufact_id,
          i_manufact
ORDER  BY ext_price DESC,
          i_brand,
          i_brand_id,
          i_manufact_id,
          i_manufact
LIMIT 100; */