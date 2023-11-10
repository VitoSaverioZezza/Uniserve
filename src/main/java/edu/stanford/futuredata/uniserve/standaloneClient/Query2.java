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

public class Query2 {

    private final Broker broker;
    private String rootPath = "";

    public Query2(Broker broker, String rootPath) {
        this.rootPath = rootPath;
        this.broker = broker;
    }

    public Map<String, RelReadQueryResults> resultsMap = new HashMap<>();

    public static final List<String> tablesToLoad = List.of(
            "catalog_sales",
            "customer",
            "customer_address",
            "date_dim"
    );

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
                timings.put(0, runQ2_0());
                results = resultsMap.get("0");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 1:
                timings.put(1, runQ2_1());
                results = resultsMap.get("1");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 2:
                timings.put(2, runQ2_2());
                results = resultsMap.get("2");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 3:
                timings.put(3, runQ2_3());
                results = resultsMap.get("3");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 4:
                timings.put(4, runQ2_4());
                results = resultsMap.get("4");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
        }
        return timings;
    }

    public List<Pair<String, Long>> runQ2_0(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery catalogSalesInFirstQ98 = api.join()
                .select("catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_sales_price")
                .alias("cs_bill_customer_sk", "cs_sales_price")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_qoy == 1 && d_year == 1998").build();

        ReadQuery customerAndAddresses = api.join()
                .select("customer_address.ca_state", "customer_address.ca_zip", "customer.c_customer_sk")
                .alias("ca_state", "ca_zip", "c_customer_sk")
                .sources("customer", "customer_address", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();

        ReadQuery finalJoin = api.join().select("j2.ca_zip", "j1.cs_sales_price", "j2.ca_state")
                .alias("ca_zip", "cs_sales_price", "ca_state")
                .sources(catalogSalesInFirstQ98, customerAndAddresses, "j1", "j2",List.of("cs_bill_customer_sk"), List.of("c_customer_sk"))
                .build();

        String filterPredicate = "" +
                " substringQUERY = def (x) { x.substring(0,5) }; "
                + "['85669', '86197', '88274', '83405','86475', '85392', '85460', '80348', '81792'].contains( substringQUERY(ca_zip) )" +
                " || cs_sales_price > 500 "
                + "||  ['CA', 'WA', 'GA'].contains(ca_state)"
                ;

        ReadQuery res = api.read().select("ca_zip").sum("cs_sales_price", "sum_prices")
                .fromFilter(finalJoin, "fJ", filterPredicate).build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = res.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("0", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ2_1(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery catalogSalesInFirstQ98 = api.join()
                .select("catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_sales_price")
                .alias("cs_bill_customer_sk", "cs_sales_price")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .stored()
                .filters("", "d_qoy == 1 && d_year == 1998").build();

        catalogSalesInFirstQ98.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery customerAndAddresses = api.join()
                .select("customer_address.ca_state", "customer_address.ca_zip", "customer.c_customer_sk")
                .alias("ca_state", "ca_zip", "c_customer_sk")
                .sources("customer", "customer_address", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();

        ReadQuery finalJoin = api.join().select("j2.ca_zip", "j1.cs_sales_price", "j2.ca_state")
                .alias("ca_zip", "cs_sales_price", "ca_state")
                .sources(catalogSalesInFirstQ98, customerAndAddresses, "j1", "j2",List.of("cs_bill_customer_sk"), List.of("c_customer_sk"))
                .build();

        String filterPredicate = "" +
                " substringQUERY = def (x) { x.substring(0,5) }; "
                + "['85669', '86197', '88274', '83405','86475', '85392', '85460', '80348', '81792'].contains( substringQUERY(ca_zip) )" +
                " || cs_sales_price > 500 "
                + "||  ['CA', 'WA', 'GA'].contains(ca_state)"
                ;

        ReadQuery res = api.read().select("ca_zip").sum("cs_sales_price", "sum_prices")
                .fromFilter(finalJoin, "fJ", filterPredicate).build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = res.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("1", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ2_2(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);
        ReadQuery catalogSalesInFirstQ98 = api.join()
                .select("catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_sales_price")
                .alias("cs_bill_customer_sk", "cs_sales_price")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_qoy == 1 && d_year == 1998").build();

        ReadQuery customerAndAddresses = api.join()
                .select("customer_address.ca_state", "customer_address.ca_zip", "customer.c_customer_sk")
                .alias("ca_state", "ca_zip", "c_customer_sk")
                .sources("customer", "customer_address", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .stored()
                .build();

        customerAndAddresses.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery finalJoin = api.join().select("j2.ca_zip", "j1.cs_sales_price", "j2.ca_state")
                .alias("ca_zip", "cs_sales_price", "ca_state")
                .sources(catalogSalesInFirstQ98, customerAndAddresses, "j1", "j2",List.of("cs_bill_customer_sk"), List.of("c_customer_sk"))
                .build();

        String filterPredicate = "" +
                " substringQUERY = def (x) { x.substring(0,5) }; "
                + "['85669', '86197', '88274', '83405','86475', '85392', '85460', '80348', '81792'].contains( substringQUERY(ca_zip) )" +
                " || cs_sales_price > 500 "
                + "||  ['CA', 'WA', 'GA'].contains(ca_state)"
                ;

        ReadQuery res = api.read().select("ca_zip").sum("cs_sales_price", "sum_prices")
                .fromFilter(finalJoin, "fJ", filterPredicate).build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = res.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("2", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));
        return loadTimes;
    }
    public List<Pair<String, Long>> runQ2_3(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery catalogSalesInFirstQ98 = api.join()
                .select("catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_sales_price")
                .alias("cs_bill_customer_sk", "cs_sales_price")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_qoy == 1 && d_year == 1998").build();

        ReadQuery customerAndAddresses = api.join()
                .select("customer_address.ca_state", "customer_address.ca_zip", "customer.c_customer_sk")
                .alias("ca_state", "ca_zip", "c_customer_sk")
                .sources("customer", "customer_address", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();

        ReadQuery finalJoin = api.join().select("j2.ca_zip", "j1.cs_sales_price", "j2.ca_state")
                .alias("ca_zip", "cs_sales_price", "ca_state")
                .sources(catalogSalesInFirstQ98, customerAndAddresses, "j1", "j2",List.of("cs_bill_customer_sk"), List.of("c_customer_sk"))
                .stored()
                .build();

        finalJoin.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        String filterPredicate = "" +
                " substringQUERY = def (x) { x.substring(0,5) }; "
                + "['85669', '86197', '88274', '83405','86475', '85392', '85460', '80348', '81792'].contains( substringQUERY(ca_zip) )" +
                " || cs_sales_price > 500 "
                + "||  ['CA', 'WA', 'GA'].contains(ca_state)"
                ;

        ReadQuery res = api.read().select("ca_zip").sum("cs_sales_price", "sum_prices")
                .fromFilter(finalJoin, "fJ", filterPredicate).build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = res.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("3", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> runQ2_4(){
        TestMethods tm = new TestMethods(rootPath);
        API api = new API(broker);

        ReadQuery catalogSalesInFirstQ98 = api.join()
                .select("catalog_sales.cs_bill_customer_sk", "catalog_sales.cs_sales_price")
                .alias("cs_bill_customer_sk", "cs_sales_price")
                .sources("catalog_sales", "date_dim", List.of("cs_sold_date_sk"), List.of("d_date_sk"))
                .filters("", "d_qoy == 1 && d_year == 1998").build();

        ReadQuery customerAndAddresses = api.join()
                .select("customer_address.ca_state", "customer_address.ca_zip", "customer.c_customer_sk")
                .alias("ca_state", "ca_zip", "c_customer_sk")
                .sources("customer", "customer_address", List.of("c_current_addr_sk"), List.of("ca_address_sk"))
                .build();

        ReadQuery finalJoin = api.join().select("j2.ca_zip", "j1.cs_sales_price", "j2.ca_state")
                .alias("ca_zip", "cs_sales_price", "ca_state")
                .sources(catalogSalesInFirstQ98, customerAndAddresses, "j1", "j2",List.of("cs_bill_customer_sk"), List.of("c_customer_sk"))
                .build();

        String filterPredicate = "" +
                " substringQUERY = def (x) { x.substring(0,5) }; "
                + "['85669', '86197', '88274', '83405','86475', '85392', '85460', '80348', '81792'].contains( substringQUERY(ca_zip) )" +
                " || cs_sales_price > 500 "
                + "||  ['CA', 'WA', 'GA'].contains(ca_state)"
                ;

        ReadQuery res = api.read().select("ca_zip").sum("cs_sales_price", "sum_prices")
                .fromFilter(finalJoin, "fJ", filterPredicate).store().build();

        res.run(broker);
        List<Pair<String,Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = res.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("4", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
}
    //SQL Query definition
    /*
    *
    * SELECT ca_zip,
               Sum(cs_sales_price)
FROM   catalog_sales,
       customer,
       customer_address,
       date_dim
WHERE  cs_bill_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND ( Substr(ca_zip, 1, 5) IN ( '85669', '86197', '88274', '83405',
                                       '86475', '85392', '85460', '80348',
                                       '81792' )
              OR ca_state IN ( "CA", "WA", "GA" )
              OR cs_sales_price > 500 )
       AND cs_sold_date_sk = d_date_sk
       AND d_qoy = 1
       AND d_year = 1998
GROUP  BY ca_zip
ORDER  BY ca_zip
LIMIT 100;
    * */

