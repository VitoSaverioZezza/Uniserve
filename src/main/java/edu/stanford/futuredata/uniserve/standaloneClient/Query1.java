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

public class Query1 {
    private final Broker broker;
    private final Map<String, RelReadQueryResults> resultsMap = new HashMap<>();
    private String rootPath = "";

    public Query1(Broker broker, String rootPath){
        this.rootPath = rootPath;
        this.broker = broker;
    }

    public final static List<String> tablesToLoad = List.of(
            "store_sales",
            "date_dim",
            "store",
            "customer_demographics",
            "household_demographics",
            "customer_address"
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
                timings.put(0, run1_0());
                results = resultsMap.get("0");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 1:
                timings.put(1, run1_1());
                results = resultsMap.get("1");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 2:
                timings.put(2, run1_2());
                results = resultsMap.get("2");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 3:
                timings.put(3, run1_3());
                results = resultsMap.get("3");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 4:
                timings.put(4, run1_4());
                results = resultsMap.get("4");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 5:
                timings.put(5, run1_5());
                results = resultsMap.get("5");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
            case 6:
                timings.put(6, run1_6());
                results = resultsMap.get("6");
                for(RelRow r: results.getData()){
                    r.print();
                }
                return timings;
        }
        return timings;
    }

    public List<Pair<String, Long>> run1_0(){
        API api = new API(this.broker);
        List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
        List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
        List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
        List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
        List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
        List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
        List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
        List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
        List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
        List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");

        TestMethods tm = new TestMethods(rootPath);
        List<Pair<String, Long>> writeTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery j1 = api.join()
                .select(j1RawSchema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                        " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                .build();
        ReadQuery j2 = api.join()
                .select(j2RawSchema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();
        ReadQuery j3 = api.join()
                .select(j3RawSchema.toArray(new String[0]))
                .alias(j3Schema.toArray(new String[0]))
                .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                        "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                        "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                .build();
        ReadQuery j4 = api.join()
                .select(j4RawSchema.toArray(new String[0]))
                .alias(j4Schema.toArray(new String[0]))
                .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                        "hd_dep_count == 1 || hd_dep_count == 3")
                .build();
        ReadQuery j5 = api.join()
                .select(j5RawSchema.toArray(new String[0]))
                .alias(j5Schema.toArray(new String[0]))
                .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                        "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                .build();
        ReadQuery q1 = api.read()
                .avg("ss_quantity", "avg_ss_quantity")
                .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                .fromFilter(j5, "j5",
                        "(" +
                                "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                ") && (" +
                                "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                ")")
                .build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = q1.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("0", results);
        writeTimes.add(new Pair<>("Read", elapsedTime));

        return writeTimes;
    }
    public List<Pair<String, Long>> run1_1(){
        API api = new API(broker);
        List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
        List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
        List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
        List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
        List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
        List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
        List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
        List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
        List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
        List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");

        TestMethods tm = new TestMethods(rootPath);

        ReadQuery j1 = api.join()
                .select(j1RawSchema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                        " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                .stored()
                .build();

        j1.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);

        ReadQuery j2 = api.join()
                .select(j2RawSchema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();

        ReadQuery j3 = api.join()
                .select(j3RawSchema.toArray(new String[0]))
                .alias(j3Schema.toArray(new String[0]))
                .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                        "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                        "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                .build();

        ReadQuery j4 = api.join()
                .select(j4RawSchema.toArray(new String[0]))
                .alias(j4Schema.toArray(new String[0]))
                .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                        "hd_dep_count == 1 || hd_dep_count == 3")
                .build();

        ReadQuery j5 = api.join()
                .select(j5RawSchema.toArray(new String[0]))
                .alias(j5Schema.toArray(new String[0]))
                .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                        "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                .build();

        ReadQuery q1 = api.read()
                .avg("ss_quantity", "avg_ss_quantity")
                .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                .fromFilter(j5, "j5",
                        "(" +
                                "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                ") && (" +
                                "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                ")")
                .build();

        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = q1.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;

        resultsMap.put("1", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> run1_2(){
        API api = new API(broker);
        TestMethods tm = new TestMethods(rootPath);
        List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
        List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
        List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
        List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
        List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
        List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
        List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
        List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
        List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
        List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");
        ReadQuery j1 = api.join()
                .select(j1RawSchema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                        " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                .build();
        ReadQuery j2 = api.join()
                .select(j2RawSchema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .stored()
                .build();
        j2.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);
        ReadQuery j3 = api.join()
                .select(j3RawSchema.toArray(new String[0]))
                .alias(j3Schema.toArray(new String[0]))
                .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                        "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                        "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                .build();
        ReadQuery j4 = api.join()
                .select(j4RawSchema.toArray(new String[0]))
                .alias(j4Schema.toArray(new String[0]))
                .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                        "hd_dep_count == 1 || hd_dep_count == 3")
                .build();
        ReadQuery j5 = api.join()
                .select(j5RawSchema.toArray(new String[0]))
                .alias(j5Schema.toArray(new String[0]))
                .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                        "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                .build();
        ReadQuery q1 = api.read()
                .avg("ss_quantity", "avg_ss_quantity")
                .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                .fromFilter(j5, "j5",
                        "(" +
                                "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                ") && (" +
                                "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                ")")
                .build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = q1.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;
        resultsMap.put("2", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> run1_3(){
        API api = new API(broker);
        TestMethods tm = new TestMethods(rootPath);
        List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
        List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
        List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
        List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
        List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
        List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
        List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
        List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
        List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
        List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");
        ReadQuery j1 = api.join()
                .select(j1RawSchema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                        " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                .build();
        ReadQuery j2 = api.join()
                .select(j2RawSchema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();
        ReadQuery j3 = api.join()
                .select(j3RawSchema.toArray(new String[0]))
                .alias(j3Schema.toArray(new String[0]))
                .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                        "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                        "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                .stored()
                .build();
        j3.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);
        ReadQuery j4 = api.join()
                .select(j4RawSchema.toArray(new String[0]))
                .alias(j4Schema.toArray(new String[0]))
                .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                        "hd_dep_count == 1 || hd_dep_count == 3")
                .build();
        ReadQuery j5 = api.join()
                .select(j5RawSchema.toArray(new String[0]))
                .alias(j5Schema.toArray(new String[0]))
                .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                        "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                .build();
        ReadQuery q1 = api.read()
                .avg("ss_quantity", "avg_ss_quantity")
                .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                .fromFilter(j5, "j5",
                        "(" +
                                "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                ") && (" +
                                "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                ")")
                .build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = q1.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;
        resultsMap.put("3", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> run1_4(){
        API api = new API(broker);
        TestMethods tm = new TestMethods(rootPath);
        List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
        List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
        List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
        List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
        List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
        List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
        List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
        List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
        List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
        List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");
        ReadQuery j1 = api.join()
                .select(j1RawSchema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                        " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                .build();
        ReadQuery j2 = api.join()
                .select(j2RawSchema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();
        ReadQuery j3 = api.join()
                .select(j3RawSchema.toArray(new String[0]))
                .alias(j3Schema.toArray(new String[0]))
                .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                        "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                        "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                .build();
        ReadQuery j4 = api.join()
                .select(j4RawSchema.toArray(new String[0]))
                .alias(j4Schema.toArray(new String[0]))
                .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                        "hd_dep_count == 1 || hd_dep_count == 3")
                .stored()
                .build();
        j4.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);
        ReadQuery j5 = api.join()
                .select(j5RawSchema.toArray(new String[0]))
                .alias(j5Schema.toArray(new String[0]))
                .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                        "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                .build();
        ReadQuery q1 = api.read()
                .avg("ss_quantity", "avg_ss_quantity")
                .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                .fromFilter(j5, "j5",
                        "(" +
                                "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                ") && (" +
                                "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                ")")
                .build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = q1.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;
        resultsMap.put("4", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> run1_5(){
        API api = new API(broker);
        TestMethods tm = new TestMethods(rootPath);
        List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
        List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
        List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
        List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
        List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
        List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
        List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
        List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
        List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
        List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");
        ReadQuery j1 = api.join()
                .select(j1RawSchema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                        " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                .build();
        ReadQuery j2 = api.join()
                .select(j2RawSchema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();
        ReadQuery j3 = api.join()
                .select(j3RawSchema.toArray(new String[0]))
                .alias(j3Schema.toArray(new String[0]))
                .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                        "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                        "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                .build();
        ReadQuery j4 = api.join()
                .select(j4RawSchema.toArray(new String[0]))
                .alias(j4Schema.toArray(new String[0]))
                .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                        "hd_dep_count == 1 || hd_dep_count == 3")
                .build();
        ReadQuery j5 = api.join()
                .select(j5RawSchema.toArray(new String[0]))
                .alias(j5Schema.toArray(new String[0]))
                .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                        "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                .stored()
                .build();
        j5.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);
        ReadQuery q1 = api.read()
                .avg("ss_quantity", "avg_ss_quantity")
                .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                .fromFilter(j5, "j5",
                        "(" +
                                "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                ") && (" +
                                "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                ")")
                .build();
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = q1.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;
        resultsMap.put("5", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
    public List<Pair<String, Long>> run1_6(){
        API api = new API(broker);
        TestMethods tm = new TestMethods(rootPath);
        List<String> j1RawSchema = List.of("store_sales.ss_quantity", "store_sales.ss_ext_wholesale_cost", "store_sales.ss_ext_sales_price", "store_sales.ss_hdemo_sk", "store_sales.ss_cdemo_sk", "store_sales.ss_sales_price", "store_sales.ss_addr_sk", "store_sales.ss_net_profit", "store_sales.ss_store_sk", "store_sales.ss_addr_sk");
        List<String> j1Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_store_sk", "ss_customer_addr_sk");
        List<String> j2RawSchema = List.of("j1.ss_quantity", "j1.ss_ext_wholesale_cost", "j1.ss_ext_sales_price", "j1.ss_hdemo_sk", "j1.ss_cdemo_sk", "j1.ss_sales_price", "j1.ss_addr_sk", "j1.ss_net_profit", "j1.ss_customer_addr_sk");
        List<String> j2Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_cdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk");
        List<String> j3RawSchema = List.of("j2.ss_quantity", "j2.ss_ext_wholesale_cost", "j2.ss_ext_sales_price", "j2.ss_hdemo_sk", "j2.ss_sales_price", "j2.ss_addr_sk", "j2.ss_net_profit", "j2.ss_customer_addr_sk", "customer_demographics.cd_marital_status", "customer_demographics.cd_education_status");
        List<String> j3Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_hdemo_sk", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status");
        List<String> j4RawSchema = List.of("j3.ss_quantity", "j3.ss_ext_wholesale_cost", "j3.ss_ext_sales_price", "j3.ss_sales_price", "j3.ss_addr_sk", "j3.ss_net_profit", "j3.ss_customer_addr_sk", "j3.cd_marital_status", "j3.cd_education_status", "household_demographics.hd_dep_count");
        List<String> j4Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_addr_sk", "ss_net_profit", "ss_customer_addr_sk", "cd_marital_status", "cd_education_status", "hd_dep_count");
        List<String> j5RawSchema = List.of("j4.ss_quantity", "j4.ss_ext_wholesale_cost", "j4.ss_ext_sales_price", "j4.ss_sales_price", "j4.ss_net_profit", "j4.cd_marital_status", "j4.cd_education_status", "j4.hd_dep_count", "customer_address.ca_country", "customer_address.ca_state");
        List<String> j5Schema = List.of("ss_quantity", "ss_ext_wholesale_cost", "ss_ext_sales_price", "ss_sales_price", "ss_net_profit", "cd_marital_status", "cd_education_status", "hd_dep_count", "ca_country", "ca_state");
        ReadQuery j1 = api.join()
                .select(j1RawSchema.toArray(new String[0]))
                .alias(j1Schema.toArray(new String[0]))
                .sources("store_sales", "date_dim", List.of("ss_sold_date_sk"), List.of("d_date_sk"))
                .filters("ss_sales_price <= 200 && ss_sales_price >= 50" +
                        " && ss_net_profit <= 300 && ss_net_profit >= 50", "d_year == 2001")
                .build();
        ReadQuery j2 = api.join()
                .select(j2RawSchema.toArray(new String[0]))
                .alias(j2Schema.toArray(new String[0]))
                .sources(j1, "store", "j1", List.of("ss_store_sk"), List.of("s_store_sk"))
                .build();
        ReadQuery j3 = api.join()
                .select(j3RawSchema.toArray(new String[0]))
                .alias(j3Schema.toArray(new String[0]))
                .sources(j2, "customer_demographics", "j2", List.of("ss_cdemo_sk"), List.of("cd_demo_sk"))
                .filters("", "(cd_marital_status == 'D' && cd_education_status == 'Secondary')" +
                        "|| (cd_marital_status == 'U' && cd_education_status == 'Advanced Degree') " +
                        "|| (cd_marital_status == 'M' && cd_education_status == 'Primary')")
                .build();
        ReadQuery j4 = api.join()
                .select(j4RawSchema.toArray(new String[0]))
                .alias(j4Schema.toArray(new String[0]))
                .sources(j3, "household_demographics", "j3", List.of("ss_hdemo_sk"), List.of("hd_demo_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200)",
                        "hd_dep_count == 1 || hd_dep_count == 3")
                .build();
        ReadQuery j5 = api.join()
                .select(j5RawSchema.toArray(new String[0]))
                .alias(j5Schema.toArray(new String[0]))
                .sources(j4, "customer_address", "j4", List.of("ss_customer_addr_sk"), List.of("ca_address_sk"))
                .filters("(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150 && hd_dep_count == 3) " +
                                "|| (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1) " +
                                "|| (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)",
                        "['AZ', 'NE', 'IA', 'MS', 'CA', 'NV', 'GA', 'TX', 'NJ'].contains(ca_state) && ca_country == 'United States'")
                .build();
        ReadQuery q1 = api.read()
                .avg("ss_quantity", "avg_ss_quantity")
                .avg("ss_ext_sales_price", "avg_ss_ext_sales_price")
                .avg("ss_ext_wholesale_cost", "avg_ss_ext_wholesale_cost")
                .sum("ss_ext_wholesale_cost", "sum_ss_ext_wholesale_cost")
                .fromFilter(j5, "j5",
                        "(" +
                                "(cd_marital_status == 'U' && cd_education_status == 'Advanced Degree' && ss_sales_price >= 100 && ss_sales_price <= 150.00 && hd_dep_count == 3)" +
                                " || (cd_marital_status == 'M' && cd_education_status == 'Primary' && ss_sales_price >= 50 && ss_sales_price <= 100 && hd_dep_count == 1)" +
                                " || (cd_marital_status == 'D' && cd_education_status == 'Secondary' && ss_sales_price >= 150 && ss_sales_price <= 200 && hd_dep_count == 1)" +
                                ") && (" +
                                "(ca_country == 'United States' && ['AZ', 'NE', 'IA'].contains(ca_state) && ss_net_profit >= 100 && ss_net_profit <= 200 )" +
                                "(ca_country == 'United States' && ['MS', 'NV', 'CA'].contains(ca_state) && ss_net_profit >= 150 && ss_net_profit <= 300 )" +
                                "(ca_country == 'United States' && ['TX', 'NJ', 'GA'].contains(ca_state) && ss_net_profit >= 50 && ss_net_profit <= 250)" +
                                ")")
                .store()
                .build();
        q1.run(broker);
        List<Pair<String, Long>> loadTimes = tm.loadDataInMem(broker, tablesToLoad);
        long readStartTime = System.currentTimeMillis();
        RelReadQueryResults results = q1.run(broker);
        long elapsedTime = System.currentTimeMillis() - readStartTime;
        resultsMap.put("6", results);
        loadTimes.add(new Pair<>("Read", elapsedTime));

        return loadTimes;
    }
}
