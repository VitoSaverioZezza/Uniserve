package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.WriteQueryBuilder;
import edu.stanford.futuredata.uniserve.utilities.TableInfo;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteBatch {
    private static final Logger logger = LoggerFactory.getLogger(WriteBatch.class);
    private final Broker broker;
    private List<WriteQueryBuilder> plans = new ArrayList<>();

    public WriteBatch(Broker broker){
        this.broker = broker;
    }

    public WriteBatch setPlans(WriteQueryBuilder... plans){
        this.plans.addAll(List.of(plans));
        return this;
    }

    public Pair<Boolean, List<Pair<String, Long>>> run(){
        Map<String, ReadQuery> triggeredQueries = new HashMap<>();
        List<Pair<String, Long>> writeTimes = new ArrayList<>();
        boolean success = true;
        for(WriteQueryBuilder plan: plans){
            TableInfo ti = broker.getTableInfo(plan.getTable());
            if(ti==null || ti.equals(Broker.NIL_TABLE_INFO)){
                throw new RuntimeException("Invalid table name: " + plan.getTable());
            }
            List<ReadQuery> tableTriggeredQueries = ti.getRegisteredQueries();
            for(ReadQuery query: tableTriggeredQueries){
                triggeredQueries.put(query.getResultTableName(), query);
            }
            if(success) {
                logger.info("Writing {} to table {}", plan.numDataItems(), plan.getTable());
                long t = System.currentTimeMillis();
                success = plan.runNoStoreUpdate();
                long elapsedTime = System.currentTimeMillis() - t;
                writeTimes.add(new Pair<>(plan.getTable(), elapsedTime));
            }else{
                return new Pair<>(false, writeTimes);
            }
        }
        for(Map.Entry<String,ReadQuery> triggeredQuery: triggeredQueries.entrySet()){
            logger.info("Updating stored query result table {}", triggeredQuery.getKey());
            long t = System.currentTimeMillis();
            triggeredQuery.getValue().updateStoredResults(broker);
            long elapsedTime = System.currentTimeMillis()-t;
            writeTimes.add(new Pair<>(triggeredQuery.getKey(), elapsedTime));
        }
        return new Pair<>(success, writeTimes);
    }
}
