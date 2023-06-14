package edu.stanford.futuredata.uniserve.relationalmock;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.QueryEngine;
import edu.stanford.futuredata.uniserve.interfaces.VolatileShuffleQueryPlan;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.RMAvgAgesOnWrite;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.RMFilterPeopleByAgeOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.RMFilterPeopleByAgeOnWriteQueryPlan;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.RMSimpleInsertPersonQueryPlan;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.planbuilders.RMFilterPeopleByAgeOnReadQueryPlanBuilder;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.planbuilders.RMFilterPeopleByAgeOnWriteQueryPlanBuilder;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.planbuilders.RMSimpleInsertPersonQueryPlanBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class RMQueryEngine implements QueryEngine {
    private final Broker broker;
    private static final Logger logger = LoggerFactory.getLogger(RMQueryEngine.class);

    public RMQueryEngine(Broker broker){
        this.broker = broker;
    }

    public boolean createTable(String tableName, int numShards){
        return broker.createTable(tableName, numShards);
    }

    public boolean insertPersons(List<RMRowPerson> listPerson, String table){
        RMSimpleInsertPersonQueryPlanBuilder builder = new RMSimpleInsertPersonQueryPlanBuilder();
        RMSimpleInsertPersonQueryPlan plan = builder.setTable(table).build();
        return broker.simpleWriteQuery(plan, listPerson);
    }

    public List<RMRowPerson> filterByAge(int minAge,
                                         int maxAge,
                                         boolean onWrite,
                                         boolean saveResults,
                                         List<RMRowPerson> personList){
        List<RMRowPerson> results;
        if(onWrite){
            RMFilterPeopleByAgeOnWriteQueryPlan plan = new RMFilterPeopleByAgeOnWriteQueryPlanBuilder()
                    .setMaxAge(maxAge)
                    .setMinAge(minAge)
                    .build();
            results = broker.volatileShuffleQuery(plan, personList);
        }else{
            RMFilterPeopleByAgeOnReadQueryPlan plan = new RMFilterPeopleByAgeOnReadQueryPlanBuilder()
                    .setMaxAge(minAge)
                    .setMinAge(maxAge)
                    .setTableName("People")
                    .build();
            results = broker.retrieveAndCombineReadQuery(plan);
        }
        if(saveResults){
            RMSimpleInsertPersonQueryPlan plan = new RMSimpleInsertPersonQueryPlanBuilder().setTable("YoungPeople").build();
            assertTrue(broker.simpleWriteQuery(plan, results));
        }
        return results;
    }

    public int avgAges(int minAge, int maxAge, String tableName){
        RMFilterPeopleByAgeOnReadQueryPlan plan = new RMFilterPeopleByAgeOnReadQueryPlanBuilder()
                .setMinAge(minAge)
                .setMaxAge(maxAge)
                .setTableName(tableName)
                .build();
        List<RMRowPerson> youngPeople = broker.retrieveAndCombineReadQuery(plan);
        RMAvgAgesOnWrite avgPlan = new RMAvgAgesOnWrite();
        return broker.volatileShuffleQuery(avgPlan, youngPeople);
    }

    public int avgAges(int minAge, int maxAge, List<RMRowPerson> rawData){
        RMFilterPeopleByAgeOnWriteQueryPlan plan = new RMFilterPeopleByAgeOnWriteQueryPlanBuilder()
                .setMaxAge(maxAge)
                .setMinAge(minAge)
                .build();
        List<RMRowPerson> youngPeople = broker.volatileShuffleQuery(plan, rawData);
        RMAvgAgesOnWrite avgPlan = new RMAvgAgesOnWrite();
        return broker.volatileShuffleQuery(avgPlan, youngPeople);
    }
}
