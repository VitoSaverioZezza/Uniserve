package edu.stanford.futuredata.uniserve.relationalmock;

import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.QueryEngine;
import edu.stanford.futuredata.uniserve.interfaces.SerializablePredicate;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.*;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.planbuilders.RMSimpleInsertPersonQueryPlanBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class RMQueryEngine implements QueryEngine {
    private final Broker broker;
    private static final Logger logger = LoggerFactory.getLogger(RMQueryEngine.class);

    public RMQueryEngine(Broker broker){
        this.broker = broker;
    }

    public boolean createTable(String tableName, int numShards){
        return broker.createTable(tableName, numShards);
    }

    public boolean insertPersons(List<RMRow> listPerson, String table){
        RMSimpleInsertPersonQueryPlanBuilder builder = new RMSimpleInsertPersonQueryPlanBuilder();
        RMSimpleInsertPersonQueryPlan plan = builder.setTable(table).build();
        return broker.simpleWriteQuery(plan, listPerson);
    }

    public List<RMRow> filter(SerializablePredicate<RMRow> predicate, String tableName){
        return broker.retrieveAndCombineReadQuery(new RMDynFilter(predicate, tableName));
    }
}
