package edu.stanford.futuredata.uniserve.relationalmock.queryplans;

import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import edu.stanford.futuredata.uniserve.relationalmock.RMRowPerson;
import edu.stanford.futuredata.uniserve.relationalmock.RMShard;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.planbuilders.RMSimpleInsertPersonQueryPlanBuilder;

import java.util.List;

public class RMSimpleInsertPersonQueryPlan implements SimpleWriteQueryPlan<RMRowPerson, RMShard> {
    private String table;

    public RMSimpleInsertPersonQueryPlan(RMSimpleInsertPersonQueryPlanBuilder builder){
        this.table = builder.getTable();
    }

    public RMSimpleInsertPersonQueryPlan(){
        table = "People";
    }

    @Override
    public String getQueriedTable() {
        return table;
    }

    @Override
    public boolean write(RMShard shard, List<RMRowPerson> rows) {
        shard.getPersons().addAll(rows);
        return true;
    }
}
