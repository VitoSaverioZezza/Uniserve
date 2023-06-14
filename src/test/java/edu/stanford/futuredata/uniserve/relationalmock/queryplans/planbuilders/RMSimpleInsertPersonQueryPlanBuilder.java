package edu.stanford.futuredata.uniserve.relationalmock.queryplans.planbuilders;

import edu.stanford.futuredata.uniserve.relationalmock.RMRowPerson;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.RMSimpleInsertPersonQueryPlan;

import java.util.List;

public class RMSimpleInsertPersonQueryPlanBuilder {
    private String table;

    public String getTable() {
        return table;
    }

    public RMSimpleInsertPersonQueryPlanBuilder setTable(String table) {
        this.table = table;
        return this;
    }


    public RMSimpleInsertPersonQueryPlan build(){
        return new RMSimpleInsertPersonQueryPlan(this);
    }
}
