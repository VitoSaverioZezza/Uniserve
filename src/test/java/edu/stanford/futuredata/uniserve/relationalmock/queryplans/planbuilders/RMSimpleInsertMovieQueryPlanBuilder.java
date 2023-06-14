package edu.stanford.futuredata.uniserve.relationalmock.queryplans.planbuilders;

import edu.stanford.futuredata.uniserve.relationalmock.queryplans.RMSimpleInsertMovieQueryPlan;

public class RMSimpleInsertMovieQueryPlanBuilder {
    private String table;

    public RMSimpleInsertMovieQueryPlanBuilder setTable(String table) {
        this.table = table;
        return this;
    }

    public String getTable() {
        return table;
    }

    public RMSimpleInsertMovieQueryPlan build(){
        return new RMSimpleInsertMovieQueryPlan(this);
    }
}
