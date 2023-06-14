package edu.stanford.futuredata.uniserve.relationalmock.queryplans;

import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import edu.stanford.futuredata.uniserve.relationalmock.RMRowMovie;
import edu.stanford.futuredata.uniserve.relationalmock.RMShard;
import edu.stanford.futuredata.uniserve.relationalmock.queryplans.planbuilders.RMSimpleInsertMovieQueryPlanBuilder;

import java.util.List;

public class RMSimpleInsertMovieQueryPlan implements SimpleWriteQueryPlan<RMRowMovie, RMShard> {
    private String table = "Movies";

    public RMSimpleInsertMovieQueryPlan(RMSimpleInsertMovieQueryPlanBuilder builder){
        this.table = builder.getTable();
    }

    @Override
    public String getQueriedTable() {
        return table;
    }

    @Override
    public boolean write(RMShard shard, List<RMRowMovie> rows) {
        //shard.getMovies().addAll(rows);
        return true;
    }
}
