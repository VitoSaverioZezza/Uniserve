package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;

import java.util.List;

public class SimpleWriteQuery implements SimpleWriteQueryPlan<RelRow, RelShard> {
    private final String queriedTable;

    public SimpleWriteQuery(String queriedTable){
        this.queriedTable = queriedTable;
    }
    @Override
    public String getQueriedTable() {
        return queriedTable;
    }

    @Override
    public boolean write(RelShard shard, List<RelRow> rows) {
        return shard.insertRows(rows) && shard.committRows();
    }
}
