package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;

import java.util.List;

public class WriteQuery implements WriteQueryPlan<RelRow, RelShard> {
    private final String queriedTable;

    public WriteQuery(String queriedTable){
        this.queriedTable = queriedTable;
    }

    @Override
    public String getQueriedTable() {
        return queriedTable;
    }

    @Override
    public boolean preCommit(RelShard shard, List<RelRow> rows) {
        return shard.insertRows(rows);
    }

    @Override
    public void commit(RelShard shard) {
        shard.committRows();
    }

    @Override
    public void abort(RelShard shard) {
        shard.abortTransactions();
    }
}
