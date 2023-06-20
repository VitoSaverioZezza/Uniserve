package edu.stanford.futuredata.uniserve.operators;

import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;

import java.util.List;

public class SimpleWriteOperator<R extends Row, S extends Shard<R>> implements SimpleWriteQueryPlan<R,S> {
    String queriedTable;

    SimpleWriteOperator(String queriedTable){
        this.queriedTable = queriedTable;
    }

    @Override
    public String getQueriedTable() {
        return queriedTable;
    }

    @Override
    public boolean write(S shard, List<R> rows) {
        try {
            shard.setRows(rows);
            shard.insertRows();
            return true;
        }catch (Exception e) {
            return false;
        }
    }
}
