package edu.stanford.futuredata.uniserve.operators;

import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;

import java.util.List;

public class WriteOperator<R extends Row, S extends Shard<R>> implements WriteQueryPlan<R, S> {
    private String queriedTable;

    WriteOperator(String queriedTable){
        this.queriedTable = queriedTable;
    }

    @Override
    public String getQueriedTable() {
        return queriedTable;
    }

    @Override
    public boolean preCommit(S shard, List<R> rows) {
        try{
            shard.setRows(rows);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    @Override
    public void commit(S shard) {
        try {
            shard.insertRows();
        }catch (Exception e){
            ;
        }
    }

    @Override
    public void abort(S shard) {

    }
}
