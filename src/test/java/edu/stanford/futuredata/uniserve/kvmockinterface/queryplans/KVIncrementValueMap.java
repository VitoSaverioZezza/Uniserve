package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import edu.stanford.futuredata.uniserve.interfaces.MapQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVRow;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;

import java.util.List;

public class KVIncrementValueMap implements MapQueryPlan<KVRow> {

    private final String tableName;

    public KVIncrementValueMap(String tableName){this.tableName = tableName;}
    public KVIncrementValueMap() {
        this.tableName = "table1";
    }

    @Override
    public String getQueriedTable() {
        return tableName;
    }

    @Override
    public boolean map(List<KVRow> rows) {
        for(KVRow row: rows){
            row.incrementValue();
            if(row.getValue()==1235){
                return false;
            }
        }
        return true;
    }
}
