package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.interfaces.WriteQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;

import java.util.ArrayList;
import java.util.List;

public class WriteQuery implements WriteQueryPlan<RelRow, RelShard> {
    private final String queriedTable;
    private final Boolean[] keyStructure;

    public WriteQuery(String queriedTable, Boolean[] keyStructure){
        this.keyStructure = keyStructure;
        this.queriedTable = queriedTable;
    }

    @Override
    public String getQueriedTable() {
        return queriedTable;
    }

    @Override
    public boolean preCommit(RelShard shard, List<RelRow> rows) {
        List<RelRow> rowsToBeUpdated = new ArrayList<>();
        List<RelRow> data = shard.getData();
        for(RelRow newRow: rows){
            for(RelRow oldRow: data){
                boolean equal = true;
                for(int i = 0; i<oldRow.getSize(); i++){
                    Object newField = newRow.getField(i);
                    Object oldField = oldRow.getField(i);
                    if(keyStructure[i]){
                        if(newField == null && oldField == null){
                            continue;
                        }else if(oldField == null){
                            equal = false;
                            break;
                        }else if(!(oldRow.getField(i).equals(newRow.getField(i)))) {
                            equal = false;
                            break;
                        }
                    }
                }
                if(equal){
                    rowsToBeUpdated.add(oldRow);
                    break;
                }
            }
        }
        shard.removeRows(rowsToBeUpdated);
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
