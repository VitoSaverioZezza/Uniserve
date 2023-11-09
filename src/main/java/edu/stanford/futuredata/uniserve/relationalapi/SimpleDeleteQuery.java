package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;

import java.util.ArrayList;
import java.util.List;

public class SimpleDeleteQuery implements SimpleWriteQueryPlan<RelRow, RelShard> {
    private final String queriedTable;
    private final Boolean[] keyStructure;

    public SimpleDeleteQuery(String queriedTable, Boolean[] keyStructure){
        this.keyStructure = keyStructure;
        this.queriedTable = queriedTable;
    }

    @Override
    public String getQueriedTable() {
        return queriedTable;
    }

    @Override
    public boolean write(RelShard shard, List<RelRow> rows) {
        List<RelRow> storedData = shard.getData();
        List<RelRow> rowsToRemove = new ArrayList<>();
        for(RelRow rowToDelete: rows){
            for(RelRow storedRow: storedData){
                boolean equal = true;
                for(int i = 0; i< storedRow.getSize(); i++) {
                    if (keyStructure[i]) {
                        if (storedRow.getField(i) != null && !(storedRow.getField(i).equals(rowToDelete.getField(i)))) {
                            equal = false;
                            break;
                        }
                    }
                }
                if(equal){
                    rowsToRemove.add(storedRow);
                }
            }
        }
        shard.removeRows(rowsToRemove);
        shard.committRows();
        return true;
    }
}
