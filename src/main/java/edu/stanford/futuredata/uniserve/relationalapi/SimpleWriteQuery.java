package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;

import java.util.ArrayList;
import java.util.List;

public class SimpleWriteQuery implements SimpleWriteQueryPlan<RelRow, RelShard> {
    private final String queriedTable;
    private final Boolean[] keyStructure;

    public SimpleWriteQuery(String queriedTable, Boolean[] keyStructure){
        this.keyStructure = keyStructure;
        this.queriedTable = queriedTable;
    }
    @Override
    public String getQueriedTable() {
        return queriedTable;
    }

    @Override
    public boolean write(RelShard shard, List<RelRow> rows) {
        List<RelRow> rowsToBeUpdated = new ArrayList<>();
        List<RelRow> data = shard.getData();
        for(int i = 0; i<rows.size(); i++){
            boolean isDuplicate = true;
            RelRow newRow = rows.get(i);
            for(int j = 0; j<data.size() && isDuplicate; j++){
                RelRow currentRow = data.get(j);
                for(int k = 0; k<currentRow.getSize() && isDuplicate; k++){
                    if(keyStructure[k] && !newRow.getField(k).equals(currentRow.getField(k))){
                        isDuplicate = false;
                    }
                }
                if(isDuplicate){
                    rowsToBeUpdated.add(currentRow);
                    break;
                }
            }
        }
        shard.removeRows(rowsToBeUpdated);
        return shard.insertRows(rows) && shard.committRows();
    }
}
