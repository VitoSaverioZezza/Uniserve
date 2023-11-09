package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WriteResultsPlan implements SimpleWriteQueryPlan<RelRow, RelShard> {
    private final String destinationTable;
    private final Boolean[] keyStructure;

    public WriteResultsPlan(String destinationTable, Boolean[] keyStructure){
        this.destinationTable = destinationTable;
        this.keyStructure = keyStructure;
    }
    @Override
    public String getQueriedTable() {
        return destinationTable;
    }

    @Override
    public boolean write(RelShard shard, List<RelRow> rows) {
        List<RelRow> data = shard.getData();
        List<RelRow> dataToRemove = new ArrayList<>();
        for(RelRow newRow: rows) {
            RelRow matchRow = getMatchingRow(data, newRow);
            if(matchRow != null){
                dataToRemove.add(matchRow);
            }
        }
        shard.removeRows(dataToRemove);
        shard.insertRows(rows);
        shard.committRows();
        return false;
    }

    public RelRow getMatchingRow(List<RelRow> data, RelRow newRow) {
        for (RelRow oldRow : data) {
            boolean equal = true;
            for (int i = 0; i < keyStructure.length && equal; i++) {
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
            if (equal) {
                return oldRow;
            }
        }
        return null;
    }
}
