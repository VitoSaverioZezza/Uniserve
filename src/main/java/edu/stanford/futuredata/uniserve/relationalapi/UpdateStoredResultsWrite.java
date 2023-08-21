package edu.stanford.futuredata.uniserve.relationalapi;

import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;

import java.util.ArrayList;
import java.util.List;

public class UpdateStoredResultsWrite implements SimpleWriteQueryPlan<RelRow, RelShard> {
    private String queriedTable;
    private Boolean[] keyStructure = new Boolean[0];


    public UpdateStoredResultsWrite(Boolean[] keyStructure, String name){
        this.keyStructure = keyStructure;
        this.queriedTable = name;
    }

    public void setKeyStructure(Boolean[] keyStructure) {
        this.keyStructure = keyStructure;
    }

    public void setQueriedTable(String queriedTable) {
        this.queriedTable = queriedTable;
    }

    @Override
    public String getQueriedTable() {
        return queriedTable;
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
        for (RelRow savedRow : data) {
            boolean equal = true;
            for (int i = 0; i < keyStructure.length && equal; i++) {
                if (keyStructure[i] && !savedRow.getField(i).equals(newRow.getField(i))) {
                    equal = false;
                }
            }
            if (equal) {
                return savedRow;
            }
        }
        return null;
    }
}
