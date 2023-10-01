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

    /*
    @Override
    public boolean write(RelShard shard, List<RelRow> rows) {
        shard.clear();
        shard.insertRows(rows);
        shard.committRows();
        return true;
    }
    */

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
