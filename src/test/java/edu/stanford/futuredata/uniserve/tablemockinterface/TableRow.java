package edu.stanford.futuredata.uniserve.tablemockinterface;

import edu.stanford.futuredata.uniserve.interfaces.Row;

import java.util.Map;

/**Atomic information consisting of attribute names in the form of strings with integer values
 * The TableRow exposes a key for shard allocation and is immutable
 * */
public class TableRow implements Row {
    private final int key;
    private final Map<String, Integer> row;

    /**@param key partition key assigned to the row
     * @param row data stored by the row*/
    public TableRow(Map<String, Integer> row, int key) {
        this.key = key;
        this.row = row;
    }

    /**@return partition key of the data item*/
    @Override
    public int getPartitionKey(Boolean[] keyStructure) {
        return this.key;
    }

    /**@return the data of the row*/
    public Map<String, Integer> getRow() {
        return row;
    }
}
