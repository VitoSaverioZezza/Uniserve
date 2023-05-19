package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;
import java.util.List;

/**Defines the logic of an eventually consistent write query.*/
public interface SimpleWriteQueryPlan<R extends Row, S extends Shard> extends Serializable {
    /**A write query can be executed on a single table
     * @return The table name of the table being queried*/
    String getQueriedTable();
    /** Stage a write query on the rows.
     * @return true if the write operation has been successfully executed, false otherwise.*/
    boolean write(S shard, List<R> rows);
}
