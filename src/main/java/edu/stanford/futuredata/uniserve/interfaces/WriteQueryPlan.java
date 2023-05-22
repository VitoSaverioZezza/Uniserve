package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;
import java.util.List;

/**Defines the logic for a 2PC style write query on a single table*/
public interface WriteQueryPlan<R extends Row, S extends Shard> extends Serializable {
    /**@return the name of the table being queried*/
    String getQueriedTable();

    /**@param shard the shard to be prepared for the commit phase.
     * @param rows the rows to be written.
     * @return true if the shard is ready to commit, false otherwise, triggering an abort of the write operation.*/
    boolean preCommit(S shard, List<R> rows);
    /**Commit the query.
     * @param shard the shard storing the data to be committed for this query*/
    void commit(S shard);
    /**Abort the query.
     * @param shard the shard storing the result of the operation to be rolled back*/
    void abort(S shard);

    boolean simpleMap(List<R> rows);
    boolean map(List<R> rows);
}
