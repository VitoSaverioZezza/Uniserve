package edu.stanford.futuredata.uniserve.interfaces;

import java.io.Serializable;
import java.util.List;

public interface SimpleWriteSimpleTransform <R extends Row, S extends Shard> extends Serializable {
    /**Get the name of the table involved in the query
     * @return The name of the  queried table as a String*/
    String getQueriedTable();

    /**Perform an eventually consistent write
     *
     * @param shard The shard to be written
     * @param rows The list of rows involved in the write query
     * @return true if the write operation has successfully been executed, false otherwise
     */
    boolean write(S shard, List<R> rows);

    /**Apply the transformation to the rows
     *
     * @param rows The list of rows involved in the write query
     * @return true if the transformation has successfully been executed, false otherwise
     * */
    boolean transform(List<R> rows);
}
