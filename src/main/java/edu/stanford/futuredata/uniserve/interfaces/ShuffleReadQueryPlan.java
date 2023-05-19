package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface ShuffleReadQueryPlan<S extends Shard, T> extends Serializable {
    /**@returns the list of tables being queried*/
    List<String> getQueriedTables();

    /** Returns a mapping between table names and partition keys of the rows on which the query executes.
     * If the mapping contains -1, all rows are involved in the query
     * @return Map < tableName, List < Partition keys to be queried on the table>>
      */
    Map<String, List<Integer>> keysForQuery();
    // Scatter.
    Map<Integer, List<ByteString>> scatter(S shard, int numRepartitions);
    // Gather.
    ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, S> ephemeralShards);

    // The query will return the result of this function executed on all results from gather.
    T combine(List<ByteString> shardQueryResults);
}
