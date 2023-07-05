package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface ShuffleOnReadQueryPlan<S extends Shard, T> extends Serializable {
    /**@returns the list of tables being queried*/
    List<String> getQueriedTables();
    /** Returns a mapping between table names and partition keys of the rows on which the query executes.
     * If the mapping contains -1, all rows are involved in the query
     * @return Map < tableName, List < Partition keys to be queried on the table>>
      */
    Map<String, List<Integer>> keysForQuery();
    /**User-defined scatter operation.
     * A scatter is executed once for each shard involved in the query
     * @param shard the shard for which the shard is being executed
     * @param numRepartitions the number of actors in the query
     * @return a mapping between datastores id and serialized results. The results will be given as parameters for the
     * gather operation.
     * */
    Map<Integer, List<ByteString>> scatter(S shard, int numRepartitions, String tableName);
    /** User-definde Gather operation
     * A gather is executed by every datastore involved in the query
     * @param ephemeralData a mapping between table identifiers and a List < ByteStrings given to the
     *                      datastore executing the gather as a result of all scatter operations executed for all shards
     *                      of the given table >
     * @param ephemeralShards a mapping between table identifiers and ephemeral shards associated to it
     * @return a result to be used as input for the combine operation
     * */
    ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, S> ephemeralShards);
    /** The query will return the result of this function executed on all results from gather.
     * @param shardQueryResults values returned by all gather operations executed by all datastores
     * @return the query result*/
    T combine(List<ByteString> shardQueryResults);
}
