package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**Read query plan that does not require shuffling of data, from a single table*/
public interface RetrieveAndCombineQueryPlan<S extends Shard, T> extends Serializable {

    /**@return A list containing the names of all tables to be queried*/
    List<String> getTableNames();

    /**@return a mapping between tableNames of the queried tables and a list of the partition keys to be queried on each
     * table
     */
    Map<String, List<Integer>> keysForQuery();
    ByteString retrieve(S shard, String tableName);
    T combine(Map<String,List<ByteString>> retrieveResults);
}
