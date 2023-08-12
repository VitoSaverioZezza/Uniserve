package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;

import java.io.Serializable;
import java.util.HashMap;
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

    default boolean writeSubqueryResults(S shard, String tableName, List<Object> data){return true;}
    default Map<String, ReadQuery> getSubqueriesResults(){return new HashMap<>();}
}
