package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.relationalapi.ReadQuery;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**Read query plan that does not require shuffling of data, from a single table*/
public interface RetrieveAndCombineQueryPlan<S extends Shard, T> extends Serializable {

    /**@return A list containing the names of all tables to be queried*/
    List<String> getTableNames();
    boolean isThisSubquery();
    /**@return a mapping between tableNames of the queried tables and a list of the partition keys to be queried on each
     * table
     */
    Map<String, List<Integer>> keysForQuery();
    ByteString retrieve(S shard, String tableName, Map<String, ReadQueryResults> concreteSubqueriesResults);
    T combine(Map<String,List<ByteString>> retrieveResults);
    default boolean writeIntermediateShard(S intermediateShard, ByteString retrievedResults){return true;}

    default Map<String, ReadQuery> getVolatileSubqueries(){return new HashMap<>();}
    default Map<String, ReadQuery> getConcreteSubqueries(){return new HashMap<>();}
    default boolean isStored(){return false;}
    default String getResultTableName(){return "";}
    default SimpleWriteQueryPlan getWriteResultPlan(){return null;}
}
