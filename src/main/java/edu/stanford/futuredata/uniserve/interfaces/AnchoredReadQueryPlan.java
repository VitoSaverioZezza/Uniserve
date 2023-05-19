package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface AnchoredReadQueryPlan<S extends Shard, T> extends Serializable {
    /**
     * @return the list of table names being queried
     * */
    List<String> getQueriedTables();
    /**
     * @return Mapping between the names of the tables involved in the query and the relative keys on which the query
     * executes.  Query will execute on all shards containing any key from the list. The list includes -1
     * if the query needs to be executed on all table's shards.
     * */
    Map<String, List<Integer>> keysForQuery();

    /**@return the name of the table that does not need shuffling*/
    String getAnchorTable();

    /**Query plans of sub queries*/
    default List<AnchoredReadQueryPlan<S, Map<String, Map<Integer, Integer>>>> getSubQueries() {return List.of();}


    /**Given a Shard, get partition keys of the rows (contained in? To be queried in?) the shard. The
     * Call in ServiceBrokerDataStore suggests that this method returns all the partition keys of all rows in the
     * shard, since the returned list is forwarded to other datastores as "AllPartitionKeys"*/
    List<Integer> getPartitionKeys(S s);


    /**
     * A scatter is executed for each shard of all non-anchor tables and provides a result for each shard of the anchor table.
     * In particular:
     * A single scatter will return a List of ByteStrings for each shard of the anchor table.
     * The operation has access to the partition keys associated with each anchor-shard.
     * The partition keys lists are the results of AnchoredReadQueryPlan.getPartitionKeys(anchor shard)
     * <p></p>
     * @param shard shard of the non-anchor table triggering the call
     * @param partitionKeys Mapping between < anchor shard id, getPartitionKeys(anchor shard) >.
     * @return Map < AnchorShardID, List < ByteString >>
     */
    Map<Integer, List<ByteString>> scatter(S shard, Map<Integer, List<Integer>> partitionKeys);

    // Gather.
    ByteString gather(S localShard, Map<String, List<ByteString>> ephemeralData, Map<String, S> ephemeralShards);
    // Gather.
    default void gather(S localShard, Map<String, List<ByteString>> ephemeralData, Map<String, S> ephemeralShards, S returnShard) { }
    /**If the query returns its result as a shard, this optional structure returned by the call will contain the name
     * of the table the shard is stored in. If the query returns an aggregate value, the query returns an empty optional
     * object.
     * @return The name of the table storing the shard containing the query results or an empty Optional object
     * if the query returns an aggregate value */
    default Optional<String> returnTableName() {return Optional.empty();}
    // The query will return the result of this function executed on all results from gather.
    T combine(List<ByteString> shardQueryResults);
}
