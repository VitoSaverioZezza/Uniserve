package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.AnchoredReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.*;

public class KVFilterSumGet implements AnchoredReadQueryPlan<KVShard, Integer> {

    private final List<Integer> keys;

    public KVFilterSumGet(List<Integer> keys) {
        this.keys = keys;
    }

    @Override
    public List<String> getQueriedTables() {
        return List.of("table", "intermediate1");
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return Map.of("table", keys);
    }

    @Override
    public String getAnchorTable() {
        return "table";
    }

    @Override
    public List<Integer> getPartitionKeys(KVShard s) {
        return List.of();
    }

    @Override
    public List<AnchoredReadQueryPlan<KVShard, Map<String, Map<Integer, Integer>>>> getSubQueries() {
        return List.of(new KVIntermediateSumGet(keys));
    }

    /**Executes once for each shard of all non-anchor tables.
     * @param shard the shard of a non-anchor table the scatter is executing on
     * @param partitionKeys a mapping between all anchor shard ids and the result of the getPartitionKeys executed for
     *                      the shard.
     * @return a mapping between anchor shard ids and lists of ByteString results that in this case are equal
     * to the serialization of the current non-anchor shard's element having key 0 (or the value 0 itself if such key
     * doesn't exist).
     **/
    @Override
    public Map<Integer, List<ByteString>> scatter(KVShard shard, Map<Integer, List<Integer>> partitionKeys) {
        int sum = shard.KVMap.getOrDefault(0, 0);
        ByteString b = Utilities.objectToByteString(sum);
        Map<Integer, List<ByteString>> ret = new HashMap<>();
        partitionKeys.keySet().forEach(k -> ret.put(k, List.of(b)));
        return ret;
    }

    /**A gather operation executes once for each shard of the anchor map and takes as input the results associated
     * to its id from all scatter executions on all non-anchor shards.
     * @param localShard the anchor shard the gather operation is executing for
     * @param ephemeralData the results of the scatter operations for the current shard id, stored as a mapping between
     *                      non-anchor table names and list containing all ByteString results for all the scatters
     *                      associated to the table's shards
     * @param ephemeralShards a mapping between table names and the ephemeral shards associated to that table for this
     *                        particular anchor shard
     * @return a ByteString result to be later combined with all other results of the gather operations associated to all
     * other anchor shards. In this case, the result
     * */
    @Override
    public ByteString gather(KVShard localShard, Map<String, List<ByteString>> ephemeralData, Map<String, KVShard> ephemeralShards) {
        int oldSum = ephemeralData.get("intermediate1").stream().map(i -> (Integer) Utilities.byteStringToObject(i)).mapToInt(i -> i).sum();
        int average = oldSum / keys.size();
        int sum = 0;
        for (Integer key : keys) {
            Optional<Integer> value = localShard.queryKey(key);
            if (key < average && value.isPresent()) {
                sum += value.get();
            }
        }
        return Utilities.objectToByteString(sum);

    }

    @Override
    public Integer combine(List<ByteString> shardQueryResults) {
        return shardQueryResults.stream().map(i -> (Integer) Utilities.byteStringToObject(i)).mapToInt(i -> i).sum();
    }
}
