package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelShard;

import java.util.List;
import java.util.Map;

public class AggregateQuery implements ShuffleOnReadQueryPlan<RelShard, Object> {
    @Override
    public List<String> getQueriedTables() {
        return null;
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return null;
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(RelShard shard, int numRepartitions, String tableName) {
        return null;
    }

    @Override
    public ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, RelShard> ephemeralShards) {
        return null;
    }

    @Override
    public Object combine(List<ByteString> shardQueryResults) {
        return null;
    }

    @Override
    public boolean writeSubqueryResults(RelShard shard, String tableName, List<Object> data) {
        return ShuffleOnReadQueryPlan.super.writeSubqueryResults(shard, tableName, data);
    }
}
