package edu.stanford.futuredata.uniserve.kvmockinterface.queryplans;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryPlan;
import edu.stanford.futuredata.uniserve.kvmockinterface.KVShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.Collections;
import java.util.List;

public class KVReadQueryPlanGet implements ReadQueryPlan<KVShard, Integer> {

    private final String tableName;

    private final Integer key;

    public KVReadQueryPlanGet(Integer key) {
        this.key = key;
        this.tableName = "table";
    }

    public KVReadQueryPlanGet(String tableName, Integer key) {
        this.key = key;
        this.tableName = tableName;
    }

    @Override
    public List<String> getQueriedTables() {
        return Collections.singletonList(tableName);
    }

    @Override
    public List<Integer> keysForQuery() {
        return Collections.singletonList(this.key);
    }

    @Override
    public ByteString queryShard(List<KVShard> shard) {
        return Utilities.objectToByteString(shard.get(0).queryKey(this.key).get());
    }

    @Override
    public ByteString queryShard(KVShard shard, long startTime, long endTime) {
        return null;
    }

    @Override
    public ByteString combineIntermediates(List<ByteString> intermediates) {
        return null;
    }

    @Override
    public Integer aggregateShardQueries(List<ByteString> shardQueryResults) {
        return (Integer) Utilities.byteStringToObject(shardQueryResults.get(0));
    }

    @Override
    public int getQueryCost() {
        return 1;
    }

    @Override
    public List<ReadQueryPlan> getSubQueries() {return Collections.emptyList();}

    @Override
    public void setSubQueryResults(List<Object> subQueryResults) {}
}