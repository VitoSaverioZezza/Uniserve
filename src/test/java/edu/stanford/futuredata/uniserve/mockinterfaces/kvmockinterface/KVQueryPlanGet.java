package edu.stanford.futuredata.uniserve.mockinterfaces.kvmockinterface;

import edu.stanford.futuredata.uniserve.interfaces.QueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;

import java.util.Collections;
import java.util.List;

public class KVQueryPlanGet implements QueryPlan<Integer> {

    private final Integer key;

    public KVQueryPlanGet(Integer key) {
        this.key = key;
    }

    @Override
    public List<Integer> keysForQuery() {
        return Collections.singletonList(this.key);
    }

    @Override
    public Integer queryShard(Shard shard) {
        return ((KVShard) shard).queryKey(this.key).get();
    }

    @Override
    public String aggregateShardQueries(List<Integer> shardQueryResults) {
        return Integer.toString(shardQueryResults.get(0));
    }
}
