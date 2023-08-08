package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelShard;

import java.util.List;
import java.util.Map;

public class IntermediateQuery implements RetrieveAndCombineQueryPlan<RelShard, RelReadQueryResults> {

    @Override
    public List<String> getTableNames() {
        return null;
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return null;
    }

    @Override
    public ByteString retrieve(RelShard shard, String tableName) {
        return null;
    }

    @Override
    public RelReadQueryResults combine(Map<String, List<ByteString>> retrieveResults) {
        return null;
    }
}
