package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.util.List;
import java.util.Map;

public interface VolatileRetrieveAndCombineQueryPlan<S extends Shard, T>
{
    List<String> getTableNames();

    Map<String, List<Integer>> keysForQuery();
    ByteString retrieve(S shard, String tableName);
    T combine(Map<String,List<ByteString>> retrieveResults);
}
