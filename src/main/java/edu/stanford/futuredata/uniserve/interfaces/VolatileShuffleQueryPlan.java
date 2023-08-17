package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface VolatileShuffleQueryPlan<V, S extends Shard> extends Serializable {
    String getQueriedTables();
    Map<Integer, List<ByteString>> scatter(S data, int actorCount);
    ByteString gather(List<ByteString> scatteredData);
    V combine(List<ByteString> gatherResults);

    void setTableName(String tableName);
    boolean write(S shard, List<Row> data);
}
