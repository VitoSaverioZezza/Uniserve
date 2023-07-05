package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface VolatileShuffleQueryPlan<V> extends Serializable {
    String getQueriedTables();
    Map<Integer, List<ByteString>> scatter(List<Object> data, int actorCount);
    ByteString gather(List<ByteString> scatteredData);
    V combine(List<ByteString> gatherResults);
}
