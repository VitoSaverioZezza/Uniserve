package edu.stanford.futuredata.uniserve.interfaces;

import com.google.protobuf.ByteString;
import org.apache.curator.shaded.com.google.common.primitives.Bytes;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface VolatileShuffleQueryPlan <R extends Row, V> extends Serializable {
    String getQueriedTables();
    Map<Integer, List<ByteString>> scatter(List<R> data, int actorCount);
    ByteString gather(List<ByteString> scatteredData);
    V combine(List<ByteString> gatherResults);
}
