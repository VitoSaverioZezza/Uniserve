package edu.stanford.futuredata.uniserve.secondapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.VolatileShuffleQueryPlan;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class JoinOnWrite implements VolatileShuffleQueryPlan<List<Object>> {
    @Override
    public String getQueriedTables() {
        return null;
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(List<Object> data, int actorCount) {
        return null;
    }

    @Override
    public ByteString gather(List<ByteString> list) {
        return null;
    }

    @Override
    public List<Object> combine(List<ByteString> list) {
        return null;
    }
}
