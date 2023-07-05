package edu.stanford.futuredata.uniserve.queryapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AggregateOnReadQuery<S extends Shard> implements RetrieveAndCombineQueryPlan<S, Object> {
    private final List<String> queriedTables;
    private final Map<String, List<Integer>> keysForQuery;

    public abstract List<Object> extractDataFromShard(S s);
    public abstract Object computePartialResult(List<Object> data);
    public abstract Object computeResult(List<Object> partialResults);

    public AggregateOnReadQuery(List<String> queriedTables, Map<String , List<Integer>> keysForQuery){
        this.queriedTables = queriedTables;
        this.keysForQuery = keysForQuery;
    }

    @Override
    public List<String> getTableNames() {
        return queriedTables;
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return keysForQuery;
    }

    @Override
    public ByteString retrieve(S s) {
        List<Object> extractedData = extractDataFromShard(s);
        Object[] partialResult = new Object[]{computePartialResult(extractedData)};
        return Utilities.objectToByteString(partialResult);
    }

    @Override
    public Object combine(Map<String, List<ByteString>> map) {
        List<Object> partialResult = new ArrayList<>();
        for(Map.Entry<String, List<ByteString>> e: map.entrySet()){
            List<ByteString> partialResultsSerializedArrays = e.getValue();
            for(ByteString partialResultSerializedArray: partialResultsSerializedArrays){
                Object[] partialResultArray = (Object[]) Utilities.byteStringToObject(partialResultSerializedArray);
                partialResult.add(partialResultArray[0]);
            }
        }
        return computeResult(partialResult);
    }
}
