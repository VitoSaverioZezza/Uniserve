package edu.stanford.futuredata.uniserve.api;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.api.lambdamethods.ExtractFromShardLambda;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SelectQuery<S extends Shard> implements RetrieveAndCombineQueryPlan<S, List<Object>> {
    private final String tableName;
    private final Map<String, List<Integer>> keysForQuery;
    Serializable extractFromShardLambda;

    public SelectQuery(String tableName){
        this.tableName = tableName;
        this.keysForQuery = Map.of(tableName, List.of(-1));
    }

    @Override
    public List<String> getTableNames() {
        return List.of(tableName);
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return keysForQuery;
    }

    //Serialized Object[] with a single element in pos 0
    //said element is an ArrayList of Object->Row->KVRow
    @Override
    public ByteString retrieve(S s, String tableName) {
        Object[] obj = new Object[]{extract(s)};
        return Utilities.objectToByteString(obj);
    }

    @Override
    public List<Object> combine(Map<String, List<ByteString>> map) {
        List<ByteString> serRetrievedResults = map.get(tableName);
        List<Object> retrievedResults = new ArrayList<>();

        for(ByteString serRetArray: serRetrievedResults){
            Object[] objArray = (Object[]) Utilities.byteStringToObject(serRetArray);
            List<Object> results = (List<Object>) objArray[0];
            retrievedResults.addAll(results);
        }
        return retrievedResults;
    }

    public void setKeysForQuery(List<Integer> keys){
        this.keysForQuery.put(tableName, keys);
    }

    public void setExtractFromShardLambda(Serializable extractFromShardLambda) {
        this.extractFromShardLambda = extractFromShardLambda;
    }

    //ARRAYLIST of Object->rows->kvrows
    private Object extract(S shard){
        return ((ExtractFromShardLambda<S, Object>) extractFromShardLambda).extract(shard);
    }
}
