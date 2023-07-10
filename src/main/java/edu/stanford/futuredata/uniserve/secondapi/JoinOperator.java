package edu.stanford.futuredata.uniserve.secondapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.ExtractFromShardKey;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.ExtractFromShardLambda;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.*;

public class JoinOperator<S extends Shard> implements ShuffleOnReadQueryPlan<S, List<Object>> {
    private final String tableOne;
    private final String tableTwo;
    private final Map<String, List<Integer>> keysForQuery = new HashMap<>();
    private ExtractFromShardLambda<S, Collection> extractKeysOne;
    private ExtractFromShardLambda<S, Collection> extractKeysTwo;
    private ExtractFromShardKey<S, Object> extractDataOne;
    private ExtractFromShardKey<S, Object> extractDataTwo;

    private Serializable serEKOne;
    private Serializable serEKTwo;
    private Serializable serEDOne;
    private Serializable serEDTwo;

    public JoinOperator(String tableOne, String tableTwo){
        this.tableOne = tableOne;
        this.tableTwo = tableTwo;
        keysForQuery.put(tableOne, List.of(-1));
        keysForQuery.put(tableTwo, List.of(-1));
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(S shard, int numServer, String tableName) {
        Map<Integer, List<ByteString>> returned = new HashMap<>();
        Collection keysTableOne;
        Collection keysTableTwo;
        List<Pair<Object, Object>> results = new ArrayList<>();

        if(tableName.equals(tableOne)) {
            keysTableOne = extractTableOne(shard);
            if(keysTableOne == null){
                return null;
            }
            for (Object key : keysTableOne) {
                Object value = extractDataOne(shard, key);
                if (value != null) {
                    results.add(new Pair<>(key, value));
                }
            }
        }else{
            keysTableTwo = extractTableTwo(shard);
            if(keysTableTwo == null){
                return null;
            }
            for(Object key: keysTableTwo){
                Object value = extractDataTwo(shard, key);
                if(value!=null) {
                    results.add(new Pair<>(key, value));
                }
            }
        }

        for(Pair<Object, Object> assignment: results){
            int key = assignment.getValue0().hashCode() % numServer;
            Object[] objArray = new Object[]{assignment};
            returned.computeIfAbsent(key, k->new ArrayList<>()).add(Utilities.objectToByteString(objArray));
        }
        return returned;
    }

    @Override
    public ByteString gather(Map<String, List<ByteString>> scatteredData, Map<String, S> ephemeralShards) {
        List<ByteString> tableOnePairs = scatteredData.get(tableOne);
        List<ByteString> tableTwoPairs = scatteredData.get(tableTwo);
        List<Pair<Object, Object>> desTableOnePairs = new ArrayList<>();
        List<Pair<Object, Object>> desTableTwoPairs = new ArrayList<>();
        List<Pair<Object, Object>> matchingPairs = new ArrayList<>();

        for(ByteString serObjArray: tableOnePairs){
            Object[] objArray = (Object[]) Utilities.byteStringToObject(serObjArray);
            Object item = objArray[0];
            Pair<Object, Object> pair = (Pair<Object, Object>) item;
            desTableOnePairs.add(pair);
        }
        for(ByteString serObjArray: tableTwoPairs){
            Object[] objArray = (Object[]) Utilities.byteStringToObject(serObjArray);
            desTableTwoPairs.add((Pair<Object, Object>) objArray[0]);
        }

        for(Pair<Object, Object> itemTableOne: desTableOnePairs){
            for(Pair<Object, Object> itemTableTwo: desTableTwoPairs){
                if(itemTableOne.getValue0().equals(itemTableTwo.getValue0())){
                    matchingPairs.add(new Pair<>(itemTableOne.getValue1(), itemTableTwo.getValue1()));
                }
            }
        }
        Object[] ret = matchingPairs.toArray();
        return Utilities.objectToByteString(ret);
    }

    @Override
    public List<Object> combine(List<ByteString> gatherResults) {
        List<Object> allPairs = new ArrayList<>();
        for(ByteString item: gatherResults){
            Object[] gatherResult = (Object[]) Utilities.byteStringToObject(item);
            Collections.addAll(allPairs, gatherResult);
        }
        return allPairs;
    }


    @Override
    public List<String> getQueriedTables() {
        return List.of(tableOne, tableTwo);
    }
    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return keysForQuery;
    }

    public void setExtractDataOne(ExtractFromShardKey<S, Object> extractDataOne){
        this.serEDOne = extractDataOne;
    }
    public void setExtractDataTwo(ExtractFromShardKey<S, Object> extractDataTwo){
        this.serEDTwo = extractDataTwo;
    }
    public void setExtractKeysOne(ExtractFromShardLambda<S, Collection> extractOne){
        this.serEKOne = extractOne;
    }
    public void setExtractKeysTwo(ExtractFromShardLambda<S, Collection> extractTwo){
        this.serEKTwo = extractTwo;
    }

    private Collection extractTableOne(S shard){
        extractKeysOne = (ExtractFromShardLambda<S, Collection>) serEKOne;
        return extractKeysOne.extract(shard);
    }
    private Collection extractTableTwo(S shard){
        extractKeysTwo = (ExtractFromShardLambda<S, Collection>) serEKTwo;
        return extractKeysTwo.extract(shard);
    }
    private Object extractDataOne(S shard, Object key){
        extractDataOne = (ExtractFromShardKey<S, Object>) serEDOne;
        return extractDataOne.extract(shard, key);
    }
    private Object extractDataTwo(S shard, Object key){
        extractDataTwo = (ExtractFromShardKey<S, Object>) serEDTwo;
        return extractDataTwo.extract(shard, key);
    }
}
