package edu.stanford.futuredata.uniserve.queryapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.queryapi.predicates.ExtractLambda;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.*;

public abstract class JoinOnReadQuery<S extends Shard> implements Serializable, ShuffleOnReadQueryPlan<S, List<Pair<Object,Object>>>{
    private final List<String> queriedTables = new ArrayList<>();
    private final Map<String, List<Integer>> keysForQuery;

    public JoinOnReadQuery(String tableOne,
                           String tableTwo,
                           Map<String, List<Integer>> keysForQuery
                           ){
        this.queriedTables.add(0, tableOne);
        this.queriedTables.add(1, tableTwo);
        this.keysForQuery = keysForQuery;
    }

    @Override
    public List<String> getQueriedTables() {
        return queriedTables;
    }

    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return keysForQuery;
    }

    public abstract List<Pair<Object, Object>> extractFromTableOne(S shard);
    public abstract List<Pair<Object, Object>> extractFromTableTwo(S shard);

    @Override
    public Map<Integer, List<ByteString>> scatter(S shard, int numRepartitions, String tableName) {
        Map<Integer, List<ByteString>> r = new HashMap<>();
        List<Pair<Object, Object>> keyDataPairsList;
        try{
            if(tableName.equals(getQueriedTables().get(0))){
                keyDataPairsList = extractFromTableOne(shard);
            }else{
                keyDataPairsList = extractFromTableTwo(shard);
            }
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

        for(Pair<Object, Object> keyData: keyDataPairsList){
            int key = keyData.getValue0().hashCode() % numRepartitions;
            Pair<Object, Object>[] o = new Pair[]{keyData};
            r.computeIfAbsent(key, k->new ArrayList<>()).add(Utilities.objectToByteString(o));
        }
        return r;
    }

    @Override
    public ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, S> ephemeralShards) {
        List<ByteString> dataTableOne = ephemeralData.get(queriedTables.get(0));
        List<ByteString> dataTableTwo = ephemeralData.get(queriedTables.get(1));
        ArrayList<Pair<Object, Object>> matchingPairs = new ArrayList<>();

        List<Pair<Object, Object>> keyDataListOne = new ArrayList<>();
        List<Pair<Object, Object>> keyDataListTwo = new ArrayList<>();

        for(ByteString serPair: dataTableOne){
            Pair<Object, Object>[] pairs = (Pair<Object, Object>[]) Utilities.byteStringToObject(serPair);
            Collections.addAll(keyDataListOne, pairs);
        }
        for(ByteString serPair: dataTableTwo){
            Pair<Object, Object>[] pairs = (Pair<Object, Object>[]) Utilities.byteStringToObject(serPair);
            Collections.addAll(keyDataListTwo, pairs);
        }

        for(Pair<Object,Object> obj1: keyDataListOne){
            for(Pair<Object, Object> obj2: keyDataListTwo){
                if(obj2.getValue0().equals(obj1.getValue0())){
                    matchingPairs.add(new Pair<>(obj1.getValue1(), obj2.getValue1()));
                }
            }
        }

        Pair[] pairsArray = matchingPairs.toArray(new Pair[0]);
        return Utilities.objectToByteString(pairsArray);
    }

    @Override
    public List<Pair<Object, Object>> combine(List<ByteString> shardQueryResults) {
        List<Pair<Object, Object>> results = new ArrayList<>();
        for(ByteString serArray: shardQueryResults){
            Pair<Object, Object>[] pairsArray = (Pair<Object, Object>[]) Utilities.byteStringToObject(serArray);
            Collections.addAll(results, pairsArray);
        }
        return results;
    }
}
