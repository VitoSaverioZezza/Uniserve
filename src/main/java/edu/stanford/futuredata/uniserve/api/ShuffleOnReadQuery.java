package edu.stanford.futuredata.uniserve.api;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.api.lambdamethods.CombineLambdaShuffle;
import edu.stanford.futuredata.uniserve.api.lambdamethods.GatherLambda;
import edu.stanford.futuredata.uniserve.api.lambdamethods.ScatterLambda;
import edu.stanford.futuredata.uniserve.api.querybuilders.ShuffleReadQueryBuilder;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShuffleOnReadQuery implements ShuffleOnReadQueryPlan<Shard, Object> {
    private List<String> queriedTables;
    private Map<String, List<Integer>> keysForQuery;
    private Map<String, Serializable> scatterLogics;
    private Serializable gatherLogic;
    private Serializable combineLogic;


    public ShuffleOnReadQuery(ShuffleReadQueryBuilder builder){
        this.combineLogic = builder.getCombineLogic();
        this.queriedTables = builder.getTableNames();
        this.keysForQuery = builder.getKeysForQuery();
        this.gatherLogic = builder.getGatherLogic();
        this.scatterLogics = builder.getScatterLogics();
    }

    @Override
    public List<String> getQueriedTables() {
        return queriedTables;
    }
    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return keysForQuery;
    }
    @Override
    public Map<Integer, List<ByteString>> scatter(Shard shard, int numRepartitions, String tableName, Map<String, ReadQueryResults> concreteSubqueriesResults) {
        Map<Integer, List<Object>> scatterRes = runScatter(shard, tableName, numRepartitions);
        Map<Integer, List<ByteString>> serScatterRes = new HashMap<>();
        for(Map.Entry<Integer, List<Object>> entry: scatterRes.entrySet()){
            List<Object> entryValueList = entry.getValue();
            for(Object obj: entryValueList){
                Object[] objArray = new Object[]{obj};
                serScatterRes.computeIfAbsent(entry.getKey(), k->new ArrayList<>()).add(Utilities.objectToByteString(objArray));
            }
        }
        return serScatterRes;
    }

    @Override
    public ByteString gather(Map ephemeralData, Map ephemeralShards) {
        Map<String, List<ByteString>> scatterResults = (Map<String, List<ByteString>>) ephemeralData;
        Map<String, List<Object>> desScatterResult = new HashMap<>();
        for(Map.Entry<String, List<ByteString>> entry: scatterResults.entrySet()){
            List<ByteString> data = entry.getValue();
            List<Object> desData = new ArrayList<>();
            for(ByteString serDataItemArray: data){
                Object[] dataItemArray = ((Object[]) Utilities.byteStringToObject(serDataItemArray));
                desData.add(dataItemArray[0]);
            }
            desScatterResult.put(entry.getKey(), desData);
        }
        Object gatherResult = runGather(desScatterResult);
        Object[] desRetArray = new Object[]{gatherResult};
        return Utilities.objectToByteString(desRetArray);
    }
    @Override
    public Object combine(List shardQueryResults) {
        List<ByteString> serRetrievedData = shardQueryResults;
        List<Object> gatherResults = new ArrayList<>();
        for(ByteString serGatherResult: serRetrievedData){
            Object[] gatherResultArray = (Object[]) Utilities.byteStringToObject(serGatherResult);
            Object gatherResult = gatherResultArray[0];
            gatherResults.add(gatherResult);
        }
        return runCombine(gatherResults);
    }

    private Map<Integer, List<Object>> runScatter(Shard shard, String tableName, int numServers){
        return ((ScatterLambda & Serializable) scatterLogics.get(tableName)).scatter(shard, numServers);
    }
    private Object runGather(Map<String, List<Object>> scatteredData){
        return ((GatherLambda & Serializable) gatherLogic).gather(scatteredData);
    }
    private Object runCombine(List<Object> gatheredData ){
        return ((CombineLambdaShuffle & Serializable) combineLogic).combine(gatheredData);
    }

    public Object run(Broker broker){
        return broker.shuffleReadQuery(this);
    }
}
