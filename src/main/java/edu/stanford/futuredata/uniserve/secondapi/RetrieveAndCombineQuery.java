package edu.stanford.futuredata.uniserve.secondapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.Shard;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.CombineLambdaRetAndComb;
import edu.stanford.futuredata.uniserve.secondapi.lambdamethods.RetrieveLambda;
import edu.stanford.futuredata.uniserve.secondapi.querybuilders.RetrieveAndCombineQueryBuilder;
import edu.stanford.futuredata.uniserve.utilities.Utilities;

import java.io.Serializable;
import java.util.*;

public class RetrieveAndCombineQuery implements RetrieveAndCombineQueryPlan{
    private List<String> tableNames;
    private Map<String, List<Integer>> keysForQuery;
    private Serializable combineLambda;

    private Object retrieveLambdas;


    public RetrieveAndCombineQuery(RetrieveAndCombineQueryBuilder builder){
        this.combineLambda = builder.getCombineLambda();
        this.retrieveLambdas = builder.getRetrieveLogic();
        this.tableNames = builder.getTableNames();
        this.keysForQuery = builder.getKeysForQuery();
    }


    @Override
    public List<String> getTableNames() {
        return tableNames;
    }
    @Override
    public Map<String, List<Integer>> keysForQuery() {
        return keysForQuery;
    }
    @Override
    public ByteString retrieve(Shard shard, String tableName) {
        Collection retrievedData = retrieveData(shard, tableName);
        List<Object> retrDataList = new ArrayList<>(retrievedData);
        Object[] retrDataArray = retrDataList.toArray();
        return Utilities.objectToByteString(retrDataArray);
    }
    @Override
    public Object combine(Map map) {
        Map<String, List<ByteString>> serRetrievedData = (Map<String, List<ByteString>>) map;
        Map<String, List<Object>> retrievedData = new HashMap<>();
        for(Map.Entry<String, List<ByteString>> entry: serRetrievedData.entrySet()){
            for(ByteString serObjArray: entry.getValue()){
                Object[] desObjArray = (Object[]) Utilities.byteStringToObject(serObjArray);
                List<Object> desObjList = new ArrayList<>(Arrays.asList(desObjArray));
                retrievedData.computeIfAbsent(entry.getKey(), k->new ArrayList<>()).addAll(desObjList);
            }
        }
        return combineData(retrievedData);
    }



    private Collection retrieveData(Shard shard, String tableName){
        return ((RetrieveLambda & Serializable)  ((Map<String, RetrieveLambda>)retrieveLambdas).get(tableName)).retrieve(shard, tableName);
    }
    private Object combineData(Map<String, List<Object>> retrievedData){
        return ((CombineLambdaRetAndComb & Serializable) combineLambda).combine(retrievedData);
    }


    public Object run(Broker broker){
        return broker.retrieveAndCombineReadQuery(this);
    }
}
