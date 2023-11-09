package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.apache.commons.jexl3.*;
import org.javatuples.Pair;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class FilterAndProjectionQuery implements RetrieveAndCombineQueryPlan<RelShard, RelReadQueryResults> {
    private String sourceName = "";  //tableName or subquery alias
    private List<String> resultSchema = new ArrayList<>();
    private List<String> systemResultSchema = new ArrayList<>();
    private String filterPredicate = "";
    private Serializable cachedFilterPredicate = null;
    private boolean stored = false;
    private boolean isThisSubquery = false;
    private boolean isDistinct = false;
    private Map<String, ReadQuery> sourceSubqueries = new HashMap<>();
    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();
    private String resultTableName = "";
    private WriteResultsPlan writeResultsPlan = null;
    private final List<Serializable> operations = new ArrayList<>();
    private List<Integer> resultSourceIndexes = new ArrayList<>();
    private List<Pair<String, Integer>> predicateVarToIndexes = new ArrayList<>();

    public FilterAndProjectionQuery setSourceName(String sourceName) {
        this.sourceName = sourceName;
        return this;
    }
    public FilterAndProjectionQuery setResultSchema(List<String> resultSchema) {
        this.resultSchema = resultSchema;
        return this;
    }
    public FilterAndProjectionQuery setSystemResultSchema(List<String> systemResultSchema) {
        this.systemResultSchema = systemResultSchema;
        return this;
    }
    public FilterAndProjectionQuery setFilterPredicate(String filterPredicate) {
        if(filterPredicate != null && !filterPredicate.isEmpty()){
            this.cachedFilterPredicate = MVEL.compileExpression(filterPredicate);
            this.filterPredicate = filterPredicate;
            return this;
        }
        this.filterPredicate = filterPredicate;
        return this;
    }
    public FilterAndProjectionQuery setIsThisSubquery(boolean isThisSubquery) {
        this.isThisSubquery = isThisSubquery;
        return this;
    }
    public FilterAndProjectionQuery setDistinct(){
        this.isDistinct = true;
        return this;
    }
    public FilterAndProjectionQuery setSourceSubqueries(Map<String, ReadQuery> sourceSubqueries){
        this.sourceSubqueries = sourceSubqueries;
        return this;
    }
    public FilterAndProjectionQuery setPredicateSubqueries(Map<String, ReadQuery> predicateSubqueries){
        this.predicateSubqueries = predicateSubqueries;
        return this;
    }
    public FilterAndProjectionQuery setStored(){
        this.stored = true;
        return this;
    }
    public FilterAndProjectionQuery setResultTableName(String resultTableName){
        this.resultTableName = resultTableName;
        Boolean[] keyStructure = new Boolean[resultSchema.size()];
        Arrays.fill(keyStructure, true);
        this.writeResultsPlan = new WriteResultsPlan(resultTableName, keyStructure);
        return this;
    }
    public FilterAndProjectionQuery setOperations(List<SerializablePredicate> operations) {
        this.operations.addAll(operations);
        return this;
    }
    public FilterAndProjectionQuery setResultSourceIndexes(List<Integer> resultSourceIndexes){
        this.resultSourceIndexes = resultSourceIndexes;
        return this;
    }
    public FilterAndProjectionQuery setPredicateVarToIndexes(List<Pair<String,Integer>> predicateVarToIndexes){
        this.predicateVarToIndexes = predicateVarToIndexes;
        return this;
    }

    public List<String> getSystemResultSchema() {
        return systemResultSchema;
    }
    public List<String> getPredicates(){
        return List.of(filterPredicate);
    }

    public boolean isStored(){
        return stored;
    }
    public boolean isThisSubquery() {
        return isThisSubquery;
    }
    public Map<String, ReadQuery> getSourceSubqueries() {
        return sourceSubqueries;
    }
    public Map<String, ReadQuery> getConcreteSubqueries(){return predicateSubqueries;}
    public String getResultTableName(){return resultTableName;}
    public WriteResultsPlan getWriteResultPlan() {
        return writeResultsPlan;
    }
    public boolean getDistinct(){return isDistinct;}
    public List<Serializable> getOperations() {return operations;}

    @Override
    public List<String> getTableNames() {
        if(sourceSubqueries.isEmpty())
            return List.of(sourceName);
        else
            return List.of();
    }
    @Override
    public Map<String, List<Integer>> keysForQuery() {
        if(sourceSubqueries.isEmpty())
            return Map.of(sourceName, List.of(-1));
        else
            return Map.of();
    }

    @Override
    public ByteString retrieve(RelShard shard, String tableName, Map<String, ReadQueryResults> concreteSubqueriesResults) {
        ArrayList<RelRow> retrievedData = new ArrayList<>(shard.getData(
                isDistinct,
                !(resultSourceIndexes == null || resultSourceIndexes.isEmpty()),
                resultSourceIndexes,
                cachedFilterPredicate,
                concreteSubqueriesResults,
                predicateVarToIndexes,
                operations
                ));
        return Utilities.objectToByteString(retrievedData);
    }

    @Override
    public RelReadQueryResults combine(Map<String, List<ByteString>> retrieveResults) {
        RelReadQueryResults results = new RelReadQueryResults();
        results.setFieldNames(resultSchema);
        List<ByteString> shardQueryResults = retrieveResults.get(sourceName);
        if(isThisSubquery){
            List<Map<Integer, Integer>> desIntermediateShardLocations = shardQueryResults.stream().map(v->(Map<Integer,Integer>)Utilities.byteStringToObject(v)).collect(Collectors.toList());
            List<Pair<Integer, Integer>> ret = new ArrayList<>();
            for(Map<Integer,Integer> intermediateShardsLocation: desIntermediateShardLocations){
                for(Map.Entry<Integer, Integer> shardLocation: intermediateShardsLocation.entrySet()){
                    ret.add(new Pair<>(shardLocation.getKey(), shardLocation.getValue()));
                }
            }
            results.setIntermediateLocations(ret);
        }else {
            ArrayList<RelRow> ret = new ArrayList<>();
            for(ByteString serSubset: shardQueryResults){
                ret.addAll((List<RelRow>)Utilities.byteStringToObject(serSubset));
            }
            if(isDistinct){
                results.addData(removeDuplicates(ret));
            }else{
                results.addData(ret);
            }
        }
        return results;
    }
    private ArrayList<RelRow> removeDuplicates(ArrayList<RelRow> data){
        ArrayList<RelRow> nonDuplicateRows = new ArrayList<>();
        for(int i = 0; i < data.size(); i++){
            List<RelRow> sublist = data.subList(i+1, data.size());
            if(!sublist.contains(data.get(i))){
                nonDuplicateRows.add(data.get(i));
            }
        }
        return nonDuplicateRows;
    }
}
