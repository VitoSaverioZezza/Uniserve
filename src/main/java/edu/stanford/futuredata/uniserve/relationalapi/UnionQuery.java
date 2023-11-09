package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.javatuples.Pair;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class UnionQuery implements RetrieveAndCombineQueryPlan<RelShard, RelReadQueryResults> {
    private String sourceOne = ""; //either the name of the table or the alias of the subquery
    private String sourceTwo = "";
    private boolean sourceOneTable = true; //true if the source is a table
    private boolean sourceTwoTable = true;
    private List<String> resultSchema = new ArrayList<>(); //user final schema
    private List<String> systemResultSchema = new ArrayList<>(); //system final schema, this is in dotted notation!
    private Map<String, String> filterPredicates = new HashMap<>(); //filters for both sources
    private final Map<String, Serializable> cachedFilterPredicates = new HashMap<>();
    private Map<String, ReadQuery> sourceSubqueries = new HashMap<>(); //map from subquery alias to subquery
    private boolean stored = false;
    private boolean isThisSubquery = false;
    private boolean isDistinct = false; //still necessary, if one source is a subquery without equal rows
    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();
    private String resultTableName = "";
    private WriteResultsPlan writeResultsPlan = null;
    private final List<Serializable> operations = new ArrayList<>();
    private List<Pair<String, Integer>> predicateVarToIndexesOne = new ArrayList<>();
    private List<Pair<String, Integer>> predicateVarToIndexesTwo = new ArrayList<>();


    public UnionQuery setSourceOne(String sourceOne) {
        this.sourceOne = sourceOne;
        return this;
    }
    public UnionQuery setSourceTwo(String sourceTwo) {
        this.sourceTwo = sourceTwo;
        return this;
    }
    public UnionQuery setTableFlags(boolean isSourceOneTable, boolean isSourceTwoTable){
        sourceOneTable = isSourceOneTable;
        sourceTwoTable = isSourceTwoTable;
        return this;
    }
    public UnionQuery setResultSchema(List<String> resultSchema) {
        this.resultSchema = resultSchema;
        return this;
    }
    public UnionQuery setSystemResultSchema(List<String> systemResultSchema) {
        this.systemResultSchema = systemResultSchema;
        return this;
    }
    public UnionQuery setFilterPredicates(Map<String, String> filterPredicates) {
        for(Map.Entry<String,String> p: filterPredicates.entrySet()){
            if(p.getValue() != null && !p.getValue().isEmpty()){
                cachedFilterPredicates.put(p.getKey(), MVEL.compileExpression(p.getValue()));
                this.filterPredicates.put(p.getKey(), p.getValue());
            }
            return this;
        }
        this.filterPredicates = filterPredicates;
        return this;
    }
    public UnionQuery setSourceSubqueries(Map<String, ReadQuery> sourceSubqueries) {
        this.sourceSubqueries = sourceSubqueries;
        return this;
    }
    public UnionQuery setStored(){
        this.stored = true;
        return this;
    }
    public UnionQuery setIsThisSubquery(boolean isThisSubquery){
        this.isThisSubquery = isThisSubquery;
        return this;
    }
    public UnionQuery setDistinct(){
        this.isDistinct = true;
        return this;
    }
    public UnionQuery setPredicateSubqueries(Map<String, ReadQuery> predicateSubqueries){
        this.predicateSubqueries = predicateSubqueries;
        return this;
    }
    public UnionQuery setResultTableName(String resultTableName){
        this.resultTableName = resultTableName;
        Boolean[] keyStructure = new Boolean[resultSchema.size()];
        Arrays.fill(keyStructure, true);
        this.writeResultsPlan = new WriteResultsPlan(resultTableName, keyStructure);
        return this;
    }
    public UnionQuery setOperations(List<SerializablePredicate> operations) {
        this.operations.addAll(operations);
        return this;
    }

    public List<String> getPredicates(){
        return new ArrayList<>(filterPredicates.values());
    }
    public List<String> getSystemResultSchema() {
        return systemResultSchema;
    }
    public boolean isStored(){
        return stored;
    }


    public boolean isThisSubquery() {
        return isThisSubquery;
    }
    public Map<String, ReadQuery> getSourceSubqueries(){return sourceSubqueries;}
    public Map<String, ReadQuery> getConcreteSubqueries(){return predicateSubqueries;}
    public String getResultTableName(){return resultTableName;}
    public WriteResultsPlan getWriteResultPlan() {
        return writeResultsPlan;
    }
    public boolean getDistinct(){return isDistinct;}
    public List<Serializable> getOperations() {return operations;}

    @Override
    public List<String> getTableNames() {
        List<String> returned = new ArrayList<>();
        if(sourceTwoTable)
            returned.add(sourceTwo);
        if(sourceOneTable)
            returned.add(sourceOne);
        return returned;
    }
    @Override
    public Map<String, List<Integer>> keysForQuery() {
        Map<String, List<Integer>> returned = new HashMap<>();
        if(sourceTwoTable)
            returned.put(sourceTwo, List.of(-1));
        if(sourceOneTable)
            returned.put(sourceOne, List.of(-1));
        return returned;
    }

    @Override
    public ByteString retrieve(RelShard shard, String sourceName, Map<String, ReadQueryResults> concreteSubqueriesResults) {
        Serializable cachedFilterPredicate = cachedFilterPredicates.get(sourceName);
        List<Pair<String, Integer>> predicateVarToIndexes = null;
        if(sourceName.equals(sourceOne)){
            predicateVarToIndexes = predicateVarToIndexesOne;
        } else if (sourceName.equals(sourceTwo)) {
            predicateVarToIndexes = predicateVarToIndexesTwo;
        }
        ArrayList<RelRow> retrievedResults = new ArrayList<>(
                shard.getData(
                        isDistinct, false, null,
                        cachedFilterPredicate, concreteSubqueriesResults,
                        predicateVarToIndexes, operations
                )
        );
        return Utilities.objectToByteString(retrievedResults);
    }

    @Override
    public RelReadQueryResults combine(Map<String, List<ByteString>> retrieveResults) {
        RelReadQueryResults results = new RelReadQueryResults();
        results.setFieldNames(resultSchema);
        List<ByteString> shardQueryResultsOne = retrieveResults.get(sourceOne);
        List<ByteString> shardQueryResultsTwo = retrieveResults.get(sourceTwo);
        List<ByteString> shardQueryResults = new ArrayList<>();
        if(shardQueryResultsOne != null){
            shardQueryResults.addAll(shardQueryResultsOne);
        }
        if(shardQueryResultsTwo != null){
            shardQueryResults.addAll(shardQueryResultsTwo);
        }
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
            ArrayList<RelRow> retrievedRows = new ArrayList<>();
            for (List<ByteString> retrievedList : retrieveResults.values()) {
                for (ByteString bs : retrievedList) {
                    List<RelRow> desPartialRes = (ArrayList<RelRow>) Utilities.byteStringToObject(bs);
                    retrievedRows.addAll(desPartialRes);
                }
            }
            if(isDistinct){
                results.addData(checkDistinct(retrievedRows));
            }else{
                results.addData(retrievedRows);
            }
        }
        return results;
    }
    private ArrayList<RelRow> checkDistinct(ArrayList<RelRow> data){
        if(!isDistinct){
            return data;
        }
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
