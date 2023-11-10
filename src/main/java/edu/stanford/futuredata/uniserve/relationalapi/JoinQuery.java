package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.apache.commons.jexl3.*;
import org.javatuples.Pair;
import org.mvel2.MVEL;
import org.mvel2.compiler.CompiledExpression;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static edu.stanford.futuredata.uniserve.relationalapi.ReadQuery.logger;

public class JoinQuery implements ShuffleOnReadQueryPlan<RelShard, RelReadQueryResults> {
    private String sourceOne = ""; //either the name of the table or the alias of the subquery
    private String sourceTwo = "";
    private boolean sourceOneTable = true; //true if the source is a table
    private boolean sourceTwoTable = true;
    private Map<String, List<String>> sourceSchemas = new HashMap<>(); //schemas of both sources
    private List<String> resultSchema = new ArrayList<>(); //user final schema
    private List<String> systemResultSchema = new ArrayList<>(); //system final schema, this is in dotted notation!
    private Map<String, List<String>> sourcesJoinAttributes = new HashMap<>();
    private Map<String, String> filterPredicates = new HashMap<>(); //filters for both sources
    private final  Map<String, Serializable> cachedFilterPredicates = new HashMap<>();
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
    private Serializable compiledHavingPredicate = null;
    private String havingPredicate = "";

    public JoinQuery setSourceOne(String sourceOne) {
        this.sourceOne = sourceOne;
        return this;
    }
    public JoinQuery setSourceTwo(String sourceTwo) {
        this.sourceTwo = sourceTwo;
        return this;
    }
    public JoinQuery setTableFlags(boolean isSourceOneTable, boolean isSourceTwoTable){
        sourceOneTable = isSourceOneTable;
        sourceTwoTable = isSourceTwoTable;
        return this;
    }
    public JoinQuery setSourceSchemas(Map<String, List<String>> sourceSchemas) {
        this.sourceSchemas = sourceSchemas;
        return this;
    }
    public JoinQuery setResultSchema(List<String> resultSchema) {
        this.resultSchema = resultSchema;
        return this;
    }
    public JoinQuery setSystemResultSchema(List<String> systemResultSchema) {
        this.systemResultSchema = systemResultSchema;
        return this;
    }
    public JoinQuery setSourcesJoinAttributes(Map<String, List<String>> sourcesJoinAttributes) {
        this.sourcesJoinAttributes = sourcesJoinAttributes;
        return this;
    }
    public JoinQuery setFilterPredicates(Map<String, String> filterPredicates) {
        for(Map.Entry<String,String> p: filterPredicates.entrySet()){
            if(p.getValue() != null && !p.getValue().isEmpty()){
                try {
                    cachedFilterPredicates.put(p.getKey(), MVEL.compileExpression(p.getValue()));
                    this.filterPredicates.put(p.getKey(), p.getValue());
                }catch (Exception e){
                    throw new RuntimeException("Impossible to compile predicate");
                }
            }
            return this;
        }
        this.filterPredicates = filterPredicates;
        return this;
    }
    public JoinQuery setSourceSubqueries(Map<String, ReadQuery> sourceSubqueries) {
        this.sourceSubqueries = sourceSubqueries;
        return this;
    }
    public JoinQuery setStored(){
        this.stored = true;
        return this;
    }
    public JoinQuery setIsThisSubquery(boolean isThisSubquery){
        this.isThisSubquery = isThisSubquery;
        return this;
    }
    public JoinQuery setDistinct(){
        this.isDistinct = true;
        return this;
    }
    public JoinQuery setPredicateSubqueries(Map<String, ReadQuery> predicateSubqueries){
        this.predicateSubqueries = predicateSubqueries;
        return this;
    }
    public JoinQuery setResultTableName(String resultTableName){
        this.resultTableName = resultTableName;
        Boolean[] keyStructure = new Boolean[resultSchema.size()];
        Arrays.fill(keyStructure, true);
        this.writeResultsPlan = new WriteResultsPlan(resultTableName, keyStructure);
        return this;
    }
    public JoinQuery setOperations(List<SerializablePredicate> operations) {
        this.operations.addAll(operations);
        return this;
    }
    public JoinQuery setPredicateVarToIndexesOne(List<Pair<String, Integer>> predicateVarToIndexes) {
        this.predicateVarToIndexesOne = predicateVarToIndexes;
        return this;
    }
    public JoinQuery setPredicateVarToIndexesTwo(List<Pair<String, Integer>> predicateVarToIndexes) {
        this.predicateVarToIndexesTwo = predicateVarToIndexes;
        return this;
    }
    public JoinQuery setHavingPredicate(String havingPredicate){
        this.havingPredicate = havingPredicate;
        try{
            compiledHavingPredicate = MVEL.compileExpression(havingPredicate);
        }catch (Exception e){
            ReadQuery.logger.warn("Impossible to compile predicate on result rows, no row will be selected");
            compiledHavingPredicate = null;
            havingPredicate = "";
            return this;
        }
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
    public Map<String, ReadQuery> getVolatileSubqueries(){return sourceSubqueries;}
    public Map<String, ReadQuery> getConcreteSubqueries(){return predicateSubqueries;}
    public String getResultTableName(){return resultTableName;}
    public WriteResultsPlan getWriteResultPlan() {
        return writeResultsPlan;
    }
    public boolean getDistinct(){return isDistinct;}
    public List<Serializable> getOperations() {return operations;}
    public List<String> getJoinArgsSrc1(){return sourcesJoinAttributes.get(sourceOne);}
    public List<String> getJoinArgsSrc2(){return sourcesJoinAttributes.get(sourceTwo);}

    @Override
    public List<String> getQueriedTables() {
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
    public Map<Integer, List<ByteString>> scatter(RelShard shard, int numRepartitions, String sourceName, Map<String, ReadQueryResults> concreteSubqueriesResults) {
        Serializable cachedFilterPredicate = cachedFilterPredicates.get(sourceName);
        List<Pair<String, Integer>> predicateVarToIndexes = null;
        Map<Integer, List<ByteString>> returnedAssignment = new HashMap<>();
        List<String> joinAttributes = sourcesJoinAttributes.get(sourceName);
        List<String> sourceSchema = sourceSchemas.get(sourceName);
        if(sourceName.equals(sourceOne)){
            predicateVarToIndexes = predicateVarToIndexesOne;
        } else if (sourceName.equals(sourceTwo)) {
            predicateVarToIndexes = predicateVarToIndexesTwo;
        }

        List<Integer> joinAttributesIndexes = new ArrayList<>();
        for(String joinAttribute: joinAttributes){
            int index = sourceSchema.indexOf(joinAttribute);
            assert (index >= 0);
            joinAttributesIndexes.add(sourceSchema.indexOf(joinAttribute));
        }

        returnedAssignment = shard.getGroups(
                cachedFilterPredicate, concreteSubqueriesResults, predicateVarToIndexes, joinAttributesIndexes, numRepartitions
        );
        return returnedAssignment;
    }
    @Override
    public ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, RelShard> ephemeralShards) {
    //public List<ByteString> gather(Map<String, List<ByteString>> ephemeralData, Map<String, RelShard> ephemeralShards) {
        List<RelRow> rowsSourceOne = ephemeralShards.get(sourceOne).getData();
        List<RelRow> rowsSourceTwo = ephemeralShards.get(sourceTwo).getData();
        if(rowsSourceOne == null || rowsSourceOne.isEmpty() || rowsSourceTwo == null || rowsSourceTwo.isEmpty())
            return  Utilities.objectToByteString(new ArrayList<>());
        List<String> schemaSourceOne = sourceSchemas.get(sourceOne);
        List<String> schemaSourceTwo = sourceSchemas.get(sourceTwo);
        List<String> joinAttributesOne = sourcesJoinAttributes.get(sourceOne);
        List<String> joinAttributesTwo = sourcesJoinAttributes.get(sourceTwo);
        List<RelRow> joinedRows = ephemeralShards.get(sourceOne).join(
                ephemeralShards.get(sourceTwo).getData(),
                schemaSourceOne, schemaSourceTwo, joinAttributesOne, joinAttributesTwo,
                systemResultSchema, sourceOne, operations, isDistinct,
                resultSchemaSystemIndexes
                );
        //return joinedRows.stream().map(Utilities::objectToByteString).collect(Collectors.toList());
        return Utilities.objectToByteString(new ArrayList<>(joinedRows));
    }

    private Integer[] resultSchemaSystemIndexes;
    public JoinQuery setResultSchemaIndexes(Integer[] resultSchemaSystemIndexes){
        this.resultSchemaSystemIndexes = resultSchemaSystemIndexes;
        return this;
    }


    @Override
    public RelReadQueryResults combine(List<ByteString> shardQueryResults) {
        RelReadQueryResults results = new RelReadQueryResults();
        results.setFieldNames(resultSchema);
        if(isThisSubquery){
            List<Map<Integer, Integer>> desIntermediateShardLocations = shardQueryResults.stream().map(v->(Map<Integer,Integer>)Utilities.byteStringToObject(v)).collect(Collectors.toList());
            List<Pair<Integer, Integer>> ret = new ArrayList<>();
            for(Map<Integer,Integer> intermediateShardsLocation: desIntermediateShardLocations){
                for(Map.Entry<Integer, Integer> shardLocation: intermediateShardsLocation.entrySet()){
                    ret.add(new Pair<>(shardLocation.getKey(), shardLocation.getValue()));
                }
            }
            results.setIntermediateLocations(ret);
        }else{
            List<RelRow> ret = new ArrayList<>();
            for(ByteString serSubset: shardQueryResults){
                ret.addAll((List<RelRow>)Utilities.byteStringToObject(serSubset));
            }
            if(isDistinct){
                results.addData(checkDistinct(new ArrayList<>(ret)));
            }else {
                results.addData(ret);
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
