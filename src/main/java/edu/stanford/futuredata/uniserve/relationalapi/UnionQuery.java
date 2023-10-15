package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.RetrieveAndCombineQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.SimpleWriteQueryPlan;
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

public class UnionQuery implements RetrieveAndCombineQueryPlan<RelShard, RelReadQueryResults> {
    private String sourceOne = ""; //either the name of the table or the alias of the subquery
    private String sourceTwo = "";
    private boolean sourceOneTable = true; //true if the source is a table
    private boolean sourceTwoTable = true;
    //private Map<String, List<String>> sourceSchemas = new HashMap<>(); //schemas of both sources
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
    //public UnionQuery setSourceSchemas(Map<String, List<String>> sourceSchemas) {
    //    this.sourceSchemas = sourceSchemas;
    //    return this;
    //}
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
    public UnionQuery setPredicateVarToIndexesOne(List<Pair<String, Integer>> predicateVarToIndexes) {
        this.predicateVarToIndexesOne = predicateVarToIndexes;
        return this;
    }
    public UnionQuery setPredicateVarToIndexesTwo(List<Pair<String, Integer>> predicateVarToIndexes) {
        this.predicateVarToIndexesTwo = predicateVarToIndexes;
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
    public ByteString retrieve(RelShard shard, String tableName, Map<String, ReadQueryResults> concreteSubqueriesResults) {
        List<RelRow> data = shard.getData();
        if(data.isEmpty()){
            return Utilities.objectToByteString(new ArrayList<>());
        }
        ArrayList<RelRow> filteredData = new ArrayList<>(filter(data, tableName, concreteSubqueriesResults));
        if(filteredData.isEmpty()){
            return Utilities.objectToByteString(new ArrayList<>());
        }
        ArrayList<RelRow> retrievedResults = checkDistinct(filteredData);
        if(retrievedResults.isEmpty()){
            return Utilities.objectToByteString(new ArrayList<>());
        }
        if(!operations.isEmpty()){
            ArrayList<RelRow> operationsRows = new ArrayList<>();
            for(RelRow retrievedRow:retrievedResults){
                operationsRows.add(applyOperations(retrievedRow));
            }
            return Utilities.objectToByteString(operationsRows);
        }else {
            return Utilities.objectToByteString(retrievedResults);
        }
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


    @Override
    public boolean writeIntermediateShard(RelShard intermediateShard, ByteString gatherResults){
        List<RelRow> rows = (List<RelRow>) Utilities.byteStringToObject(gatherResults);
        return intermediateShard.insertRows(rows) && intermediateShard.committRows();
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
    private List<RelRow> filter(List<RelRow> data, String sourceName, Map<String, ReadQueryResults> subqueriesResults){
        String predicate = filterPredicates.get(sourceName);
        if(predicate == null || predicate.isEmpty()){
            return data;
        }
        Map<String, RelReadQueryResults> relsubqRes = new HashMap<>();
        for(Map.Entry<String, ReadQueryResults> entry: subqueriesResults.entrySet()){
            relsubqRes.put(entry.getKey(), (RelReadQueryResults) entry.getValue());
        }
        ArrayList<RelRow> results = new ArrayList<>();
        for (RelRow row: data){
            if(evaluatePredicate(row, sourceName, relsubqRes)){
                results.add(row);
            }
        }
        return results;
    }
    private boolean evaluatePredicate(RelRow row, String sourceName, Map<String, RelReadQueryResults> subqueriesResults){
        Map<String, Object> values = new HashMap<>();
        //List<String> sourceSchema = sourceSchemas.get(sourceName);
        String filterPredicate = filterPredicates.get(sourceName);

        for(Map.Entry<String, RelReadQueryResults> subqRes: subqueriesResults.entrySet()){
            if(filterPredicate.contains(subqRes.getKey())){
                values.put(subqRes.getKey(), subqRes.getValue().getData().get(0).getField(0));
            }
        }
        /*
        for (String attributeName : sourceSchema) {
            Object val = row.getField(sourceSchema.indexOf(attributeName));
            String systemName = sourceName + "." + attributeName;
            if (filterPredicate.contains(systemName)) {
                values.put(systemName, val);
            }
            if (filterPredicate.contains(attributeName)) {
                values.put(attributeName, val);
            }
        }

         */


        if(sourceName.equals(sourceOne)) {
            for (Pair<String, Integer> nameToVar : predicateVarToIndexesOne) {
                Object val = row.getField(nameToVar.getValue1());
                if (val == null) {
                    return false;
                } else {
                    values.put(nameToVar.getValue0(), val);
                }
            }
        }else{
            for (Pair<String, Integer> nameToVar : predicateVarToIndexesTwo) {
                Object val = row.getField(nameToVar.getValue1());
                if (val == null) {
                    return false;
                } else {
                    values.put(nameToVar.getValue0(), val);
                }
            }
        }


        if(values.containsValue(null)){
            return false;
        }
        try{
            /*
            JexlEngine jexl = new JexlBuilder().create();
            JexlExpression expression = jexl.createExpression(filterPredicate);
            JexlContext context = new MapContext(values);
            Object result = expression.evaluate(context);
            if(!(result instanceof Boolean))
                return false;
            else {
                return (Boolean) result;
            }

             */

            //Serializable compiled = MVEL.compileExpression(filterPredicate);
            Serializable compiled = cachedFilterPredicates.get(sourceName);
            Object result = MVEL.executeExpression(compiled, values);
            if(!(result instanceof Boolean))
                return false;
            else
                return (Boolean) result;

        }catch (Exception e ){
            System.out.println(e.getMessage());
            return false;
        }
    }
    private RelRow applyOperations(RelRow inputRow){
        List<Object> newRow = new ArrayList<>();
        for(int i = 0; i<inputRow.getSize(); i++){
            newRow.add(applyOperation(inputRow.getField(i), operations.get(i)));
        }
        return new RelRow(newRow.toArray());
    }

    private Object applyOperation(Object o, Serializable pred){
        SerializablePredicate predicate = (SerializablePredicate) pred;
        return predicate.run(o);
    }
}
