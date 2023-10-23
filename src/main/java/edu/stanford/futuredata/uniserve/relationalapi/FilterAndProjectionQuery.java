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

    //These queries have a single source. There is no need for the use of the dotted notation when specifying the
    //attributes the query operates on. The user may want to specify aliases, therefore the schema of the results may differ
    //from the schema of the source. In this context, the resulting schema is specified in 2 distinct forms
    //the system schema specifies the attributes to be operated on in terms of the source schema, while the
    //user schema specifies them in terms of aliases given by the user.
    //In case the user doesn't specify any aliases, the results will have the names of the source's attributes
    //In case the user doesn't specify an alias, the results will use the source attribute name for that particular
    //attribute.


    private String sourceName = "";  //tableName or subquery alias
    //private List<String> sourceSchema = new ArrayList<>();
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
    /*public FilterAndProjectionQuery setSourceSchema(List<String> sourceSchema) {
        this.sourceSchema = sourceSchema;
        return this;
    }*/
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
        //List<RelRow> shardData = shard.getData();

        //List<RelRow> filteredData; //= filter(shardData, concreteSubqueriesResults);
        //if(!(filterPredicate == null || filterPredicate.isEmpty() || cachedFilterPredicate == null || cachedFilterPredicate.equals(""))){
        //    filteredData = shard.getFilteredData(cachedFilterPredicate, concreteSubqueriesResults, predicateVarToIndexes);
        //}else {
        //    filteredData = shard.getData();
        //}

        //ArrayList<RelRow> retrievedData = project(filteredData);

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
    //@Override
    //public boolean writeIntermediateShard(RelShard intermediateShard, ByteString retrievedResults){
    //    List<RelRow> rows = (List<RelRow>) Utilities.byteStringToObject(retrievedResults);
    //    return intermediateShard.insertRows(rows) && intermediateShard.committRows();
    //}


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


    private List<RelRow> filter(List<RelRow> data, Map<String, ReadQueryResults> subqRes){
        if(filterPredicate.isEmpty()){
            return data;
        }
        Map<String, RelReadQueryResults> sRes = new HashMap<>();
        for(Map.Entry<String, ReadQueryResults>entry:subqRes.entrySet()){
            sRes.put(entry.getKey(), (RelReadQueryResults) entry.getValue());
        }
        List<RelRow> filteredData = new ArrayList<>();
        for(RelRow row: data){
            if(checkFilterPredicate(row,sRes)){
                filteredData.add(row);
            }
        }
        return filteredData;
    }
    private boolean checkFilterPredicate(RelRow row, Map<String, RelReadQueryResults> subqRes){
        Map<String, Object> values = new HashMap<>();
        for(Map.Entry<String, RelReadQueryResults> entry: subqRes.entrySet()){
            if(filterPredicate.contains(entry.getKey())){
                values.put(entry.getKey(), entry.getValue().getData().get(0).getField(0));
            }
        }
        /*
        for(String sourceAttributeName: sourceSchema){
            if(filterPredicate.contains(sourceAttributeName)){
                Object val = row.getField(sourceSchema.indexOf(sourceAttributeName));
                values.put(sourceAttributeName, val);
            }
        }
        for(String userAlias: resultSchema){
            if(filterPredicate.contains(userAlias)){
                Object val = row.getField(sourceSchema.indexOf(systemResultSchema.get(resultSchema.indexOf(userAlias))));
                values.put(userAlias, val);
            }
        }
        */
        for(Pair<String, Integer> nameToVar: predicateVarToIndexes){
            Object val = row.getField(nameToVar.getValue1());
            if(val == null){
                return false;
            }else{
                values.put(nameToVar.getValue0(), val);
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
            Object result = MVEL.executeExpression(cachedFilterPredicate, values);
            if(!(result instanceof Boolean))
                return false;
            else
                return (Boolean) result;

        }catch (Exception e ){
            System.out.println(e.getMessage());
            return false;
        }
    }
    private ArrayList<RelRow> project(List<RelRow> data){
        ArrayList<RelRow> projectionResults = new ArrayList<>();
        for(RelRow rawRow: data){
            /*
            List<Object> rawNewRow = new ArrayList<>(systemResultSchema.size());
            for(String systemAttributeName: systemResultSchema){
                rawNewRow.add(rawRow.getField(sourceSchema.indexOf(systemAttributeName)));
            }
            RelRow newRow = new RelRow(rawNewRow.toArray());
            if(operations.isEmpty()) {
                projectionResults.add(newRow);
            }else{
                projectionResults.add(applyOperations(newRow));
            }
            */
            List<Object> rawNewRow = new ArrayList<>(resultSourceIndexes.size());
            for(Integer index: resultSourceIndexes){
                rawNewRow.add(rawRow.getField(index));
            }
            RelRow newRow = new RelRow(rawNewRow.toArray());
            if(operations.isEmpty()) {
                projectionResults.add(newRow);
            }else{
                projectionResults.add(applyOperations(newRow));
            }
        }
        return projectionResults;
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
