package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.RelReadQueryBuilder;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.apache.commons.jexl3.*;
import org.javatuples.Pair;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class AggregateQuery implements ShuffleOnReadQueryPlan<RelShard, RelReadQueryResults> {
    private String sourceName = "";
    private boolean sourceIsTable = true;
    private List<String> sourceSchema = new ArrayList<>();
    private List<String> resultSchema = new ArrayList<>();
    private List<String> systemSelectedFields = new ArrayList<>();
    private List<Pair<Integer, String>> aggregatesSpecification = new ArrayList<>();
    private String filterPredicate = "";
    private Serializable cachedFilterPredicate = null;
    private Map<String, ReadQuery> sourceSubqueries = new HashMap<>();
    private String havingPredicate = "";
    private boolean stored = false;
    private boolean isThisSubquery = false;
    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();
    private String resultTableName = "";
    private WriteResultsPlan writeResultsPlan = null;
    private final List<Serializable> operations = new ArrayList<>();
    private List<Pair<Integer, Integer>> aggregatesOPToIndex = new ArrayList<>();
    private List<Pair<String, Integer>> predicateVarToIndexes = new ArrayList<>();
    private List<Integer> groupAttributesIndexes = new ArrayList<>();


    public AggregateQuery setSourceName(String sourceName) {
        this.sourceName = sourceName;
        return this;
    }
    public AggregateQuery setSourceIsTable(boolean sourceIsTable) {
        this.sourceIsTable = sourceIsTable;
        return this;
    }
    public AggregateQuery setSourceSchema(List<String> sourceSchema) {
        this.sourceSchema = sourceSchema;
        return this;
    }
    public AggregateQuery setResultSchema(List<String> resultSchema) {
        this.resultSchema = resultSchema;
        return this;
    }
    public AggregateQuery setSystemSelectedFields(List<String> systemSelectedFields) {
        this.systemSelectedFields = systemSelectedFields;
        return this;
    }
    public AggregateQuery setAggregatesSpecification(List<Pair<Integer, String>> aggregatesSpecification) {
        this.aggregatesSpecification = aggregatesSpecification;
        return this;
    }
    public AggregateQuery setFilterPredicate(String filterPredicate) {
        if(filterPredicate != null && !filterPredicate.isEmpty()){
            this.cachedFilterPredicate = MVEL.compileExpression(filterPredicate);
            this.filterPredicate = filterPredicate;
            return this;
        }
        this.filterPredicate = filterPredicate;
        return this;
    }
    public AggregateQuery setSourceSubqueries(Map<String, ReadQuery> sourceSubqueries) {
        this.sourceSubqueries = sourceSubqueries;
        return this;
    }
    public AggregateQuery setHavingPredicate(String havingPredicate) {
        this.havingPredicate = havingPredicate;
        return this;
    }
    public AggregateQuery setStored(){
        this.stored = true;
        return this;
    }
    public AggregateQuery setIsThisSubquery(boolean isThisSubquery){
        this.isThisSubquery = isThisSubquery;
        return this;
    }
    public AggregateQuery setPredicateSubqueries(Map<String, ReadQuery> predicateSubqueries){
        this.predicateSubqueries = predicateSubqueries;
        return this;
    }
    public AggregateQuery setResultTableName(String resultTableName){
        this.resultTableName = resultTableName;
        Boolean[] keyStructure = new Boolean[resultSchema.size()];
        Arrays.fill(keyStructure, 0, aggregatesSpecification.size(), true);
        Arrays.fill(keyStructure, aggregatesSpecification.size(), keyStructure.length, false);
        this.writeResultsPlan = new WriteResultsPlan(resultTableName, keyStructure);
        return this;
    }
    public AggregateQuery setOperations(List<SerializablePredicate> operations) {
        this.operations.addAll(operations);
        return this;
    }
    public AggregateQuery setAggregatesOPToIndex(List<Pair<Integer, Integer>> aggregatesOPToIndex){
        this.aggregatesOPToIndex = aggregatesOPToIndex;
        return this;
    }
    public AggregateQuery setPredicateVarToIndexes(List<Pair<String, Integer>> predicateVarToIndexes) {
        this.predicateVarToIndexes = predicateVarToIndexes;
        return this;
    }
    public AggregateQuery setGroupAttributesIndexes(List<Integer> groupAttributesIndexes){
        this.groupAttributesIndexes =groupAttributesIndexes;
        return this;
    }

    public List<Pair<Integer, String>> getAggregatesSpecification(){
        return this.aggregatesSpecification;
    }
    public List<String> getSystemSelectedFields() {
        return systemSelectedFields;
    }
    public List<String> getPredicates(){
        return List.of(filterPredicate, havingPredicate);
    }
    public boolean isStored(){
        return stored;
    }
    public boolean isThisSubquery() {
        return isThisSubquery;
    }
    public Map<String, ReadQuery> getVolatileSubqueries() {
        return sourceSubqueries;
    }
    public Map<String, ReadQuery> getConcreteSubqueries(){return predicateSubqueries;}
    public String getResultTableName(){return resultTableName;}
    public WriteResultsPlan getWriteResultPlan() {
        return writeResultsPlan;
    }


    @Override
    public List<String> getQueriedTables() {
        if(sourceIsTable){
            return List.of(sourceName);
        }else{
            return List.of();
        }
    }
    @Override
    public Map<String, List<Integer>> keysForQuery() {
        if(sourceIsTable){
            return Map.of(sourceName,List.of(-1));
        }else{
            return Map.of();
        }
    }

    @Override
    public Map<Integer, List<ByteString>> scatter(RelShard shard, int numRepartitions, String tableName, Map<String, ReadQueryResults> concreteSubqueriesResults) {
        return shard.getGroups(
                cachedFilterPredicate,
                concreteSubqueriesResults,
                predicateVarToIndexes,
                groupAttributesIndexes,
                numRepartitions
        );
    }
    @Override
    public ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, RelShard> ephemeralShards) {
        List<RelRow> rows = ephemeralShards.get(sourceName).getData();
        Map<List<Object>, List<RelRow>> groups = new HashMap<>();
        for(RelRow row: rows){
            List<Object> rowGroupFields = getGroup(row);
            groups.computeIfAbsent(rowGroupFields, k->new ArrayList<>()).add(row);
        }
        ArrayList<RelRow> resultRows = new ArrayList<>();
        for(Map.Entry<List<Object>, List<RelRow>> group: groups.entrySet()){
            List<Object> result = group.getKey(); //first part of the result rows contains group attributes
            List<Object> aggregatedAttributeRes = computeAggregates(group.getValue()); //second part of the row containing aggregates
            result.addAll(aggregatedAttributeRes);
            if(checkHavingPredicate(result)) {
                if(operations.isEmpty()) {
                    resultRows.add(new RelRow(result.toArray()));
                }else{
                    resultRows.add(applyOperations(new RelRow(result.toArray())));
                }
            }
        }
        return Utilities.objectToByteString(resultRows);
    }
    @Override
    public RelReadQueryResults combine(List<ByteString> shardQueryResults) {
        RelReadQueryResults results = new RelReadQueryResults();
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
            List<RelRow> ret = new ArrayList<>();
            for(ByteString serSubset: shardQueryResults){
                ret.addAll((List<RelRow>)Utilities.byteStringToObject(serSubset));
            }
            results.addData(ret);
        }
        results.setFieldNames(resultSchema);
        return results;
    }
    private boolean checkHavingPredicate(List<Object> row){
        if(havingPredicate == null || havingPredicate.isEmpty()){
            return true;
        }
        String predToTest = new String(havingPredicate);
        Map<String, Object> values = new HashMap<>();
        for(String fieldAlias: resultSchema){
            if(havingPredicate.contains(fieldAlias)){
                values.put(fieldAlias, row.get(resultSchema.indexOf(fieldAlias)));
            }
        }
        for(String systemName: sourceSchema){
            if(havingPredicate.contains(systemName)){
                values.put(systemName, row.get(systemSelectedFields.indexOf(systemName))); //system names can be used only for the group attributes
            }
        }
        if(values.containsValue(null)){
            return false;
        }
        try{
            JexlEngine jexl = new JexlBuilder().create();
            JexlExpression expression = jexl.createExpression(predToTest);
            JexlContext context = new MapContext(values);
            Object result = expression.evaluate(context);
            if(!(result instanceof Boolean)) return false;
            else return (Boolean) result;
        }catch (Exception e ){
            System.out.println(e.getMessage());
            return false;
        }
    }
    private List<Object> computeAggregates(List<RelRow> groupRows) {
        List<Object> res = new ArrayList<>(aggregatesOPToIndex.size());
        for (Pair<Integer, Integer> aggregate : aggregatesOPToIndex) {
            Integer index = aggregate.getValue1();
            if (Objects.equals(aggregate.getValue0(), RelReadQueryBuilder.AVG)) {
                Double count = 0D;
                Double sum = 0D;
                for (RelRow row : groupRows) {
                    Object val = row.getField(index);
                    if(val != null){
                        sum += (Double) ((Number)row.getField(index)).doubleValue();
                        count++;
                    }
                }
                if(count != 0) {
                    res.add(sum / count);
                }else{
                    res.add(null);
                }
            } else if (Objects.equals(aggregate.getValue0(), RelReadQueryBuilder.MIN)) {
                Double min = Double.MAX_VALUE;
                for(RelRow row: groupRows){
                    Object val = row.getField(index);
                    if(val != null){
                        min = Double.min(min, ((Number)row.getField(index)).doubleValue());
                    }
                }
                res.add(min);
            } else if (Objects.equals(aggregate.getValue0(), RelReadQueryBuilder.MAX)) {
                Double max = Double.MIN_VALUE;
                for (RelRow row : groupRows) {
                    Object val = row.getField(index);
                    if(val != null){
                        max = Double.max(max, ((Number) row.getField(index)).doubleValue());
                    }
                }
                res.add(max);
            } else if (Objects.equals(aggregate.getValue0(), RelReadQueryBuilder.COUNT)) {
                Double count = 0D;
                for(RelRow row: groupRows){
                    Object val = row.getField(index);
                    if(val != null){
                        count++;
                    }
                }
                res.add(count);
            }else if (Objects.equals(aggregate.getValue0(), RelReadQueryBuilder.SUM)) {
                Double sum = 0D;
                for(RelRow row: groupRows){
                    Object val = row.getField(index);
                    if(val != null){
                        sum += ((Number) row.getField(index)).doubleValue();
                    }
                }
                res.add(sum);
            }else{
                throw new RuntimeException("AggregateError, undefined operator");
            }
        }
        return res;
    }
    private List<Object> getGroup(RelRow row){
        List<Object> ret = new ArrayList<>();
        for(Integer groupAttributeIndex: groupAttributesIndexes){
            ret.add(row.getField(groupAttributeIndex));
        }
        return ret;
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
