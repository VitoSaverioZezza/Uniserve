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
    private Map<String, ReadQuery> sourceSubqueries = new HashMap<>();
    private String havingPredicate = "";
    private boolean stored = false;
    private boolean isThisSubquery = false;
    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();
    private String resultTableName = "";
    private WriteResultsPlan writeResultsPlan = null;
    private List<SerializablePredicate> operations = new ArrayList<>();


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
        this.operations = operations;
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
        List<RelRow> data = shard.getData();
        List<RelRow> filteredData = filter(data, concreteSubqueriesResults);
        Map<Integer, List<ByteString>> ret = new HashMap<>();
        for(RelRow row: filteredData){
            int key = 0;
            for(String groupAttribute: systemSelectedFields){
                key += row.getField(sourceSchema.indexOf(groupAttribute)).hashCode();
            }
            key = key % numRepartitions;
            ret.computeIfAbsent(key, k->new ArrayList<>()).add(Utilities.objectToByteString(row));
        }
        return ret;
    }
    @Override
    public ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, RelShard> ephemeralShards) {
        List<RelRow> rows = ephemeralData.get(sourceName).stream().map(v -> (RelRow) Utilities.byteStringToObject(v)).collect(Collectors.toList());
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
    public boolean writeIntermediateShard(RelShard intermediateShard, ByteString gatherResults){
        List<RelRow> rows = (List<RelRow>) Utilities.byteStringToObject(gatherResults);
        return intermediateShard.insertRows(rows) && intermediateShard.committRows();
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
            //List of Map<ShardID, dsID> to be converted in what is in the RelReadQueryResults
        }else {
            List<RelRow> ret = new ArrayList<>();
            for(ByteString serSubset: shardQueryResults){
                ret.addAll((List<RelRow>)Utilities.byteStringToObject(serSubset));
            }
            results.addData(ret);
            //List of RelRows lsis to be merged together in the structure
        }
        results.setFieldNames(resultSchema);
        return results;
    }

    private List<RelRow> filter(List<RelRow> data, Map<String, ReadQueryResults> subqRes ) {
        if(filterPredicate == null || filterPredicate.isEmpty()){
            return data;
        }
        ArrayList<RelRow> filteredData = new ArrayList<>();
        Map<String, RelReadQueryResults> sRes = new HashMap<>();
        for(Map.Entry<String, ReadQueryResults> entry: subqRes.entrySet()){
            sRes.put(entry.getKey(), (RelReadQueryResults) entry.getValue());
        }
        for (RelRow row: data){
            if(evaluatePredicate(row, sRes)){
                filteredData.add(row);
            }
        }
        return filteredData;
    }
    private boolean evaluatePredicate(RelRow row, Map<String, RelReadQueryResults> subqRes){
        Map<String, Object> values = new HashMap<>();

        for(Map.Entry<String, RelReadQueryResults> entry: subqRes.entrySet()){
            if(filterPredicate.contains(entry.getKey())){
                values.put(entry.getKey(), entry.getValue().getData().get(0).getField(0));
            }
        }

        for(String attributeName: sourceSchema){
            if(filterPredicate.contains(attributeName)){
                Object val = row.getField(sourceSchema.indexOf(attributeName));
                values.put(attributeName, val);
            }
        }

        JexlEngine jexl = new JexlBuilder().create();
        JexlExpression expression = jexl.createExpression(filterPredicate);
        JexlContext context = new MapContext(values);
        Object result = expression.evaluate(context);
        try{
            if(!(result instanceof Boolean))
                return false;
            else {
                return (Boolean) result;
            }
        }catch (Exception e ){
            System.out.println(e.getMessage());
            return false;
        }
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
        JexlEngine jexl = new JexlBuilder().create();
        JexlExpression expression = jexl.createExpression(predToTest);
        JexlContext context = new MapContext(values);
        Object result = expression.evaluate(context);
        try{
            if(!(result instanceof Boolean)) return false;
            else return (Boolean) result;
        }catch (Exception e ){
            System.out.println(e.getMessage());
            return false;
        }
    }
    private List<Object> computeAggregates(List<RelRow> groupRows) {
        List<Object> res = new ArrayList<>(aggregatesSpecification.size());
        for (Pair<Integer, String> aggregate : aggregatesSpecification) {
            String attributeName = aggregate.getValue1();
            if (Objects.equals(aggregate.getValue0(), RelReadQueryBuilder.AVG)) {
                int count = 0, sum = 0;
                for (RelRow row : groupRows) {
                    sum += (Integer)row.getField(sourceSchema.indexOf(attributeName));
                    count++;
                }
                res.add(sum/count);
            } else if (Objects.equals(aggregate.getValue0(), RelReadQueryBuilder.MIN)) {
                int min = Integer.MAX_VALUE;
                for(RelRow row: groupRows){
                    min = Integer.min(min, (Integer)row.getField(sourceSchema.indexOf(attributeName)));
                }
                res.add(min);
            } else if (Objects.equals(aggregate.getValue0(), RelReadQueryBuilder.MAX)) {
                int max = Integer.MIN_VALUE;
                for (RelRow row : groupRows) {
                    max = Integer.max(max, (Integer) row.getField(sourceSchema.indexOf(attributeName)));
                }
                res.add(max);
            } else if (Objects.equals(aggregate.getValue0(), RelReadQueryBuilder.COUNT)) {
                res.add(groupRows.size());
            }else if (Objects.equals(aggregate.getValue0(), RelReadQueryBuilder.SUM)) {
                int sum = 0;
                for(RelRow row: groupRows){
                    sum += (Integer)row.getField(sourceSchema.indexOf(attributeName));
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
        for(String groupAttribute: systemSelectedFields){
            ret.add(row.getField(sourceSchema.indexOf(groupAttribute)));
        }
        return ret;
    }
    private RelRow applyOperations(RelRow inputRow){
        List<Object> newRow = new ArrayList<>();
        for(int i = 0; i<inputRow.getSize(); i++){
            SerializablePredicate lambda = operations.get(i);
            newRow.add(lambda.run(inputRow.getField(i)));
        }
        return new RelRow(newRow.toArray());
    }

}
