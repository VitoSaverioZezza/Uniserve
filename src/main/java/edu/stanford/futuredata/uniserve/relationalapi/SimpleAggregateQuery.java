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

public class SimpleAggregateQuery implements ShuffleOnReadQueryPlan<RelShard, RelReadQueryResults> {

    //these queries operate on a single source, therefore there is no need for the use of the dotted notation
    //there are no attributes selected for projection, the only columns are the results of the aggregate operations
    //therefore the result schema is the list of the attribute aliases provided by the user

    private String sourceName = "";
    private boolean sourceIsTable = true;
    private List<String> resultsSchema = new ArrayList<>();
    private List<Pair<Integer, String>> aggregatesSpecification = new ArrayList<>();
    private String filterPredicate = "";
    private Serializable cachedFilterPredicate = null;
    private Map<String, ReadQuery> sourceSubqueries = new HashMap<>();
    private boolean stored;
    private boolean isThisSubquery = false;
    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();
    private String resultTableName = "";
    private WriteResultsPlan writeResultsPlan = null;
    private final List<Serializable> operations = new ArrayList<>();
    private List<Pair<String, Integer>> predicateVarToIndexes = new ArrayList<>();
    private List<Pair<Integer, Integer>> aggregatesOPsToIndexes = new ArrayList<>();

    public SimpleAggregateQuery setSourceName(String sourceName) {
        this.sourceName = sourceName;
        return this;
    }
    public SimpleAggregateQuery setSourceIsTable(boolean sourceIsTable){
        this.sourceIsTable = sourceIsTable;
        return this;
    }
    public SimpleAggregateQuery setResultsSchema(List<String> resultsSchema) {
        this.resultsSchema = resultsSchema;
        return this;
    }
    public SimpleAggregateQuery setAggregatesSpecification(List<Pair<Integer, String>> aggregatesSpecification) {
        this.aggregatesSpecification = aggregatesSpecification;
        return this;
    }
    public SimpleAggregateQuery setFilterPredicate(String filterPredicate) {
        if(filterPredicate != null && !filterPredicate.isEmpty()){
            this.cachedFilterPredicate = MVEL.compileExpression(filterPredicate);
            this.filterPredicate = filterPredicate;
            return this;
        }
        this.filterPredicate = filterPredicate;
        return this;
    }
    public SimpleAggregateQuery setSourceSubqueries(Map<String, ReadQuery> sourceSubqueries){
        this.sourceSubqueries = sourceSubqueries;
        return this;
    }
    public SimpleAggregateQuery setStored(){
        this.stored = true;
        return this;
    }
    public SimpleAggregateQuery setIsThisSubquery(boolean isThisSubquery) {
        this.isThisSubquery = isThisSubquery;
        return this;
    }
    public SimpleAggregateQuery setPredicateSubqueries(Map<String, ReadQuery> predicateSubqueries){
        this.predicateSubqueries = predicateSubqueries;
        return this;
    }
    public SimpleAggregateQuery setResultTableName(String resultTableName){
        this.resultTableName = resultTableName;
        Boolean[] keyStructure = new Boolean[resultsSchema.size()];
        Arrays.fill(keyStructure, true);
        this.writeResultsPlan = new WriteResultsPlan(resultTableName, keyStructure);
        return this;
    }
    public SimpleAggregateQuery setOperations(List<SerializablePredicate> operations) {
        this.operations.addAll(operations);
        return this;
    }
    public SimpleAggregateQuery setPredicateVarToIndexes(List<Pair<String, Integer>> predicateVarToIndexes) {
        this.predicateVarToIndexes = predicateVarToIndexes;
        return this;
    }
    public SimpleAggregateQuery setAggregatesOPsToIndexes(List<Pair<Integer, Integer>> aggregatesOPsToIndexes){
        this.aggregatesOPsToIndexes = aggregatesOPsToIndexes;
        return this;
    }

    public List<Pair<Integer, String>> getAggregatesSpecification() {
        return aggregatesSpecification;
    }
    public List<String> getPredicates(){
        return List.of(filterPredicate);
    }
    public boolean isStored(){
        return stored;
    }
    public boolean isThisSubquery(){
        return isThisSubquery;
    }
    public Map<String, ReadQuery> getVolatileSubqueries(){return sourceSubqueries;}
    public Map<String, ReadQuery> getConcreteSubqueries(){
        return predicateSubqueries;
    }
    public String getResultTableName(){return resultTableName;}
    public WriteResultsPlan getWriteResultPlan() {
        return writeResultsPlan;
    }
    public List<Serializable> getOperations() {return operations;}

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
            return Map.of(sourceName, List.of(-1));
        }else{
            return Map.of();
        }
    }


    @Override
    public Map<Integer, List<ByteString>> scatter(RelShard shard, int numRepartitions, String tableName, Map<String, ReadQueryResults> concreteSubqueriesResults) {
        Integer aggregatingDSID = sourceName == null ? 0 : sourceName.hashCode() % numRepartitions;
        if(aggregatingDSID < 0){
            aggregatingDSID = aggregatingDSID*-1;
        }
        RelRow partialResultsRow = shard.getAggregate(
                cachedFilterPredicate, concreteSubqueriesResults, predicateVarToIndexes, aggregatesOPsToIndexes
        );
        Map<Integer, List<ByteString>> ret = new HashMap<>();
        List<ByteString> serializedRow = new ArrayList<>();

        serializedRow.add(Utilities.objectToByteString(partialResultsRow));
        ret.put(aggregatingDSID, serializedRow);
        return ret;
    }
    @Override
    public ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, RelShard> ephemeralShards) {
    //public List<ByteString> gather(Map<String, List<ByteString>> ephemeralData, Map<String, RelShard> ephemeralShards) {
        List<RelRow> partialResults = ephemeralShards.get(sourceName).getData();
        if(partialResults == null || partialResults.isEmpty()){
            return Utilities.objectToByteString(new ArrayList<>());
            //return new ArrayList<>();
        }
        ArrayList<RelRow> results = new ArrayList<>();
        results.add(computeResults(partialResults));
        return Utilities.objectToByteString(results);
        //return results.stream().map(Utilities::objectToByteString).collect(Collectors.toList());

    }
    @Override
    public RelReadQueryResults combine(List<ByteString> shardQueryResults) {
        RelReadQueryResults results = new RelReadQueryResults();
        results.setFieldNames(resultsSchema);
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
        return results;
    }
    private RelRow computeResults(List<RelRow> partialResults){
        List<Object> resultRow = new ArrayList<>();
        for(int i = 0; i<aggregatesOPsToIndexes.size(); i++){
            Integer aggregateCode = aggregatesOPsToIndexes.get(i).getValue0();
            if(aggregateCode.equals(RelReadQueryBuilder.AVG)){
                Double count = 0D;
                Double sum = 0D;
                for(RelRow row: partialResults){
                    Object val = row.getField(i);
                    if(val == null){
                        count += 1D;
                        sum += 0D;
                    }else {
                        Double[] countSum = (Double[]) row.getField(i);
                        count += countSum[0];
                        sum += countSum[1];
                    }
                }
                if(count != 0D) {
                    resultRow.add(sum / count);
                }else{
                    resultRow.add(count);

                }
            } else if (aggregateCode.equals(RelReadQueryBuilder.MIN)) {
                Double min = Double.MAX_VALUE;
                for (RelRow row: partialResults) {
                    Object val = row.getField(i);
                    if (val != null) {
                        min = Double.min(min, ((Number) val).doubleValue());
                    }
                }
                resultRow.add(min);
            } else if (aggregateCode.equals(RelReadQueryBuilder.MAX)) {
                Double max = Double.MIN_VALUE;
                for (RelRow row: partialResults){
                    Object val = row.getField(i);
                    if(val != null) {
                        max = Double.max(max, ((Number) val).doubleValue());
                    }
                }
                resultRow.add(max);
            } else if (aggregateCode.equals(RelReadQueryBuilder.COUNT)) {
                Double count = 0D;
                for(RelRow row: partialResults){
                    Object val = row.getField(i);
                    if(val !=null) {
                        count += ((Number) val).doubleValue();
                    }
                }
                resultRow.add(count);
            } else if (aggregateCode.equals(RelReadQueryBuilder.SUM)) {
                Double sum = 0D;
                for (RelRow row: partialResults){
                    Object val = row.getField(i);
                    if(val !=null) {
                        sum += ((Number) val).doubleValue();
                    }
                }
                resultRow.add(sum);
            }
        }
        if(operations.isEmpty()) {
            return new RelRow(resultRow.toArray());
        }else{
            return applyOperations(new RelRow(resultRow.toArray()));
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
