package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import edu.stanford.futuredata.uniserve.interfaces.ReadQueryResults;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.ReadQueryBuilder;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.apache.commons.jexl3.*;
import org.javatuples.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class AnotherSimpleAggregateQuery implements ShuffleOnReadQueryPlan<RelShard, RelReadQueryResults> {

    //these queries operate on a single source, therefore there is no need for the use of the dotted notation
    //there are no attributes selected for projection, the only colums are the results of the aggregate operations
    //therefore the result schema is the list of the attribute aliases provided by the user


    private String sourceName = "";
    private boolean sourceIsTable = true;
    private List<String> sourceSchema = new ArrayList<>();
    private List<String> resultsSchema = new ArrayList<>();
    private List<Pair<Integer, String>> aggregatesSpecification = new ArrayList<>();
    private String filterPredicate = "";
    private Map<String, ReadQuery> sourceSubqueries = new HashMap<>();
    private boolean stored;
    private boolean isThisSubquery = false;
    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();
    private String resultTableName = "";

    public AnotherSimpleAggregateQuery setSourceName(String sourceName) {
        this.sourceName = sourceName;
        return this;
    }
    public AnotherSimpleAggregateQuery setSourceIsTable(boolean sourceIsTable){
        this.sourceIsTable = sourceIsTable;
        return this;
    }
    public AnotherSimpleAggregateQuery setSourceSchema(List<String> sourceSchema) {
        this.sourceSchema = sourceSchema;
        return this;
    }
    public AnotherSimpleAggregateQuery setResultsSchema(List<String> resultsSchema) {
        this.resultsSchema = resultsSchema;
        return this;
    }
    public AnotherSimpleAggregateQuery setAggregatesSpecification(List<Pair<Integer, String>> aggregatesSpecification) {
        this.aggregatesSpecification = aggregatesSpecification;
        return this;
    }
    public AnotherSimpleAggregateQuery setFilterPredicate(String filterPredicate) {
        this.filterPredicate = filterPredicate;
        return this;
    }
    public AnotherSimpleAggregateQuery setSourceSubqueries(Map<String, ReadQuery> sourceSubqueries){
        this.sourceSubqueries = sourceSubqueries;
        return this;
    }
    public AnotherSimpleAggregateQuery setStored(){
        this.stored = true;
        return this;
    }
    public AnotherSimpleAggregateQuery setIsThisSubquery(boolean isThisSubquery) {
        this.isThisSubquery = isThisSubquery;
        return this;
    }
    public AnotherSimpleAggregateQuery setPredicateSubqueries(Map<String, ReadQuery> predicateSubqueries){
        this.predicateSubqueries = predicateSubqueries;
        return this;
    }
    public AnotherSimpleAggregateQuery setResultTableName(String resultTableName){
        this.resultTableName = resultTableName;
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
        List<RelRow> shardData = shard.getData();
        List<RelRow> filteredData = filter(shardData, concreteSubqueriesResults);

        Integer aggregatingDSID = sourceName.hashCode() % numRepartitions;
        RelRow partialResultsRow = computePartialResults(filteredData);
        Map<Integer, List<ByteString>> ret = new HashMap<>();
        List<ByteString> serializedRow = new ArrayList<>();

        serializedRow.add(Utilities.objectToByteString(partialResultsRow));
        ret.put(aggregatingDSID, serializedRow);

        return ret;
    }
    @Override
    public ByteString gather(Map<String, List<ByteString>> ephemeralData, Map<String, RelShard> ephemeralShards) {
        //this should be executed only once, by the dsID selected by the scatter!!
        List<ByteString> serializedPartialResultRows = ephemeralData.get(sourceName);
        if(serializedPartialResultRows == null || serializedPartialResultRows.isEmpty()){
            return Utilities.objectToByteString(new ArrayList<>());
        }
        List<RelRow> partialResults = serializedPartialResultRows.stream().map((v)->(RelRow)Utilities.byteStringToObject(v)).collect(Collectors.toList());
        ArrayList<RelRow> results = new ArrayList<>();
        results.add(computeResults(partialResults));
        return Utilities.objectToByteString(results);
    }
    @Override
    public void writeIntermediateShard(RelShard intermediateShard, ByteString gatherResults){
        List<RelRow> rows = (List<RelRow>) Utilities.byteStringToObject(gatherResults);
        intermediateShard.insertRows(rows);
        intermediateShard.committRows();
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
            //List of Map<ShardID, dsID> to be converted in what is in the RelReadQueryResults
        }else {
            List<RelRow> ret = new ArrayList<>();
            for(ByteString serSubset: shardQueryResults){
                ret.addAll((List<RelRow>)Utilities.byteStringToObject(serSubset));
            }
            results.addData(ret);
            //List of RelRows lsis to be merged together in the structure
        }
        return results;
    }

    private List<RelRow> filter(List<RelRow> data, Map<String, ReadQueryResults> subqRes) {
        if(filterPredicate.isEmpty()){
            return data;
        }
        List<RelRow> filteredRows = new ArrayList<>();
        Map<String, RelReadQueryResults> sRes = new HashMap<>();
        for(Map.Entry<String, ReadQueryResults> entry: subqRes.entrySet()){
            sRes.put(entry.getKey(), (RelReadQueryResults) entry.getValue());
        }
        for(RelRow row: data){
            if(checkFilterPredicate(row, sRes)){
                filteredRows.add(row);
            }
        }
        return filteredRows;
    }
    private boolean checkFilterPredicate(RelRow row, Map<String, RelReadQueryResults> subqRes){
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
    private RelRow computePartialResults(List<RelRow> shardData){
        List<Object> partialResults = new ArrayList<>();
        for(Pair<Integer, String> aggregate: aggregatesSpecification){
            Integer aggregateCode = aggregate.getValue0();
            String aggregatedAttribute = aggregate.getValue1();
            if(aggregateCode.equals(ReadQueryBuilder.AVG)){
                Integer[] countSum = new Integer[2];
                Arrays.fill(countSum, 0);
                for(RelRow row: shardData){
                    countSum[0]++;
                    countSum[1] += (Integer) row.getField(sourceSchema.indexOf(aggregatedAttribute));
                }
                partialResults.add(countSum);
            } else if (aggregateCode.equals(ReadQueryBuilder.MIN)) {
                Integer min = Integer.MAX_VALUE;
                for (RelRow row: shardData){
                    min = Integer.min(min, (Integer) row.getField(sourceSchema.indexOf(aggregatedAttribute)));
                }
                partialResults.add(min);
            } else if (aggregateCode.equals(ReadQueryBuilder.MAX)) {
                Integer max = Integer.MIN_VALUE;
                for (RelRow row: shardData){
                    max = Integer.max(max, (Integer) row.getField(sourceSchema.indexOf(aggregatedAttribute)));
                }
                partialResults.add(max);
            } else if (aggregateCode.equals(ReadQueryBuilder.COUNT)) {
                partialResults.add(shardData.size());
            } else if (aggregateCode.equals(ReadQueryBuilder.SUM)) {
                Integer sum = 0;
                for (RelRow row: shardData){
                    sum += (Integer) row.getField(sourceSchema.indexOf(aggregatedAttribute));
                }
                partialResults.add(sum);
            }
        }


        return new RelRow(partialResults.toArray());
    }
    private RelRow computeResults(List<RelRow> partialResults){
        List<Object> resultRow = new ArrayList<>();
        for(Pair<Integer, String> pair: aggregatesSpecification){
            Integer aggregateCode = pair.getValue0();
            if(aggregateCode.equals(ReadQueryBuilder.AVG)){
                Integer count = 0;
                Integer sum = 0;
                for(RelRow row: partialResults){
                    Integer[] countSum = (Integer[]) row.getField(aggregatesSpecification.indexOf(pair));
                    count += countSum[0];
                    sum += countSum[1];
                }
                if(count != 0) {
                    resultRow.add(sum / count);
                }
            } else if (aggregateCode.equals(ReadQueryBuilder.MIN)) {
                Integer min = Integer.MAX_VALUE;
                for (RelRow row: partialResults){
                    min = Integer.min(min, (Integer) row.getField(aggregatesSpecification.indexOf(pair)));
                }
                resultRow.add(min);
            } else if (aggregateCode.equals(ReadQueryBuilder.MAX)) {
                Integer max = Integer.MIN_VALUE;
                for (RelRow row: partialResults){
                    max = Integer.max(max, (Integer) row.getField(aggregatesSpecification.indexOf(pair)));
                }
                resultRow.add(max);
            } else if (aggregateCode.equals(ReadQueryBuilder.COUNT)) {
                Integer count = 0;
                for(RelRow row: partialResults){
                    count += (Integer) row.getField(aggregatesSpecification.indexOf(pair));
                }
                resultRow.add(count);
            } else if (aggregateCode.equals(ReadQueryBuilder.SUM)) {
                Integer sum = 0;
                for (RelRow row: partialResults){
                    sum += (Integer) row.getField(aggregatesSpecification.indexOf(pair));
                }
                resultRow.add(sum);
            }
        }
        return new RelRow(resultRow.toArray());
    }
}
