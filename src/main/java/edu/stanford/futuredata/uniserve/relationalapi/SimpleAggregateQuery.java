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
    //private List<String> sourceSchema = new ArrayList<>();
    private List<String> resultsSchema = new ArrayList<>();
    private List<Pair<Integer, String>> aggregatesSpecification = new ArrayList<>();
    private String filterPredicate = "";
    private Serializable cachedFilterPredicate = "";
    private Map<String, ReadQuery> sourceSubqueries = new HashMap<>();
    private boolean stored;
    private boolean isThisSubquery = false;
    private Map<String, ReadQuery> predicateSubqueries = new HashMap<>();
    private String resultTableName = "";
    private WriteResultsPlan writeResultsPlan = null;
    private List<Serializable> operations = new ArrayList<>();
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
    /*public SimpleAggregateQuery setSourceSchema(List<String> sourceSchema) {
        this.sourceSchema = sourceSchema;
        return this;
    }*/
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

        Integer aggregatingDSID = sourceName == null ? 0 : sourceName.hashCode() % numRepartitions;
        if(aggregatingDSID < 0){
            aggregatingDSID = aggregatingDSID*-1;
        }
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
    public boolean writeIntermediateShard(RelShard intermediateShard, ByteString gatherResults){
        List<RelRow> rows = (List<RelRow>) Utilities.byteStringToObject(gatherResults);
        return intermediateShard.insertRows(rows) && intermediateShard.committRows();
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
        if(filterPredicate == null || filterPredicate.isEmpty()){
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
/*
        for(String attributeName: sourceSchema){
            if(filterPredicate.contains(attributeName)){
                Object val = row.getField(sourceSchema.indexOf(attributeName));
                values.put(attributeName, val);
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


    private RelRow computePartialResults(List<RelRow> shardData){
/*
        List<Object> partialResults = new ArrayList<>();
        for(Pair<Integer, String> aggregate: aggregatesSpecification){
            Integer aggregateCode = aggregate.getValue0();
            String aggregatedAttribute = aggregate.getValue1();
            if(aggregateCode.equals(RelReadQueryBuilder.AVG)){
                Double[] countSum = new Double[2];
                Arrays.fill(countSum, 0D);
                for(RelRow row: shardData){
                    Object val = row.getField(sourceSchema.indexOf(aggregatedAttribute));
                    if(val != null){
                        countSum[1] += ((Number) row.getField(sourceSchema.indexOf(aggregatedAttribute))).doubleValue();
                        countSum[0]++;
                    }
                }
                partialResults.add(countSum);
            } else if (aggregateCode.equals(RelReadQueryBuilder.MIN)) {
                Double min = Double.MAX_VALUE;
                for (RelRow row: shardData){
                    Object val = row.getField(sourceSchema.indexOf(aggregatedAttribute));
                    if(val != null){
                        min = Double.min(min, ((Number) row.getField(sourceSchema.indexOf(aggregatedAttribute))).doubleValue());
                    }
                }
                partialResults.add(min);
            } else if (aggregateCode.equals(RelReadQueryBuilder.MAX)) {
                Double max = Double.MIN_VALUE;
                for (RelRow row: shardData){
                    Object val = row.getField(sourceSchema.indexOf(aggregatedAttribute));
                    if(val != null){
                        max = Double.max(max, ((Number) row.getField(sourceSchema.indexOf(aggregatedAttribute))).doubleValue());
                    }
                }
                partialResults.add(max);
            } else if (aggregateCode.equals(RelReadQueryBuilder.COUNT)) {
                Double cnt = 0D;
                cnt += ((Number) shardData.size()).doubleValue();
                partialResults.add(cnt);
            } else if (aggregateCode.equals(RelReadQueryBuilder.SUM)) {
                Double sum = 0D;
                for (RelRow row: shardData){
                    if(row.getField(sourceSchema.indexOf(aggregatedAttribute)) != null) {
                        sum += ((Number) row.getField(sourceSchema.indexOf(aggregatedAttribute))).doubleValue();
                    }
                }
                partialResults.add(sum);
            }
        }
*/


        List<Object> partialResults = new ArrayList<>();
        for(Pair<Integer, Integer> aggregate: aggregatesOPsToIndexes){
            Integer aggregateCode = aggregate.getValue0();
            Integer index = aggregate.getValue1();
            if(aggregateCode.equals(RelReadQueryBuilder.AVG)){
                Double[] countSum = new Double[2];
                Arrays.fill(countSum, 0D);
                for(RelRow row: shardData){
                    Object val = row.getField(index);
                    if(val != null){
                        countSum[1] += ((Number) val).doubleValue();
                        countSum[0]++;
                    }
                }
                partialResults.add(countSum);
            } else if (aggregateCode.equals(RelReadQueryBuilder.MIN)) {
                Double min = Double.MAX_VALUE;
                for (RelRow row: shardData){
                    Object val = row.getField(index);
                    if(val != null){
                        min = Double.min(min, ((Number) val).doubleValue());
                    }
                }
                partialResults.add(min);
            } else if (aggregateCode.equals(RelReadQueryBuilder.MAX)) {
                Double max = Double.MIN_VALUE;
                for (RelRow row: shardData){
                    Object val = row.getField(index);
                    if(val != null){
                        max = Double.max(max, ((Number) val).doubleValue());
                    }
                }
                partialResults.add(max);
            } else if (aggregateCode.equals(RelReadQueryBuilder.COUNT)) {
                Double cnt = 0D;
                cnt += ((Number) shardData.size()).doubleValue();
                partialResults.add(cnt);
            } else if (aggregateCode.equals(RelReadQueryBuilder.SUM)) {
                Double sum = 0D;
                for (RelRow row: shardData){
                    Object val = row.getField(index);
                    if(val != null) {
                        sum += ((Number) val).doubleValue();
                    }
                }
                partialResults.add(sum);
            }
        }



        return new RelRow(partialResults.toArray());
    }
    private RelRow computeResults(List<RelRow> partialResults){
        List<Object> resultRow = new ArrayList<>();
/*
        for(Pair<Integer, String> pair: aggregatesSpecification){
            Integer aggregateCode = pair.getValue0();
            if(aggregateCode.equals(RelReadQueryBuilder.AVG)){
                Double count = 0D;
                Double sum = 0D;
                for(RelRow row: partialResults){
                    Object val = row.getField(aggregatesSpecification.indexOf(pair));
                    if(val == null){
                        count = 1D;
                        sum = 0D;
                    }else {
                        Double[] countSum = (Double[]) row.getField(aggregatesSpecification.indexOf(pair));
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
                    if (row.getField(aggregatesSpecification.indexOf(pair)) != null) {
                        min = Double.min(min, ((Number) row.getField(aggregatesSpecification.indexOf(pair))).doubleValue());
                    }
                }
                resultRow.add(min);
            } else if (aggregateCode.equals(RelReadQueryBuilder.MAX)) {
                Double max = Double.MIN_VALUE;
                for (RelRow row: partialResults){
                    if(row.getField(aggregatesSpecification.indexOf(pair)) != null) {
                        max = Double.max(max, ((Number) row.getField(aggregatesSpecification.indexOf(pair))).doubleValue());
                    }
                }
                resultRow.add(max);
            } else if (aggregateCode.equals(RelReadQueryBuilder.COUNT)) {
                Double count = 0D;
                for(RelRow row: partialResults){
                    if(row.getField(aggregatesSpecification.indexOf(pair)) !=null) {
                        count += ((Number) row.getField(aggregatesSpecification.indexOf(pair))).doubleValue();
                    }
                }
                resultRow.add(count);
            } else if (aggregateCode.equals(RelReadQueryBuilder.SUM)) {
                Double sum = 0D;
                for (RelRow row: partialResults){
                    if(row.getField(aggregatesSpecification.indexOf(pair)) !=null) {
                        sum += ((Number) row.getField(aggregatesSpecification.indexOf(pair))).doubleValue();
                    }
                }
                resultRow.add(sum);
            }
        }


 */

        for(int i = 0; i<aggregatesOPsToIndexes.size(); i++){
            Integer aggregateCode = aggregatesOPsToIndexes.get(i).getValue0();
            if(aggregateCode.equals(RelReadQueryBuilder.AVG)){
                Double count = 0D;
                Double sum = 0D;
                for(RelRow row: partialResults){
                    Object val = row.getField(i);
                    if(val == null){
                        count = 1D;
                        sum = 0D;
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