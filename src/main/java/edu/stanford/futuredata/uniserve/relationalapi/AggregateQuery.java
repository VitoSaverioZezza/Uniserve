package edu.stanford.futuredata.uniserve.relationalapi;

import com.google.protobuf.ByteString;
import com.google.protobuf.Parser;
import edu.stanford.futuredata.uniserve.broker.Broker;
import edu.stanford.futuredata.uniserve.interfaces.Row;
import edu.stanford.futuredata.uniserve.interfaces.ShuffleOnReadQueryPlan;
import edu.stanford.futuredata.uniserve.interfaces.VolatileShuffleQueryPlan;
import edu.stanford.futuredata.uniserve.relational.RelReadQueryResults;
import edu.stanford.futuredata.uniserve.relational.RelRow;
import edu.stanford.futuredata.uniserve.relational.RelShard;
import edu.stanford.futuredata.uniserve.relationalapi.querybuilders.ReadQueryBuilder;
import edu.stanford.futuredata.uniserve.utilities.Utilities;
import org.apache.commons.jexl3.*;
import org.javatuples.Pair;

import java.util.*;

public class AggregateQuery implements VolatileShuffleQueryPlan<RelReadQueryResults, RelShard> {
    private List<String> finalSchema = new ArrayList<>();
    private List<Pair<Integer, String>> aggregatesSubschema = new ArrayList<>();
    private List<String> groupAttributesSubschema = new ArrayList<>();
    private List<String> gatherInputRowsSchema = new ArrayList<>();
    private Map<String, ReadQuery> intermediateQuery = new HashMap<>();
    private String havingPredicate = "";
    private List<String> aggregatesAliases = new ArrayList<>();
    private boolean stored = false;


    public AggregateQuery setStored(){
        this.stored = true;
        return this;
    }

    public AggregateQuery setAggregatesAliases(List<String> aggregatesAliases){
        this.aggregatesAliases = aggregatesAliases;
        return this;
    }
    public AggregateQuery setHavingPredicate(String havingPredicate){
        this.havingPredicate = havingPredicate;
        return this;
    }
    public AggregateQuery setFinalSchema(List<String> finalSchema){
        this.finalSchema = finalSchema;
        return this;
    }
    public AggregateQuery setAggregatesSubschema(List<Pair<Integer,String>> aggregatesSubschema){
        this.aggregatesSubschema = aggregatesSubschema;
        return this;
    }
    public AggregateQuery setGroupAttributesSubschema(List<String> groupAttributesSubschema){
        this.groupAttributesSubschema = groupAttributesSubschema;
        return this;
    }
    public AggregateQuery setGatherInputRowsSchema(List<String> gatherInputRowsSchema){
        this.gatherInputRowsSchema = gatherInputRowsSchema;
        return this;
    }
    public AggregateQuery setIntermediateQuery(String intermediateID, ReadQuery intermediateQuery) {
        if(!this.intermediateQuery.isEmpty()){
            throw new RuntimeException("IntermediateQuery already defined");
        }
        this.intermediateQuery.put(intermediateID, intermediateQuery);
        return this;
    }

    @Override
    public String getQueriedTables() {
        return new ArrayList<>(intermediateQuery.keySet()).get(0);
    }

    public ReadQuery getIntermediateQuery(){
        return intermediateQuery.get(new ArrayList<>(intermediateQuery.keySet()).get(0));
    }


    @Override
    public Map<Integer, List<ByteString>> scatter(RelShard shard, int numRepartitions) {
        List<RelRow> shardData = shard.getData();
        Map<Integer, List<RelRow>> dsIDtoAssignedRows = new HashMap<>();
        for(RelRow row: shardData){
            int dsID = getRowHash(row) % numRepartitions;
            dsIDtoAssignedRows.computeIfAbsent(dsID, k->new ArrayList<>()).add(row);
        }
        Map<Integer, List<ByteString>> ret = new HashMap<>();
        for(Map.Entry<Integer, List<RelRow>> assignment: dsIDtoAssignedRows.entrySet()){
            ret.put(assignment.getKey(), new ArrayList<>());
            List<RelRow> dataToSend = assignment.getValue();
            Object[] dataToSendObjArray = dataToSend.toArray();
            int STEPSIZE = 100;
            for(int i = 0; i<dataToSendObjArray.length; i+=STEPSIZE){
                Object[] dataToSendChunkArray = Arrays.copyOfRange(dataToSendObjArray, i, Integer.min(dataToSendObjArray.length, i+STEPSIZE));
                ret.get(assignment.getKey()).add(Utilities.objectToByteString(dataToSendChunkArray));
            }
        }
        return ret;
    }

    private int getRowHash(RelRow row){
        int ret = 0;
        for(String groupField: groupAttributesSubschema){
            ret += row.getField(gatherInputRowsSchema.indexOf(groupField)).hashCode();
        }
        return ret;
    }




    @Override
    public ByteString gather(List<ByteString> ephemeralData) {
        List<ByteString> serializedScatteredData = ephemeralData;
        Map<List<Object>, List<RelRow>> groups = new HashMap<>();
        for(ByteString serRowChunkArray: serializedScatteredData){
            Object[] dataChunkArray = (Object[]) Utilities.byteStringToObject(serRowChunkArray);
            for(Object o: dataChunkArray){
                RelRow row = (RelRow) o;
                List<Object> rowGroupFields = getGroup(row);
                groups.computeIfAbsent(rowGroupFields, k->new ArrayList<>()).add(row);
            }
        }
        List<RelRow> resultRows = new ArrayList<>();
        for(Map.Entry<List<Object>, List<RelRow>> group: groups.entrySet()){
            List<Object> result = group.getKey();
            List<Object> aggregatedAttributeRes = computeAggregates(group.getValue());
            if(checkHavingPredicate(aggregatedAttributeRes)) {
                result.addAll(aggregatedAttributeRes);
                resultRows.add(new RelRow(result.toArray()));
            }
        }
        return Utilities.objectToByteString(resultRows.toArray());
    }

    private boolean checkHavingPredicate(List<Object> row){
        if(havingPredicate == null || havingPredicate.isEmpty()){
            return true;
        }
        String predToTest = new String(havingPredicate);
        Map<String, Object> values = new HashMap<>();
        for(String aggregateAlias: aggregatesAliases){
            if(havingPredicate.contains(aggregateAlias)){
                values.put(aggregateAlias, row.get(aggregatesAliases.indexOf(aggregateAlias)));
            }
        }
        for(int i=groupAttributesSubschema.size(); i < gatherInputRowsSchema.size()-1; i++){
            values.put(finalSchema.get(i), row.get(i));
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
        List<Object> res = new ArrayList<>(aggregatesSubschema.size());
        for (Pair<Integer, String> aggregate : aggregatesSubschema) {
            if (Objects.equals(aggregate.getValue0(), ReadQueryBuilder.AVG)) {
                int count = 0, sum = 0;
                for (RelRow row : groupRows) {
                    sum += (Integer)row.getField(gatherInputRowsSchema.indexOf(aggregate.getValue1()));
                    count++;
                }
                res.add(sum/count);
            } else if (Objects.equals(aggregate.getValue0(), ReadQueryBuilder.MIN)) {
                int min = Integer.MAX_VALUE;
                for(RelRow row: groupRows){
                    min = Integer.min(min, (Integer)row.getField(gatherInputRowsSchema.indexOf(aggregate.getValue1())));
                }
                res.add(min);
            } else if (Objects.equals(aggregate.getValue0(), ReadQueryBuilder.MAX)) {
                int max = Integer.MIN_VALUE;
                for (RelRow row : groupRows) {
                    max = Integer.max(max, (Integer) row.getField(gatherInputRowsSchema.indexOf(aggregate.getValue1())));
                }
                res.add(max);
            } else if (Objects.equals(aggregate.getValue0(), ReadQueryBuilder.COUNT)) {
                res.add(groupRows.size());
            }else if (Objects.equals(aggregate.getValue0(), ReadQueryBuilder.SUM)) {
                int sum = 0;
                for(RelRow row: groupRows){
                    sum += (Integer)row.getField(gatherInputRowsSchema.indexOf(aggregate.getValue1()));
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
        for(String groupField: groupAttributesSubschema){
            ret.add(row.getField(groupAttributesSubschema.indexOf(groupField)));
        }
        return ret;
    }

    @Override
    public RelReadQueryResults combine(List<ByteString> shardQueryResults) {
        List<RelRow> results = new ArrayList<>();
        for(ByteString bs: shardQueryResults){
            Object[] resRowArray = (Object[]) Utilities.byteStringToObject(bs);
            for(Object o: resRowArray){
                results.add((RelRow) o);
            }
        }
        RelReadQueryResults relReadQueryResults = new RelReadQueryResults();
        relReadQueryResults.setFieldNames(finalSchema);
        relReadQueryResults.addData(results);
        return relReadQueryResults;
    }

    @Override
    public void setTableName(String tableName) {

    }

    @Override
    public boolean write(RelShard shard, List<Row> data) {
        List<RelRow> data1 = new ArrayList<>();
        for(Object o: data){
            data1.add((RelRow) o);
        }
        return shard.insertRows(data1) && shard.committRows();
    }

    public RelReadQueryResults run(Broker broker){
        RelReadQueryResults res;
        List<RelRow> subqueryData = new ArrayList<>(intermediateQuery.values()).get(0).run(broker).getData();
        List<Row> data = new ArrayList<>(subqueryData);
        res = broker.volatileShuffleQuery(this, data);
        res.setFieldNames(finalSchema);
        return res;
    }


    public List<Pair<Integer, String>> getAggregatesSubschema() {
        return aggregatesSubschema;
    }
    public List<String> getGroupAttributesSubschema(){
        return groupAttributesSubschema;
    }
    public String getHavingPredicate(){
        return havingPredicate;
    }
}
